#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 EXPERIMENT_CONFIG.json" >&2
  exit 2
fi

config_file="$(realpath -- "$1")"
if [[ ! -f "$config_file" ]]; then
  echo "Experiment configuration not found: $config_file" >&2
  exit 2
fi

run_dir="$(mktemp -d "${TMPDIR:-/tmp}/moirai-experiment.XXXXXX")"
cleanup_resources=0

cleanup() {
  local status=$?
  trap - EXIT
  if [[ "$cleanup_resources" -eq 1 ]]; then
    kubectl delete job moirai-client-load \
      --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
    kubectl delete -k "${run_dir}/replicas" \
      --ignore-not-found --cascade=foreground --wait=true >/dev/null 2>&1 || true
  fi
  rm -rf -- "$run_dir"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

results_file="$(python3 "${script_dir}/render-experiment.py" \
  "$config_file" "$script_dir" "$run_dir")"
runtime_config="${run_dir}/runtime-config.json"
with_fault="$(python3 -c 'import json,sys; print(str(json.load(open(sys.argv[1]))["with_fault"]).lower())' "$runtime_config")"
failure_time="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["failure_time"])' "$runtime_config")"

if kubectl get job moirai-client-load >/dev/null 2>&1; then
  echo "Client Job moirai-client-load already exists. Collect its results and clean up before starting a new experiment." >&2
  exit 1
fi

if kubectl get statefulset moirai-multi-replica >/dev/null 2>&1 ||
   kubectl get service moirai-multi-replicas >/dev/null 2>&1 ||
   kubectl get service moirai-multi-http >/dev/null 2>&1; then
  echo "Multi-replica resources already exist. Clean up clients and replicas before starting a new experiment." >&2
  exit 1
fi

cleanup_resources=1
kubectl apply -k "${run_dir}/replicas"
kubectl rollout status statefulset/moirai-multi-replica --timeout=300s
python3 "${script_dir}/measure-replicas.py" snapshot "${run_dir}/replicas-start.json"

kubectl apply -k "${run_dir}/clients"
job_start_seconds=$SECONDS
job_timeout_seconds=360
job_status=0
fault_applied=false

while true; do
  job_conditions="$(kubectl get job moirai-client-load \
    -o jsonpath='{range .status.conditions[*]}{.type}={.status}{end}')"

  if [[ "$job_conditions" == *"Complete=True"* ]]; then
    break
  fi
  if [[ "$job_conditions" == *"Failed=True"* ]]; then
    echo "Client Job failed; collecting available results before cleanup." >&2
    job_status=1
    break
  fi

  elapsed_seconds=$((SECONDS - job_start_seconds))
  if (( elapsed_seconds >= job_timeout_seconds )); then
    echo "Client Job did not complete within six minutes; collecting available results before cleanup." >&2
    job_status=1
    break
  fi

  if [[ "$with_fault" == "true" && "$fault_applied" == "false" && \
        "$elapsed_seconds" -ge "$failure_time" ]]; then
    echo "Injecting replica failure at ${elapsed_seconds}s by removing StatefulSet ordinal 0." >&2
    if ! python3 "${script_dir}/measure-replicas.py" snapshot \
      --allow-incomplete "${run_dir}/replicas-failure.json"; then
      echo "Could not capture the pre-failure replica snapshot." >&2
    fi
    archived_replica_logs="${run_dir}/replica-0-logs.txt"
    if ! kubectl logs moirai-multi-replica-0 -c replica > "$archived_replica_logs"; then
      echo "Could not archive logs for replica 0 before failure." >&2
      rm -f -- "$archived_replica_logs"
    fi

    replica_count="$(kubectl get statefulset moirai-multi-replica \
      -o jsonpath='{.spec.replicas}')"
    if [[ ! "$replica_count" =~ ^[0-9]+$ ]] || (( replica_count < 1 )); then
      echo "Cannot inject replica failure: invalid StatefulSet replica count '${replica_count}'." >&2
      exit 1
    fi
    remaining_replicas=$((replica_count - 1))
    statefulset_patch="{\"spec\":{\"ordinals\":{\"start\":1},\"replicas\":${remaining_replicas}}}"
    kubectl patch statefulset moirai-multi-replica \
      --type=merge --patch "$statefulset_patch"
    kubectl wait --for=delete pod/moirai-multi-replica-0 --timeout=120s
    fault_applied=true
  fi

  sleep 1
done

metrics_status=0
if python3 "${script_dir}/measure-replicas.py" snapshot \
  --allow-incomplete "${run_dir}/replicas-end.json"; then
  compare_args=()
  if [[ -s "${run_dir}/replicas-failure.json" ]]; then
    compare_args+=(--failure-snapshot "${run_dir}/replicas-failure.json")
  fi
  if ! python3 "${script_dir}/measure-replicas.py" compare \
    "${run_dir}/replicas-start.json" "${run_dir}/replicas-end.json" \
    "${run_dir}/replica-resource-usage.jsonl" "${compare_args[@]}"; then
    metrics_status=1
  fi
else
  metrics_status=1
fi

if ! bash "${script_dir}/collect-results.sh" "$results_file" \
  "${run_dir}/replica-resource-usage.jsonl" \
  "${run_dir}/replica-0-logs.txt"; then
  job_status=1
fi

if [[ "$job_status" -ne 0 || "$metrics_status" -ne 0 ]]; then
  exit 1
fi
echo "Experiment completed. Results: ${results_file}"
