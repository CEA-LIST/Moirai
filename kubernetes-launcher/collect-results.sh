#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
output_file="${1:-client-results/measurements.jsonl}"
replica_metrics_file="${2:-}"
archived_replica_logs_file="${3:-}"
mkdir -p "$(dirname -- "$output_file")"

pods="$(kubectl get pods \
  -l batch.kubernetes.io/job-name=moirai-client-load \
  --field-selector=status.phase=Succeeded \
  -o jsonpath='{.items[*].metadata.name}')"

if [[ -z "$pods" ]]; then
  if [[ -z "$replica_metrics_file" || ! -s "$replica_metrics_file" ]]; then
    echo "No succeeded client pods found. Check job status and pod logs first." >&2
    exit 1
  fi
  echo "No succeeded client pods found; preserving the replica measurements." >&2
fi

: > "$output_file"

for pod in $pods; do
  kubectl logs "$pod" -c load-generator \
    | sed -n 's/^MOIRAI_RESULT //p' \
    >> "$output_file"
done

if [[ -n "$replica_metrics_file" ]]; then
  if [[ ! -s "$replica_metrics_file" ]]; then
    echo "Replica measurement file is missing or empty: $replica_metrics_file" >&2
    exit 1
  fi
  cat -- "$replica_metrics_file" >> "$output_file"
fi

replica_pods="$(kubectl get pods \
  -l app=moirai-multi-replica \
  -o jsonpath='{.items[*].metadata.name}')"

if [[ -n "$replica_pods" ]] || \
   [[ -n "$archived_replica_logs_file" && -s "$archived_replica_logs_file" ]]; then
  {
    for pod in $replica_pods; do
      if ! kubectl logs "$pod" -c replica; then
        echo "Could not collect logs from replica pod ${pod}." >&2
      fi
    done
    if [[ -n "$archived_replica_logs_file" && -s "$archived_replica_logs_file" ]]; then
      cat -- "$archived_replica_logs_file"
    fi
  } | python3 "${script_dir}/collect-replica-memory.py" >> "$output_file"
fi

if [[ ! -s "$output_file" ]]; then
  echo "No client or replica measurement records were collected." >&2
  exit 1
fi

record_count="$(wc -l < "$output_file" | tr -d '[:space:]')"
echo "Collected ${record_count} result records into ${output_file}"
