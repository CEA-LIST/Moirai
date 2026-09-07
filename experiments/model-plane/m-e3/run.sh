#!/usr/bin/env bash
#
# M-E3 — no head-of-line blocking.
#
# The rig's representative slice as containers with published ports: two
# editor replicas and three node replicas, peered statically, every one
# hosting the same sixteen models. Model A (SimpleUML) is edited on editor-a
# and timed to per-replica agreement; model B (behaviour tree) is either
# idle or under the driver's load. See measure.py for the arms and columns.
#
# Containers with published ports rather than the Compose rig, on purpose:
# only the rig's two editors publish a port, and a convergence of tens of
# milliseconds has to be polled from the host at loopback latency on every
# replica, not through `docker exec`.
#
# Threshold, fixed in the validation plan: the median with B loaded is at
# most 1.10 x the median without.
#
# Usage, from this directory: ./run.sh
# Knobs: RUNS (30), OPS_PER_RUN (20), SEED (20260903), OP_INTERVAL_SECS (1),
# ARMS (alone,loaded,alone-again,loaded-http,loaded-wire-heavy,loaded-http-heavy),
# N_MODELS (16), HEAVY_RATE (500), HEAVY_CLIENTS (4); NOTE, a sentence for the
# manifest about the circumstances of the run, empty by default; RESUME=1 to
# add the arms of this invocation to the CSV and summaries of an earlier one
# (each arm is a session of its own, so the arms of one run may be taken in
# several invocations, which the manifest lists), instead of starting over.
#
# At the tip, `loaded-http-heavy` does not complete in the conforming write
# shape: an adapter write into a model costs about the square of the model's
# string length at the intake (27 ms per POST at 100 characters, 205 ms at
# 1,000 on one idle replica), so HEAVY_CLIENTS x 5 clients with no pause
# drive B past the adapter's 5 s reply timeout within the arm and starve A's
# writes and polls on the same loop. The arm is kept as the plan defines it
# and left out of a run by ARMS.
#
# Thirty runs per arm rather than the plan's ten, and the ten-run median
# reported beside it: a convergence of twenty to thirty milliseconds is
# quantised by the node's 10 ms loop tick (frames are drained once per tick
# and a state query is answered on the next), so the median of ten sits on
# one tick or the next depending on phase, a swing wider than the 10 percent
# the threshold reads.

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

RUNS=${RUNS:-30}
OPS_PER_RUN=${OPS_PER_RUN:-20}
SEED=${SEED:-20260903}
OP_INTERVAL_SECS=${OP_INTERVAL_SECS:-1}
ARMS=${ARMS:-alone,loaded,alone-again,loaded-http,loaded-wire-heavy,loaded-http-heavy}
N_MODELS=${N_MODELS:-16}
HEAVY_CLIENTS=${HEAVY_CLIENTS:-4}
HEAVY_RATE=${HEAVY_RATE:-500}
NOTE=${NOTE:-}
RESUME=${RESUME:-0}

stamp=$(mp_stamp)
out="$HERE/results.csv"
net="moirai-m-e3-$stamp"
ids=(editor-a editor-b node-1 node-2 node-3)

teardown() {
    local rc=$?
    [ "$rc" -eq 0 ] || mp_dump_logs "$net"
    docker ps -aq --filter "network=$net" | xargs -r docker rm --force --volumes >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
}
trap teardown EXIT

mp_require_docker
mp_assert_no_moirai_containers
docker network create "$net" >/dev/null

names=()
for id in "${ids[@]}"; do names+=("$net-$id"); done
peers_for() {
    local self="$1" spec="" i
    for i in "${!ids[@]}"; do
        [ "${ids[$i]}" = "$self" ] && continue
        spec="${spec:+$spec,}${ids[$i]}:${names[$i]}:9001"
    done
    echo "$spec"
}

# A fresh session: five replicas, peered; the models are registered by
# measure.py. Torn down after every arm, because a session ages (see
# measure.py's header) and the arms must be compared at the same age.
session_up() {
    local i name
    for i in "${!ids[@]}"; do
        mp_start_node "$net" "${names[$i]}" "${ids[$i]}" "$(peers_for "${ids[$i]}")"
    done
    for name in "${names[@]}"; do mp_wait_healthy "$name"; done
    http=""
    sync=""
    for name in "${names[@]}"; do
        http="${http:+$http,}http://$(mp_http_of "$name")"
        sync="${sync:+$sync,}$(mp_sync_of "$name")"
    done
}

session_down() {
    docker ps -aq --filter "network=$net" | xargs -r docker rm --force --volumes >/dev/null 2>&1 || true
}

rids="$(IFS=,; echo "${ids[*]}")"
summaries="$HERE/.summaries.jsonl"
rm -f "$summaries"
# The arms of an earlier invocation come back from its results-summary.csv
# and its manifest's invocations line, so a resumed run leaves no file behind
# that a plain run does not.
previous_summary=""
taken=""
if [ "$RESUME" = "1" ]; then
    [ -f "$out" ] && [ -f "$HERE/results-summary.csv" ] || mp_die "RESUME=1 with no earlier results to resume"
    previous_summary="$HERE/results-summary.csv"
    taken="$(sed -n 's/^invocations *//p' "$HERE/manifest.txt" 2>/dev/null || true)"
else
    rm -f "$out"
fi
taken="${taken:+$taken; }$stamp ARMS=$ARMS ./run.sh (load $MP_LOAD_START)"
for arm in ${ARMS//,/ }; do
    mp_say "arm $arm: fresh session (load $(mp_load_average))"
    session_up
    arm_csv="$HERE/.arm-$arm.csv"
    python3 "$HERE/measure.py" --http "$http" --sync "$sync" --ids "$rids" --n-models "$N_MODELS" \
        --runs "$RUNS" --ops-per-run "$OPS_PER_RUN" --seed "$SEED" --op-interval "$OP_INTERVAL_SECS" \
        --heavy-clients "$HEAVY_CLIENTS" --heavy-rate "$HEAVY_RATE" --arm "$arm" \
        --out "$arm_csv" --summary "$HERE/.summary-$arm.json" \
        || { session_down; mp_die "arm $arm failed"; }
    if [ -f "$out" ]; then tail -n +2 "$arm_csv" >> "$out"; else cp "$arm_csv" "$out"; fi
    cat "$HERE/.summary-$arm.json" >> "$summaries"
    rm -f "$arm_csv" "$HERE/.summary-$arm.json"
    session_down
done

verdict=$(python3 - "$summaries" "$HERE/results-summary.csv" "$previous_summary" <<'PY'
import csv, json, sys
fields = ["arm", "runs", "median_ms", "median_first10_ms", "mean_ms", "p90_ms", "load_ops",
          "a_delivered_end", "a_retained_end", "a_stable_prefix_end", "ratio_to_alone", "ratio_first10", "ratio_mean"]
arms = []
if sys.argv[3]:
    for row in csv.DictReader(open(sys.argv[3])):
        arms.append({k: (int(v) if k in ("runs", "load_ops", "a_delivered_end", "a_retained_end", "a_stable_prefix_end")
                         else float(v)) if k != "arm" else v
                     for k, v in row.items() if k in fields and not k.startswith("ratio")})
arms += [json.loads(line) for line in open(sys.argv[1])]
base = next((a for a in arms if a["arm"] == "alone"), None)
if base is None:
    raise SystemExit("VERDICT no alone arm")
with open(sys.argv[2], "w", newline="") as handle:
    w = csv.DictWriter(handle, fieldnames=fields)
    w.writeheader()
    for a in arms:
        w.writerow({**a, "ratio_to_alone": round(a["median_ms"] / base["median_ms"], 3),
                    "ratio_first10": round(a["median_first10_ms"] / base["median_first10_ms"], 3),
                    "ratio_mean": round(a["mean_ms"] / base["mean_ms"], 3)})
loaded = next((a for a in arms if a["arm"] == "loaded"), None)
if loaded is None:
    print("VERDICT no loaded arm")
else:
    ratio = loaded["median_ms"] / base["median_ms"]
    print(f"VERDICT loaded/alone = {ratio:.3f} (median of {loaded['runs']} runs, each arm its own session) -> "
          f"{'met' if ratio <= 1.10 else 'CROSSED'} (threshold 1.10)")
PY
)
all_arms=$(tail -n +2 "$HERE/results-summary.csv" | cut -d, -f1 | paste -sd,)
rm -f "$summaries"
echo "$verdict" >&2
column -s, -t "$HERE/results-summary.csv" >&2

{
    mp_manifest_common "$stamp" "RUNS=$RUNS OPS_PER_RUN=$OPS_PER_RUN SEED=$SEED OP_INTERVAL_SECS=$OP_INTERVAL_SECS HEAVY_RATE=$HEAVY_RATE HEAVY_CLIENTS=$HEAVY_CLIENTS ARMS=$all_arms ./run.sh"
    printf '%-14s%s\n' invocations "$taken"
    mp_manifest_image
    printf '%-14s%s\n' replicas "${ids[*]} (static full mesh; editor-a writes A); a fresh session per arm"
    printf '%-14s%s\n' models "$N_MODELS, created on editor-a alternating behaviour tree and SimpleUML, joined by id everywhere; A = the first SimpleUML one, B = the first behaviour-tree one"
    printf '%-14s%s\n' runs "$RUNS per arm, $OPS_PER_RUN seeded operations on A per run (seed $SEED, the same sequence in every arm); the summary carries the median over all runs, over the first ten (the plan's count) and the mean"
    printf '%-14s%s\n' resolution "convergence is quantised by the 10 ms loop tick on every replica, so a ten-run median moves by a tick between runs of this harness"
    printf '%-14s%s\n' age "every arm runs in a session of its own, started fresh: A's unstable log grows by $OPS_PER_RUN operations per run and nothing compacts on a multi-model rig, so arms taken in one session compare ages, not loads (a no-load repeat after the loaded arm read 1.65 x the first alone arm in one session); a_retained_end is what every delivery walked at the end of the arm"
    printf '%-14s%s\n' load "loaded: ${#ids[@]} of B's frames per replica per ${OP_INTERVAL_SECS}s on the wire from a member 'load' (what each replica receives under drive.sh's one op per replica per tick); loaded-http (supplementary): one POST per replica per ${OP_INTERVAL_SECS}s into B, the adapter's one-request-at-a-time queueing included; loaded-wire-heavy: $HEAVY_RATE frames/s per replica; loaded-http-heavy: $HEAVY_CLIENTS POSTing clients per replica, no pause"
    printf '%-14s%s\n' writes "every write lands in a string attribute the model's descriptor declares (A: name; B: main.ID), one character per operation, as docker/compose/drive.sh writes; the node refuses an undeclared root key at the intake"
    [ -z "$NOTE" ] || printf '%-14s%s\n' note "$NOTE"
    printf '%-14s%s\n' measured "per replica, from the last write's answer to the replica's state equalling the writer's, polled every 2 ms; per run the last replica; medians over runs"
    printf '%-14s%s\n' threshold "median(loaded) <= 1.10 x median(alone)"
    printf '%-14s%s\n' verdict "$verdict"
} > "$HERE/manifest.txt"
cat "$HERE/manifest.txt"
echo "$verdict" | grep -q "met"
