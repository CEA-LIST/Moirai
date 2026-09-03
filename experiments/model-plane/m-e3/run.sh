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
# Knobs: RUNS (10), OPS_PER_RUN (20), SEED (20260903), OP_INTERVAL_SECS (1),
# ARMS (alone,loaded,loaded-heavy), N_MODELS (16).

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

RUNS=${RUNS:-10}
OPS_PER_RUN=${OPS_PER_RUN:-20}
SEED=${SEED:-20260903}
OP_INTERVAL_SECS=${OP_INTERVAL_SECS:-1}
ARMS=${ARMS:-alone,loaded,loaded-http,loaded-wire-heavy,loaded-http-heavy}
N_MODELS=${N_MODELS:-16}
HEAVY_CLIENTS=${HEAVY_CLIENTS:-4}
HEAVY_RATE=${HEAVY_RATE:-500}

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
for i in "${!ids[@]}"; do
    mp_start_node "$net" "${names[$i]}" "${ids[$i]}" "$(peers_for "${ids[$i]}")"
done
for name in "${names[@]}"; do mp_wait_healthy "$name"; done
mp_say "${#names[@]} replicas up (load $(mp_load_average))"

http=""
sync=""
for name in "${names[@]}"; do
    http="${http:+$http,}http://$(mp_http_of "$name")"
    sync="${sync:+$sync,}$(mp_sync_of "$name")"
done
rids="$(IFS=,; echo "${ids[*]}")"

measure_log="$HERE/.measure.log"
python3 "$HERE/measure.py" --http "$http" --sync "$sync" --ids "$rids" --n-models "$N_MODELS" \
    --runs "$RUNS" --ops-per-run "$OPS_PER_RUN" --seed "$SEED" --op-interval "$OP_INTERVAL_SECS" \
    --heavy-clients "$HEAVY_CLIENTS" --heavy-rate "$HEAVY_RATE" --arms "$ARMS" --out "$out" 2>&1 | tee "$measure_log" >&2
measure_rc=${PIPESTATUS[0]}
verdict=$(grep VERDICT "$measure_log" || true)
rm -f "$measure_log"
[ -f "$out" ] || mp_die "measure.py wrote no results (exit $measure_rc)"

{
    mp_manifest_common "$stamp" "RUNS=$RUNS OPS_PER_RUN=$OPS_PER_RUN SEED=$SEED OP_INTERVAL_SECS=$OP_INTERVAL_SECS HEAVY_RATE=$HEAVY_RATE HEAVY_CLIENTS=$HEAVY_CLIENTS ARMS=$ARMS ./run.sh"
    mp_manifest_image
    printf '%-14s%s\n' replicas "${ids[*]} (static full mesh; editor-a writes A)"
    printf '%-14s%s\n' models "$N_MODELS, created on editor-a alternating behaviour tree and SimpleUML, joined by id everywhere; A = the first SimpleUML one, B = the first behaviour-tree one"
    printf '%-14s%s\n' runs "$RUNS per arm, $OPS_PER_RUN seeded operations on A per run (seed $SEED, the same sequence in every arm)"
    printf '%-14s%s\n' load "loaded: ${#ids[@]} of B's frames per replica per ${OP_INTERVAL_SECS}s on the wire from a member 'load' (what each replica receives under drive.sh's one op per replica per tick); loaded-http (supplementary): one POST per replica per ${OP_INTERVAL_SECS}s into B, the adapter's one-request-at-a-time queueing included; loaded-wire-heavy: $HEAVY_RATE frames/s per replica; loaded-http-heavy: $HEAVY_CLIENTS POSTing clients per replica, no pause"
    printf '%-14s%s\n' measured "per replica, from the last write's answer to the replica's state equalling the writer's, polled every 2 ms; per run the last replica; medians over runs"
    printf '%-14s%s\n' threshold "median(loaded) <= 1.10 x median(alone)"
    printf '%-14s%s\n' verdict "$verdict"
} > "$HERE/manifest.txt"
cat "$HERE/manifest.txt"
echo "$verdict" | grep -q "met"
