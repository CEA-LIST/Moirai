#!/usr/bin/env bash
#
# M-E4 — per-log state transfer, fresh-joiner path.
#
# For N in {1, 16}: one donor container hosting N models, the SimpleUML model
# A and (for N = 16) fifteen more alternating behaviour tree and SimpleUML,
# each at OPS seeded operations; then RUNS fresh joiner containers in turn,
# each registering A alone and adopting the donor's snapshot; and a probe
# reading the `StateResponse` for A off the wire before each one. See
# measure.py for what each column is.
#
# Threshold, fixed in the validation plan: bytes and time at N = 16 within
# 5 percent of N = 1. The verdict is computed below over the medians and
# written to the manifest; the CSV is complete either way.
#
# Usage, from this directory: ./run.sh
# Knobs: OPS (500), RUNS (15), SEED (20260903), POINTS (1,16). Fifteen runs
# because the joiner adopts on its next 10 ms loop tick, so a median of five
# carried the width of the 5 percent band in tick noise.

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

OPS=${OPS:-500}
RUNS=${RUNS:-15}
SEED=${SEED:-20260903}
POINTS=${POINTS:-1,16}

stamp=$(mp_stamp)
out="$HERE/results.csv"
net="moirai-m-e4-$stamp"

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

first=1
for n in ${POINTS//,/ }; do
    donor="$net-d$n"
    mp_say "N=$n: donor up (load $(mp_load_average))"
    mp_start_node "$net" "$donor" "d$n" ""
    mp_wait_healthy "$donor"
    append=()
    [ "$first" -eq 1 ] || append=(--append)
    first=0
    python3 "$HERE/measure.py" \
        --donor-http "http://$(mp_http_of "$donor")" \
        --donor-sync "$(mp_sync_of "$donor")" \
        --donor-name "$donor" --donor-id "d$n" \
        --n-logs "$n" --ops "$OPS" --runs "$RUNS" --seed "$SEED" \
        --joiner-prefix "$net-j${n}x" \
        --start-joiner "bash -c '. $HERE/../common.sh; mp_start_node $net {name} {id} d$n:$donor:9001; mp_wait_healthy {name}; mp_http_of {name}'" \
        --stop-joiner "docker rm --force --volumes {name} >/dev/null" \
        --out "$out" "${append[@]}"
    docker rm --force --volumes "$donor" >/dev/null
done

verdict=$(python3 - "$out" "$POINTS" <<'PY'
import csv, statistics, sys
rows = list(csv.DictReader(open(sys.argv[1])))
points = [int(p) for p in sys.argv[2].split(",")]
def med(n, field):
    return statistics.median(float(r[field]) for r in rows if int(r["n_logs"]) == n)
lo, hi = points[0], points[-1]
lines = []
# The plan's two quantities carry the threshold: the StateResponse bytes on
# the wire and the time from the request being served to adoption. The rest
# are reported beside them with their ratios and no verdict.
thresholded = {"response_bytes", "transfer_ms"}
for field, label in [("response_bytes", "wire bytes"), ("transfer_ms", "serving->adopted ms"),
                     ("served_bytes_joiner", "served log bytes (uncompressed)"),
                     ("probe_ms", "probe request->answer ms"), ("join_to_state_ms", "join->state ms")]:
    a, b = med(lo, field), med(hi, field)
    ratio = b / a if a else float("inf")
    if field in thresholded:
        met = abs(ratio - 1.0) <= 0.05
        lines.append(f"{label}: N={lo} median {a:g}, N={hi} median {b:g}, ratio {ratio:.3f} -> {'met' if met else 'CROSSED'}")
    else:
        lines.append(f"{label}: N={lo} median {a:g}, N={hi} median {b:g}, ratio {ratio:.3f} (supplementary)")
print("\n".join(lines))
PY
)
echo "$verdict" >&2

{
    mp_manifest_common "$stamp" "OPS=$OPS RUNS=$RUNS SEED=$SEED POINTS=$POINTS ./run.sh"
    mp_manifest_image
    printf '%-14s%s\n' points "$POINTS models on the donor; model A is SimpleUML, the rest alternate behaviour tree and SimpleUML"
    printf '%-14s%s\n' ops "$OPS seeded operations per model (wire.py seeded_ops, seed $SEED)"
    printf '%-14s%s\n' runs "$RUNS per N, a fresh joiner container each, registering A alone"
    printf '%-14s%s\n' bytes "response_bytes: the StateResponse line as the probe reads it; served_bytes_*: the donor's own log line, the serialised log before compression"
    printf '%-14s%s\n' time "transfer_ms: donor 'serving' line to joiner 'adopted' line, Docker daemon clock; probe_ms: host clock around the probe's request; join_to_state_ms: host clock from POST /api/models to the state matching, polled every 2 ms"
    printf '%-14s%s\n' threshold "response_bytes and transfer_ms at N=16 within 5 percent of N=1 (medians); the other columns are supplementary"
    printf '%-14s%s\n' members "every joiner stays a member of the donor's table after its run, so snapshot_members climbs by one per run and the uncompressed served log with it, one version-vector entry per event; compare runs at equal snapshot_members, or the medians, which sit at the same run index"
    printf '%-14s%s\n' verdict "$(echo "$verdict" | tr '\n' ';')"
} > "$HERE/manifest.txt"
cat "$HERE/manifest.txt"
! echo "$verdict" | grep -q CROSSED
