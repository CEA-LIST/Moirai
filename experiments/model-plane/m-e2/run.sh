#!/usr/bin/env bash
#
# M-E2 — dispatch cost and idle memory per hosted log.
#
# Two phases, both at N in {1, 4, 16, 64} hosted logs:
#
#   inprocess  `examples/model_plane_cost.rs`: 100,000 lookups of a frame's
#              id in the hosted map, timed one by one, for a hit and for a
#              miss; the deep size of the hosted map with the member table
#              counted once, `m(N)`, and of one entry, `s`; and the number of
#              member tables the logs hold. Run in release, the profile a
#              deployment ships, and again in debug, the profile the rig
#              image is built with (docker/e2e/Dockerfile), so the number the
#              containers pay is on record beside the number a release pays.
#   rig        `docker/rig.sh --models N`, then the resident set of every
#              replica at idle, read as VmRSS from /proc inside the container
#              beside `docker stats`' figure. The rig is torn down before and
#              after every N.
#
# Thresholds, fixed in the validation plan: p50 under 1 µs per lookup at
# N = 64 on both paths; p50 at N = 64 at most twice p50 at N = 1; and
# m(N) ≤ m(1) + (N − 1) × 2 × s; one member table at N = 16 and 64. The
# example prints each verdict; a crossed one is recorded here and the run
# goes on, so the CSV is complete either way.
#
# Usage, from this directory:
#
#     ./run.sh                 both phases
#     PHASE=inprocess ./run.sh
#     PHASE=rig ./run.sh
#
# Knobs: POINTS (default 1,4,16,64), LOOKUPS (100000), IDLE_SECS (20), the
# seconds the rig is left alone before RSS is read.

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

PHASE=${PHASE:-all}
POINTS=${POINTS:-1,4,16,64}
LOOKUPS=${LOOKUPS:-100000}
IDLE_SECS=${IDLE_SECS:-20}
RIG="$MOIRAI_ROOT/docker/rig.sh"

stamp=$(mp_stamp)
results="$HERE/results.csv"
rss="$HERE/rss.csv"
crossed=()
verdicts=()

# --- in process ------------------------------------------------------------

# The example prints one `met:` or `CROSSED:` line per threshold on stderr;
# they are kept, per profile, for the manifest, so a crossed threshold is
# named there and not only on a terminal that has scrolled away.
run_profile() {
    local profile="$1" out="$2" flag=()
    local log="$HERE/.$profile.log"
    [ "$profile" = release ] && flag=(--release)
    mp_say "building and running the example in $profile (load $(mp_load_average))"
    if ! ( cd "$MOIRAI_ROOT" && nice -n 10 cargo run -q "${flag[@]}" -p moirai-network \
            --features test_utils -j 2 --example model_plane_cost -- \
            --out "$out" --points "$POINTS" --lookups "$LOOKUPS" ) 2>&1 | tee "$log" >&2; then
        crossed+=("$profile: $(grep '^CROSSED:' "$log" | sed 's/^CROSSED: //' | paste -sd ';' -)")
    fi
    verdicts+=("$profile: $(grep -E '^(met|CROSSED):' "$log" | paste -sd ';' -)")
    rm -f "$log"
}

phase_inprocess() {
    local rel="$HERE/.release.csv" dbg="$HERE/.debug.csv"
    run_profile release "$rel"
    run_profile debug "$dbg"
    { head -1 "$rel"; tail -n +2 "$rel"; tail -n +2 "$dbg"; } > "$results"
    rm -f "$rel" "$dbg"
    mp_say "wrote $results"
}

# --- the rig ---------------------------------------------------------------

# VmRSS of PID 1 of a container, in bytes: the replica process itself, since
# the image execs `network_node` as its only process.
container_rss() {
    docker exec "$1" awk '/^VmRSS:/ {print $2 * 1024}' /proc/1/status
}

container_metric() {
    docker exec "$1" curl -fsS --max-time 2 "localhost:8081/api/metrics" \
        | grep -o "\"$2\":[0-9]*" | cut -d: -f2
}

rig_down() {
    ( cd "$MOIRAI_ROOT" && "$RIG" down >/dev/null 2>&1 ) || true
    mp_assert_no_moirai_containers
}

phase_rig() {
    mp_require_docker
    mp_say "the rig at each N, idle; RSS read after ${IDLE_SECS}s"
    echo "n_logs,container,replica_id,hosted_logs,frames_not_hosted,vm_rss_bytes,docker_mem_bytes" > "$rss"
    rig_down
    local n name id hosted dropped vmrss mem
    for n in ${POINTS//,/ }; do
        mp_say "--models $n (load $(mp_load_average))"
        ( cd "$MOIRAI_ROOT" && "$RIG" --models "$n" --no-load --no-dashboard >/dev/null ) \
            || mp_die "the rig did not come up at --models $n"
        sleep "$IDLE_SECS"
        # The replicas: the scaled `node` service, the two editors and the
        # islander; not the bootnode, whose name also says `node`.
        for name in $(mp_moirai_containers | grep -E '^moirai-(node-[0-9]+|editor-[ab]-1|islander-1)$' | sort); do
            id=$(docker exec "$name" sh -c 'echo "${REPLICA_ID:-$HOSTNAME}"')
            hosted=$(container_metric "$name" hosted_logs)
            dropped=$(container_metric "$name" frames_not_hosted)
            vmrss=$(container_rss "$name")
            mem=$(docker stats --no-stream --format '{{.MemUsage}}' "$name" | cut -d/ -f1 \
                | awk '{v=$1; if (v ~ /GiB/) m=1073741824; else if (v ~ /MiB/) m=1048576; else if (v ~ /KiB/) m=1024; else m=1; gsub(/[A-Za-z]/, "", v); printf "%d", v * m}')
            printf '%s,%s,%s,%s,%s,%s,%s\n' "$n" "$name" "$id" "$hosted" "$dropped" "$vmrss" "$mem" >> "$rss"
        done
        rig_down
    done
    mp_say "wrote $rss"
    # The median VmRSS over the `node` replicas at each N goes into the
    # plan's `rss_bytes` column of results.csv, when that file exists.
    if [ -f "$results" ]; then
        python3 - "$results" "$rss" <<'PY'
import csv, statistics, sys
results, rss = sys.argv[1], sys.argv[2]
by_n = {}
for row in csv.DictReader(open(rss)):
    if 'node' in row['container']:
        by_n.setdefault(row['n_logs'], []).append(int(row['vm_rss_bytes']))
rows = list(csv.DictReader(open(results)))
fields = rows[0].keys()
for row in rows:
    values = by_n.get(row['n_logs'])
    row['rss_bytes'] = str(int(statistics.median(values))) if values else ''
with open(results, 'w', newline='') as out:
    writer = csv.DictWriter(out, fieldnames=list(fields))
    writer.writeheader()
    writer.writerows(rows)
PY
        mp_say "filled rss_bytes in $results with the median node VmRSS per N"
    fi
}

# --- manifest --------------------------------------------------------------

write_manifest() {
    {
        mp_manifest_common "$stamp" "PHASE=$PHASE POINTS=$POINTS LOOKUPS=$LOOKUPS IDLE_SECS=$IDLE_SECS ./run.sh"
        if [ "$PHASE" != inprocess ]; then mp_manifest_image; fi
        printf '%-14s%s\n' points "$POINTS (hosted logs per node)"
        printf '%-14s%s\n' lookups "$LOOKUPS per path per N, each timed on its own with Instant"
        printf '%-14s%s\n' log_type "EWFlagSetLog<String>, empty; the per-log cost is the causal bookkeeping"
        printf '%-14s%s\n' memory "deep size per map entry, the member table counted once; B-tree node slack not charged; the CRDT log's heap is empty at N empty logs"
        printf '%-14s%s\n' rss "VmRSS of PID 1 (network_node) per container after ${IDLE_SECS}s idle, rig --models N --no-load --no-dashboard; results.csv carries the median over the node replicas"
        printf '%-14s%s\n' thresholds "p50 < 1 us at N=64 (hit and miss); p50(64) <= 2 x p50(1); m(N) <= m(1) + (N-1) x 2 x s; one member table"
        local verdict
        for verdict in "${verdicts[@]}"; do printf '%-14s%s\n' verdicts "$verdict"; done
        if [ "${#crossed[@]}" -gt 0 ]; then
            printf '%-14s%s\n' crossed "${crossed[*]}"
        else
            printf '%-14s%s\n' crossed "none"
        fi
    } > "$HERE/manifest.txt"
    cat "$HERE/manifest.txt"
}

case "$PHASE" in
    inprocess) phase_inprocess ;;
    rig) phase_rig ;;
    all) phase_inprocess; phase_rig ;;
    *) mp_die "PHASE must be inprocess, rig or all" ;;
esac
write_manifest
[ "${#crossed[@]}" -eq 0 ]
