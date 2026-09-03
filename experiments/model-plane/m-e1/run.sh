#!/usr/bin/env bash
#
# M-E1 — wire overhead of `log_id` per frame.
#
# Two readings. The first is a `#[test]` in `moirai-protocol`
# (`broadcast/message.rs`, `wire_overhead`): a `Message` serialised in its
# compact JSON form carries `log_id` exactly once, and removing that member
# takes exactly 44 bytes off the frame. The test prints the frame length.
#
# The second is the frame a real workload puts on the wire. Three replicas
# from the rig image, peered statically; the dashboard's `--random` driver,
# which shares its generator with the e2e suite's `r1` scenario, applies
# OPERATIONS seeded operations across them; a probe (`wire.py`) peered with
# each replica reads every `Event` frame it broadcasts and records its
# length. The overhead is stated as a percentage of the median frame, which
# has no threshold: it is the baseline any later optimisation is measured
# against.
#
# Usage, from this directory: ./run.sh
# Knobs: SEED (default 20260903), OPERATIONS (180, as r1), RATE (ops/s, 50).

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

SEED=${SEED:-20260903}
OPERATIONS=${OPERATIONS:-180}
RATE=${RATE:-50}
REPLICAS=3

stamp=$(mp_stamp)
out="$HERE/results.csv"
net="moirai-m-e1-$stamp"

teardown() {
    local rc=$?
    [ "$rc" -eq 0 ] || mp_dump_logs "$net"
    docker ps -aq --filter "network=$net" | xargs -r docker rm --force --volumes >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
}
trap teardown EXIT

mp_require_docker
mp_assert_no_moirai_containers

# --- the test, and the frame length it prints -------------------------------

mp_say "the #[test] (load $(mp_load_average))"
test_out=$( cd "$MOIRAI_ROOT" && nice -n 10 cargo test -q -p moirai-protocol --features serde -j 2 \
    wire_overhead -- --nocapture 2>&1 )
echo "$test_out" | grep -E "M-E1|test result" >&2
test_frame=$(echo "$test_out" | grep -o "M-E1 event frame: [0-9]*" | grep -o "[0-9]*$")
echo "$test_out" | grep -q "test result: ok. 3 passed" || mp_die "the wire_overhead tests did not pass"

# --- the workload on the wire ----------------------------------------------

docker network create "$net" >/dev/null
names=()
for i in $(seq 1 $REPLICAS); do names+=("$net-r$i"); done
peers_for() {
    local self="$1" spec="" name
    for name in "${names[@]}"; do
        [ "$name" = "$self" ] && continue
        spec="${spec:+$spec,}${name##*-}:$name:9001"
    done
    echo "$spec"
}
for name in "${names[@]}"; do
    mp_start_node "$net" "$name" "${name##*-}" "$(peers_for "$name")"
done
for name in "${names[@]}"; do mp_wait_healthy "$name"; done

http_targets=""
sync_addrs=()
for name in "${names[@]}"; do
    http_targets="${http_targets:+$http_targets,}http://$name:8081"
    sync_addrs+=("$(mp_sync_of "$name")")
done

mp_say "$REPLICAS replicas up; $OPERATIONS operations from seed $SEED at $RATE/s (load $(mp_load_average))"
python3 "$HERE/measure.py" \
    --http "$(for n in "${names[@]}"; do printf 'http://%s,' "$(mp_http_of "$n")"; done | sed 's/,$//')" \
    --sync "$(IFS=,; echo "${sync_addrs[*]}")" \
    --driver "docker run --rm --network $net $MP_IMAGE moirai-dashboard --random --seed $SEED --rate $RATE --count $OPERATIONS --nodes $http_targets" \
    --seed "$SEED" --operations "$OPERATIONS" --test-frame-bytes "$test_frame" \
    --out "$out"

{
    mp_manifest_common "$stamp" "SEED=$SEED OPERATIONS=$OPERATIONS RATE=$RATE ./run.sh"
    mp_manifest_image
    printf '%-14s%s\n' replicas "$REPLICAS, static PEERS, one probe connection each"
    printf '%-14s%s\n' seed "$SEED (moirai-dashboard --random, the r1 generator; replay: MOIRAI_E2E_SEED=$SEED on r1)"
    printf '%-14s%s\n' operations "$OPERATIONS at $RATE/s across the session"
    printf '%-14s%s\n' frames "compact JSON lines read off the replication socket by wire.py; Event frames only"
    printf '%-14s%s\n' test_frame "$test_frame bytes for the minimal Event frame of the #[test]"
    printf '%-14s%s\n' threshold "log_id costs exactly 44 bytes per frame (asserted by the test and re-read from every frame); the percentage has none"
} > "$HERE/manifest.txt"
cat "$HERE/manifest.txt"
