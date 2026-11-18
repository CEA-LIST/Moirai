#!/bin/bash
# EC.03c2c2's oracle (spec: cea-cdrt-knowledge/spec/items/EC.03c2c2.md): 748a185's
# fourteen md03 and nw network targets by name on this line. Every check runs:
#   C1 moirai-interp/Cargo.toml: EC.03c2c's twelve [[test]] entries, nothing else;
#      the dev-dependencies are rand_chacha, reqwest and 748a185's zenoh line;
#      default features are 748a185's.
#   C2 the fourteen targets build under moirai-interp's default features.
#   C3 the fourteen targets under --features network, run on this line's release
#      model_node under the loopback shim (ec03c2/loopback_bind.c), skip.txt's
#      names left out: every name with 748a185's outcome (names.txt, the same
#      script run on 748a185 itself), each binary's list equal to names.txt's
#      names plus skip.txt's, no E2E-SKIP, every bind on loopback, none on a
#      scouting port, no node left.
# Exit 0 when all hold, 1 otherwise, 75 when the node lock, the wait for other
# lanes' nodes, or NW.27's fixed work folder is not free (environment, not run).
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c2c2/check.sh
set -uo pipefail
# names.txt is sorted under C; pin the locale so sort and diff ignore the caller's.
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
OUT=$SCRATCH/ec03c2c2-check
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
NODES_LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/nodes.lock
# nw27/mod.rs's WORK, 748a185's constant: nw27_model_replication's TMPDIR must be under it.
NW27=/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.27/work
ENTRIES="si12_fixtures si12_copy_migration si13_concurrent_migrations si14_fixtures si14_rebase
si16_rebased_joiner si12_copy_migration_l3 si12_scale_l3 si13_copy_in_progress si13_scale_l3
si14_rebase_l3 si16_rebased_joiner_l3"
TARGETS="md03_replica_ids nw02_loopback_ports nw03_joiner_writes_l3 nw04_joiner_with_history_l3
nw07_named_model_ids_l3 nw11_http_origin_policy_l2 nw16a_opcount_l3 nw16a_peer_mode_l3
nw16a_scouting_l3 nw16a_state_transfer_size_l3 nw16a_witness_probe nw17_large_message_l3
nw27_model_replication nw27_typed_decode"
NODE=$SCRATCH/target/release/examples/model_node

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

rm -rf "$OUT" && mkdir -p "$OUT/tmp"
status=0
args=()
for t in $TARGETS; do args+=(--test "$t"); done

python3 -I - "$REPO/moirai-interp/Cargo.toml" $ENTRIES <<'PY' || status=1
import sys, tomllib
manifest = tomllib.load(open(sys.argv[1], "rb"))
want = sorted(sys.argv[2:])
got = manifest.get("test", [])
ok = sorted(got, key=lambda t: t.get("name", "")) == [{"name": n, "required-features": ["network"]} for n in want]
dev = manifest.get("dev-dependencies", {})
zenoh = {"version": "=1.10.1", "default-features": False, "features": ["transport_tcp", "unstable"]}
dev_ok = sorted(dev) == ["rand_chacha", "reqwest", "zenoh"] and dev.get("zenoh") == zenoh
default = manifest.get("features", {}).get("default")
print(f"C1 [[test]] entries: {'ok' if ok else 'FAIL'}: {sorted(t.get('name') for t in got)}")
print(f"C1 dev-dependencies: {'ok' if dev_ok else 'FAIL'}: {sorted(dev)}, zenoh {dev.get('zenoh')}")
print(f"C1 default features: {'ok' if default == ['serde', 'test_utils'] else 'FAIL'}: {default}")
sys.exit(0 if ok and dev_ok and default == ["serde", "test_utils"] else 1)
PY

cargo_here test -j 2 --offline -p moirai-interp "${args[@]}" --no-run >"$OUT/default.log" 2>&1
code=$?
echo "C2 default features build: exit $code"
[ "$code" = 0 ] || { grep -m5 -E '^error' "$OUT/default.log"; status=1; }

cc -shared -fPIC -O2 -o "$OUT/loopback_bind.so" "$HERE/../ec03c2/loopback_bind.c" -ldl \
    || { echo "C3 FAIL: the shim did not build"; exit 1; }
cargo_here build -j 2 --offline --release -p moirai-interp --features network --example model_node \
    >"$OUT/build.log" 2>&1 \
    && cargo_here test -j 2 --offline --release -p moirai-interp --features network "${args[@]}" \
        --no-run >>"$OUT/build.log" 2>&1
code=$?
echo "C3 build: exit $code"
if [ "$code" != 0 ]; then
    grep -m5 -E '^error' "$OUT/build.log"
    echo FAIL; exit 1
fi

[ -e "$NW27" ] && { echo "ENV: $NW27 exists (another run's); not run"; exit 75; }

# Every target binary runs serially, holding the node lock, once no other node runs.
run_all() {
    for i in $(seq 60); do pgrep -x model_node >/dev/null || break; sleep 30; done
    pgrep -x model_node >/dev/null && { echo "ENV: another lane's node stayed 30 minutes"; return 75; }
    mkdir -p "$NW27/tmp"
    for t in $TARGETS; do
        exe=$(grep -E "Executable tests/$t\.rs " "$OUT/build.log" | sed -E 's/.*\((.*)\)$/\1/')
        [ -x "$exe" ] || { echo "$t: no test binary" >"$OUT/run-$t.log"; continue; }
        "$exe" --list >"$OUT/list-$t.txt" 2>&1
        skips=()
        for n in $(awk -v t="$t" '$1 == t {print $2}' "$HERE/skip.txt"); do skips+=(--skip "$n"); done
        tmp=$OUT/tmp
        [ "$t" = nw27_model_replication ] && tmp=$NW27/tmp
        (cd "$REPO/moirai-interp" && env -u MOIRAI_TRANSPORT -u ZENOH_SCOUTING -u ZENOH_ROUTER \
            -u ZENOH_MODE -u ZENOH_CONFIG -u BOOTNODE_URL -u PEERS -u RUST_LOG -u NW07_SCRATCH \
            -u NW11_SCRATCH -u NW27_CASE -u NW16A_ISOLATED_NETNS \
            TMPDIR="$tmp" MOIRAI_E2E_INTERP_NODE_BIN="$NODE" NW27_LOOPBACK_SHIM="$OUT/loopback_bind.so" \
            LD_PRELOAD="$OUT/loopback_bind.so" EC03C2_BIND_LOG="$OUT/binds.log" \
            "$exe" --test-threads=1 --exact "${skips[@]}") >"$OUT/run-$t.log" 2>&1
        echo "$t: exit $?"
    done
    rm -rf "$NW27"
}
export -f run_all
export OUT TARGETS NODE REPO HERE NW27
flock -E 75 -w 3600 "$NODES_LOCK" bash -c run_all
code=$?
[ "$code" = 75 ] && { echo "ENV: not run (node lock)"; exit 75; }

# `<target> :: <name> <ok|FAILED|ignored>`, one line per test a target ran.
for t in $TARGETS; do
    awk -v t="$t" '/^test .* \.\.\. /{o=$NF; if ($0 ~ /\.\.\. ignored/) o="ignored"; print t" :: "$2" "o}' \
        "$OUT/run-$t.log"
done | sort >"$OUT/names.txt"
if diff "$HERE/names.txt" "$OUT/names.txt" >"$OUT/names.diff"; then
    echo "C3 names: $(wc -l <"$OUT/names.txt") equal to names.txt"
else
    echo "C3 names: FAIL, differences (< expected, > observed):"
    cat "$OUT/names.diff"
    status=1
fi
# Each binary's own list is names.txt's names plus skip.txt's: nothing is dropped unnamed.
for t in $TARGETS; do
    sed -nE "s/^(.*): test$/$t \1/p" "$OUT/list-$t.txt"
done | sort >"$OUT/listed.txt"
{ awk '{print $1, $3}' "$HERE/names.txt"; awk '{print $1, $2}' "$HERE/skip.txt"; } | sort >"$OUT/expected-list.txt"
if cmp -s "$OUT/expected-list.txt" "$OUT/listed.txt"; then
    echo "C3 list: $(wc -l <"$OUT/listed.txt") names, names.txt plus skip.txt"
else
    echo "C3 FAIL: the binaries list other names than names.txt plus skip.txt"
    diff "$OUT/expected-list.txt" "$OUT/listed.txt"
    status=1
fi
grep -l 'E2E-SKIP' "$OUT"/run-*.log && { echo "C3 FAIL: a scenario skipped"; status=1; }
if [ ! -s "$OUT/binds.log" ]; then
    echo "C3 FAIL: the shim logged no bind"; status=1
elif awk '($2 !~ /^127\./ && $2 != "::1") || $3 == 7446 || $3 == 7447 || $3 == 7450' "$OUT/binds.log" | grep -q .; then
    echo "C3 FAIL: a bind beyond loopback or on a scouting port"; status=1
fi
echo "C3 binds: $(wc -l <"$OUT/binds.log" 2>/dev/null || echo 0), $(awk '$4 == "rewritten"' "$OUT/binds.log" 2>/dev/null | wc -l) rewritten from the wildcard"
left=$(for p in $(pgrep -x model_node); do readlink "/proc/$p/exe"; done | grep -cF "$NODE")
[ "$left" = 0 ] || { echo "C3 FAIL: $left node(s) of this run left running"; status=1; }

[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
