#!/bin/bash
# EC.03c2c1's oracle (spec: cea-cdrt-knowledge/spec/items/EC.03c2c.md): 748a185's
# si and st node-cluster targets by name on this line. Three checks, every one run:
#   C1 moirai-interp/Cargo.toml has EC.03c2b's six [[test]] entries and this
#      part's six, nothing else; default features are 748a185's.
#   C2 moirai-interp's tests build under its default features.
#   C3 the 15 carried targets under --features network, run on this line's
#      release model_node under the loopback shim (ec03c2/loopback_bind.c):
#      every name with 748a185's outcome (names.txt, run on 748a185 itself),
#      no E2E-SKIP, every bind on loopback, none on a scouting port, no node left.
# Exit 0 when all hold, 1 otherwise, 75 when the node lock or the wait for
# other lanes' nodes runs out (environment, not run).
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c2c/check.sh
set -uo pipefail
# names.txt is sorted under C; pin the locale so sort and diff ignore the caller's.
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
OUT=$SCRATCH/ec03c2c-check
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
NODES_LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/nodes.lock
ENTRIES="si12_fixtures si12_copy_migration si13_concurrent_migrations si14_fixtures si14_rebase
si16_rebased_joiner si12_copy_migration_l3 si12_scale_l3 si13_copy_in_progress si13_scale_l3
si14_rebase_l3 si16_rebased_joiner_l3"
TARGETS=$(awk '{print $1}' "$HERE/names.txt" | sort -u)
NODE=$SCRATCH/target/release/examples/model_node

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

rm -rf "$OUT" && mkdir -p "$OUT/tmp"
status=0

python3 -I - "$REPO/moirai-interp/Cargo.toml" $ENTRIES <<'PY' || status=1
import sys, tomllib
manifest = tomllib.load(open(sys.argv[1], "rb"))
want = sorted(sys.argv[2:])
got = manifest.get("test", [])
ok = sorted(got, key=lambda t: t.get("name", "")) == [{"name": n, "required-features": ["network"]} for n in want]
default = manifest.get("features", {}).get("default")
print(f"C1 [[test]] entries: {'ok' if ok else 'FAIL'}: {sorted(t.get('name') for t in got)}")
print(f"C1 default features: {'ok' if default == ['serde', 'test_utils'] else 'FAIL'}: {default}")
sys.exit(0 if ok and default == ["serde", "test_utils"] else 1)
PY

cargo_here test -j 2 --offline -p moirai-interp --no-run >"$OUT/default.log" 2>&1
code=$?
echo "C2 default features build: exit $code"
[ "$code" = 0 ] || status=1

cc -shared -fPIC -O2 -o "$OUT/loopback_bind.so" "$HERE/../ec03c2/loopback_bind.c" -ldl \
    || { echo "C3 FAIL: the shim did not build"; exit 1; }
args=()
for t in $TARGETS; do args+=(--test "$t"); done
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

# Every target binary runs serially, holding the node lock, once no other node runs.
run_all() {
    for i in $(seq 60); do pgrep -x model_node >/dev/null || break; sleep 30; done
    pgrep -x model_node >/dev/null && { echo "ENV: another lane's node stayed 30 minutes"; return 75; }
    for t in $TARGETS; do
        exe=$(grep -E "Executable tests/$t\.rs " "$OUT/build.log" | sed -E 's/.*\((.*)\)$/\1/')
        [ -x "$exe" ] || { echo "$t: no test binary" >"$OUT/run-$t.log"; continue; }
        (cd "$REPO/moirai-interp" && env -u MOIRAI_TRANSPORT -u ZENOH_SCOUTING -u ZENOH_ROUTER \
            -u ZENOH_MODE -u ZENOH_CONFIG -u BOOTNODE_URL -u PEERS -u RUST_LOG \
            TMPDIR="$OUT/tmp" MOIRAI_E2E_INTERP_NODE_BIN="$NODE" \
            ED16A_DESCRIPTOR="$REPO/moirai-interp/tests/fixtures/ed16a/notation.metamodel.json" \
            LD_PRELOAD="$OUT/loopback_bind.so" EC03C2_BIND_LOG="$OUT/binds.log" \
            "$exe" --test-threads=1) >"$OUT/run-$t.log" 2>&1
        echo "$t: exit $?"
    done
}
export -f run_all
export OUT TARGETS NODE REPO
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
