#!/bin/bash
# EC.R1a's regression (spec: cea-cdrt-knowledge/spec/items/EC.R1a.md): every test that reads a
# descriptor fixture this item regenerates keeps the outcome it had at 684f817. Three steps, each run once:
#   R1 EC.M2's in-process check, moirai-interp/tests/ecm2/check.sh (S1..S7, B1..B3, L1, L2);
#   R2 EC.03c2c's node-cluster check, moirai-interp/tests/ec03c2c/check.sh (21 targets, release node);
#   R3 the targets of moirai-interp --features network whose build reads a regenerated fixture and
#      that R1 and R2 do not run (the list below, from the targets' dep-info at 684f817, and the five
#      nw targets that read json.metamodel.json through moirai_interp::testing::JSON_DESCRIPTOR).
# Writes <out>/names.txt: `<step> <target> :: <test> <outcome>` and `<step> exit <code>` lines, sorted.
# Given a baseline <out>, compares: exit 1 when a baseline line is missing or changed, or a new
# line names a target other than ecr1a_describe_fixtures.
#
#   [ARACHNE_BIN=<arachne>] moirai-interp/tests/ecr1a/regression.sh <out> [<baseline out>]
set -uo pipefail
export LC_ALL=C
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_RELEASE_DEBUG=0

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
OUT=${1:?the output folder}
BASE=${2:-}
S=/volatile/home/vp242103/.cache/cea-crdt-scratch/EC.R1a
TARGET=/volatile/home/vp242103/.cache/cea-crdt-scratch/EC.R1/target-moirai
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
NODES_LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/nodes.lock
R3="ec03b_host md03_common md03_replica_ids md03_semantics md03a_common md03a_semantics
si05_references_by_identity si05_round3_be10455 si07_h1_harness_read si07_i1_intake si07_p1_dead_source
si08_r2_nested_targets si08_target_side si13_migration_l3
nw03_joiner_writes_l3 nw04_joiner_with_history_l3 nw07_named_model_ids_l3 nw11_http_origin_policy_l2
nw27_model_replication"

names_of() { # step log: check.sh's parser, `<step> <target> :: <name> <outcome>`
    awk -v s="$1" '/^ *Running .*\(.*\/deps\/[^)]*\)$/{b=$NF; sub(/^\(.*\/deps\//,"",b); sub(/\)$/,"",b); sub(/-[0-9a-f]+$/,"",b); t=b}
        /^test .* \.\.\. /{o=$NF; if ($0 ~ /\.\.\. ignored/) o="ignored"; print s" "t" :: "$2" "o}' "$2"
}

rm -rf "$OUT" && mkdir -p "$OUT/tmp" "$S/inproc" "$S/l3"
ln -sfn "$TARGET" "$S/inproc/target"

ECM2_SCRATCH=$S/inproc "$REPO/moirai-interp/tests/ecm2/check.sh" >"$OUT/R1.stdout" 2>&1
grep -E '^[SBL][0-9] exit [0-9]+$' "$OUT/R1.stdout" | sed 's/^/R1 /' >"$OUT/lines.txt"
sed 's/^/R1 /' "$S/inproc/ecm2-check/names.txt" >>"$OUT/lines.txt"
cp -r "$S/inproc/ecm2-check" "$OUT/R1"

EC03_SCRATCH=$S/l3 "$REPO/moirai-interp/tests/ec03c2c/check.sh" >"$OUT/R2.stdout" 2>&1
echo "R2 exit $?" >>"$OUT/lines.txt"
sed 's/^/R2 /' "$S/l3/ec03c2c-check/names.txt" >>"$OUT/lines.txt"
cp -r "$S/l3/ec03c2c-check" "$OUT/R2"

args=(--lib); for t in $R3; do args+=(--test "$t"); done
cc -shared -fPIC -O2 -o "$OUT/loopback_bind.so" "$REPO/moirai-interp/tests/ec03c2/loopback_bind.c" -ldl
(cd "$REPO" && CARGO_TARGET_DIR=$TARGET flock "$LOCK" nice -n 10 cargo build -j 2 --offline \
    -p moirai-relay -p moirai-bootnode && CARGO_TARGET_DIR=$TARGET flock "$LOCK" nice -n 10 \
    cargo test -j 2 --offline -p moirai-interp --features network "${args[@]}" --no-run) >"$OUT/R3.build.log" 2>&1
(cd "$REPO" && env -u MOIRAI_TRANSPORT -u ZENOH_SCOUTING -u ZENOH_ROUTER -u ZENOH_MODE -u ZENOH_CONFIG \
    -u BOOTNODE_URL -u PEERS -u RUST_LOG TMPDIR="$OUT/tmp" CARGO_TARGET_DIR=$TARGET \
    MOIRAI_E2E_INTERP_NODE_BIN="$S/l3/target/release/examples/model_node" \
    MOIRAI_E2E_RELAY_BIN="$TARGET/debug/moirai-relay" MOIRAI_E2E_BOOTNODE_BIN="$TARGET/debug/moirai-bootnode" \
    LD_PRELOAD="$OUT/loopback_bind.so" EC03C2_BIND_LOG="$OUT/binds.log" \
    flock "$NODES_LOCK" nice -n 10 cargo test -j 2 --offline --no-fail-fast -p moirai-interp \
    --features network "${args[@]}" -- --test-threads=1) >"$OUT/R3.log" 2>&1
echo "R3 exit $?" >>"$OUT/lines.txt"
names_of R3 "$OUT/R3.log" >>"$OUT/lines.txt"
sort "$OUT/lines.txt" >"$OUT/names.txt"
echo "names: $(wc -l <"$OUT/names.txt") lines in $OUT/names.txt"
awk '($2 !~ /^127\./ && $2 != "::1") || $3 == 7446 || $3 == 7447 || $3 == 7450' "$OUT/binds.log" 2>/dev/null \
    | grep -q . && echo "R3 note: a bind beyond loopback or on a scouting port (binds.log)"

[ -n "$BASE" ] || exit 0
missing=$(comm -23 "$BASE/names.txt" "$OUT/names.txt")
added=$(comm -13 "$BASE/names.txt" "$OUT/names.txt")
echo "against $BASE: $(printf '%s' "$missing" | grep -c .) baseline lines missing or changed, $(printf '%s' "$added" | grep -c .) new"
printf '%s\n' "$missing" | grep . | sed 's/^/  - /'
printf '%s\n' "$added" | grep . | sed 's/^/  + /'
bad_new=$(printf '%s\n' "$added" | grep . | grep -v ' ecr1a_describe_fixtures :: ')
[ -z "$missing" ] && [ -z "$bad_new" ] && echo PASS && exit 0
echo FAIL
exit 1
