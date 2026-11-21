#!/bin/bash
# EC.M2d's regression guard R (spec: cea-cdrt-knowledge/spec/items/EC.M2d.md).
# Runs EC.M2's check.sh once from this worktree (S1..S7, B1..B3, L1 fmt, L2 clippy),
# then holds it to the same script's run at b1e254c, kept here as base-names.txt and
# base-fmt.txt. check.sh's own names verdict compares with EC.M2's names.txt, taken at M
# before later items added tests, so it is not used.
#   R1  S1..S7, B1..B3 and L2 exit 0;
#   R2  every baseline line `<suite> <target> :: <test> ok|ignored` is observed verbatim;
#   R3  every observed test absent from the baseline is one of this item's oracle
#       (an `S1 ecm2_n8_*` line);
#   R4  fmt reports no error and no file it did not report at b1e254c.
# Exit 0 when all hold, 1 otherwise.
#
#   ECM2_SCRATCH=<dir> moirai-crdt/tests/ecm2d/regress.sh
#   ECM2D_COMPARE_ONLY=1 ECM2_SCRATCH=<dir> ...   judges <dir>'s earlier run without running it
set -uo pipefail
export LC_ALL=C CARGO_NET_OFFLINE=true

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
OUT=${ECM2_SCRATCH:?ECM2_SCRATCH names the work folder}/ecm2-check

if [ "${ECM2D_COMPARE_ONLY:-}" != 1 ]; then
    "$REPO/moirai-interp/tests/ecm2/check.sh" >"$OUT.stdout" 2>&1
    echo "check.sh exit $? (its own names verdict is not used)"
fi
status=0

for s in S1 S2 S3 S4 S5 S6 S7 B1 B2 B3 L2; do
    grep -qx "$s exit 0" "$OUT.stdout" || { echo "R1 FAIL: $(grep "^$s exit" "$OUT.stdout" || echo "$s not run")"; status=1; }
done

lost=$(grep -E ' (ok|ignored)$' "$HERE/base-names.txt" | comm -23 - "$OUT/names.txt")
if [ -n "$lost" ]; then
    echo "R2 FAIL: $(grep -c . <<<"$lost") baseline lines missing or changed (first 40):"
    head -40 <<<"$lost"
    status=1
fi

new=$(awk 'NR==FNR { k=$0; sub(/ [^ ]+$/, "", k); seen[k]=1; next }
           { k=$0; sub(/ [^ ]+$/, "", k); if (!(k in seen)) print }' "$HERE/base-names.txt" "$OUT/names.txt")
alien=$(grep -vE '^S1 ecm2_n8_[a-z0-9_]+ :: ' <<<"$new" | grep . || true)
if [ -n "$alien" ]; then
    echo "R3 FAIL: $(grep -c . <<<"$alien") tests not in the baseline and not this item's oracle (first 40):"
    head -40 <<<"$alien"
    status=1
fi

fmt=$(grep -oE "^Diff in $REPO/[^ :]+\.rs" "$OUT/L1.log" | sed "s|^Diff in $REPO/||" | sort -u | comm -23 - "$HERE/base-fmt.txt")
if [ -n "$fmt" ] || grep -q '^error' "$OUT/L1.log"; then
    echo "R4 FAIL: fmt reports an error or files it did not report at b1e254c:"
    echo "$fmt"
    grep -m5 '^error' "$OUT/L1.log"
    status=1
fi

echo "R: $(grep -c . "$OUT/names.txt") test lines, $(grep -c . <<<"$new") not in the baseline"
[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
