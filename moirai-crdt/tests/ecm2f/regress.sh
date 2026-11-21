#!/bin/bash
# EC.M2f's regression guard R (spec: cea-cdrt-knowledge/spec/items/EC.M2f.md).
# EC.M2e's guard (../ecm2e/regress.sh) held to 4a42422 instead of 41d8ead: runs EC.M2's
# check.sh once from this worktree (S1..S7, B1..B3, L1 fmt, L2 clippy), then compares it
# with the same script's run at 4a42422, kept here as base-names.txt and base-fmt.txt.
#   R1  S1..S7, B1..B3 and L2 exit 0;
#   R2  every baseline line `<suite> <target> :: <test> ok|ignored` is observed verbatim
#       (EC.M2d's N8, EC.M2e's N1 and Léo's list tests among them), except $MOVED, the
#       test inline-hunk.patch deletes (re-ruled in ecm2_n4_concurrent.rs), which must be gone;
#   R3  every observed test absent from the baseline is one of this item's oracle
#       (an `S1 ecm2_n4_*` line);
#   R4  fmt reports no error and no file it did not report at 4a42422.
# Exit 0 when all hold, 1 otherwise.
#
#   ECM2_SCRATCH=<dir> moirai-crdt/tests/ecm2f/regress.sh
#   ECM2F_COMPARE_ONLY=1 ECM2_SCRATCH=<dir> ...   judges <dir>'s earlier run without running it
set -uo pipefail
export LC_ALL=C CARGO_NET_OFFLINE=true

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
OUT=${ECM2_SCRATCH:?ECM2_SCRATCH names the work folder}/ecm2-check
MOVED='S1 moirai_crdt :: list::nested_list::tests::concurrent_update_delete_insert_2'

if [ "${ECM2F_COMPARE_ONLY:-}" != 1 ]; then
    "$REPO/moirai-interp/tests/ecm2/check.sh" >"$OUT.stdout" 2>&1
    echo "check.sh exit $? (its own names verdict is not used)"
fi
status=0

for s in S1 S2 S3 S4 S5 S6 S7 B1 B2 B3 L2; do
    grep -qx "$s exit 0" "$OUT.stdout" || { echo "R1 FAIL: $(grep "^$s exit" "$OUT.stdout" || echo "$s not run")"; status=1; }
done

lost=$(grep -E ' (ok|ignored)$' "$HERE/base-names.txt" | grep -vxF "$MOVED ok" | comm -23 - "$OUT/names.txt")
if grep -q "^$MOVED " "$OUT/names.txt"; then
    echo "R2 FAIL: still run: $(grep "^$MOVED " "$OUT/names.txt")"
    status=1
fi
if [ -n "$lost" ]; then
    echo "R2 FAIL: $(grep -c . <<<"$lost") baseline lines missing or changed (first 40):"
    head -40 <<<"$lost"
    status=1
fi

new=$(awk 'NR==FNR { k=$0; sub(/ [^ ]+$/, "", k); seen[k]=1; next }
           { k=$0; sub(/ [^ ]+$/, "", k); if (!(k in seen)) print }' "$HERE/base-names.txt" "$OUT/names.txt")
alien=$(grep -vE '^S1 ecm2_n4_[a-z0-9_]+ :: ' <<<"$new" | grep . || true)
if [ -n "$alien" ]; then
    echo "R3 FAIL: $(grep -c . <<<"$alien") tests not in the baseline and not this item's oracle (first 40):"
    head -40 <<<"$alien"
    status=1
fi

fmt=$(grep -oE "^Diff in $REPO/[^ :]+\.rs" "$OUT/L1.log" | sed "s|^Diff in $REPO/||" | sort -u | comm -23 - "$HERE/base-fmt.txt")
if [ -n "$fmt" ] || grep -q '^error' "$OUT/L1.log"; then
    echo "R4 FAIL: fmt reports an error or files it did not report at 4a42422:"
    echo "$fmt"
    grep -m5 '^error' "$OUT/L1.log"
    status=1
fi

echo "R: $(grep -c . "$OUT/names.txt") test lines, $(grep -c . <<<"$new") not in the baseline"
[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
