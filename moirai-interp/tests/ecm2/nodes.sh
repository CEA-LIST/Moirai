#!/bin/bash
# EC.M2 piece 2 (spec: cea-cdrt-knowledge/spec/items/EC.M2b.md): the node and network
# targets of EC.03c2a, EC.03c2c, EC.03c2c2, EC.03c3a and EC.03c3b1 and the node
# differential, each by its registered script, unchanged, once, on this tree:
#   e2e   ec03c2/e2e.sh        c2c  ec03c2c/check.sh   c2c2 ec03c2c2/check.sh
#   c3a   ec03c3a/check.sh     c3b1 ec03c3b1/check.sh  diff ec03c2/run.sh
# e2e.sh and run.sh start nodes without taking nodes.lock, so they run under it here;
# the check scripts take it themselves. Each script's verdict lines (C1 to C3, PASS,
# FAIL, ENV, S1 to S5, e2e_interp, reference and candidate lines; not the bind counts
# or timings) and its exit code are compared with nodes-expect.txt: C3 counts from each
# script's names.txt (748a185's runs), e2e and diff lines from EC.03c2's record, C1 lines
# as the guide 71384ea's manifests give them, except c3a's workspace and Cargo.lock lines,
# which fail on M because both files are ours (EC.M2 section 4); see EC.M2b.md.
# Exit 0 every label equal, 1 a difference (printed), 75 a script reported the
# environment (node lock), 2 run.sh's instrument failed on the reference.
#
#   ECM2_SCRATCH=<dir> moirai-interp/tests/ecm2/nodes.sh [label...]
# <dir> needs target/ and ec03-run/schedules/ (ec03/run.sh's); no label runs all six.
set -uo pipefail
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
T=$(cd "$HERE/.." && pwd)
S=${ECM2_SCRATCH:?ECM2_SCRATCH names the work folder}
NODES_LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/nodes.lock
LABELS=${*:-e2e c2c c2c2 c3a c3b1 diff}
OUT=$S/ecm2-nodes
mkdir -p "$OUT"
: >"$OUT/observed.txt"

worst=0
for l in $LABELS; do
    case $l in
        e2e) cmd=(flock -E 75 -w 3600 "$NODES_LOCK" "$T/ec03c2/e2e.sh") ;;
        c2c | c2c2 | c3a | c3b1) cmd=("$T/ec03${l}/check.sh") ;;
        diff) cmd=(flock -E 75 -w 3600 "$NODES_LOCK" "$T/ec03c2/run.sh") ;;
        *) echo "unknown label $l"; exit 1 ;;
    esac
    echo "== $l: ${cmd[*]}"
    EC03_SCRATCH=$S "${cmd[@]}" >"$OUT/$l.log" 2>&1
    code=$?
    echo "$l: exit $code"
    { grep -E '^(C[0-9] |PASS|FAIL|ENV|S[0-9]|e2e_interp \(|reference \(|candidate lines)' "$OUT/$l.log" \
        | grep -v '^C3 binds:' | sed -E 's/; finished in [0-9.]+s$//'
      echo "exit $code"; } | sed "s/^/$l /" >>"$OUT/observed.txt"
    [ "$code" = 75 ] && worst=75
    [ "$code" = 2 ] && [ "$l" = diff ] && [ "$worst" != 75 ] && worst=2
done

pattern=$(echo "$LABELS" | tr ' ' '\n' | sed 's/.*/^& /' | paste -sd'|')
grep -E "$pattern" "$HERE/nodes-expect.txt" | sort >"$OUT/expected.txt"
sort -o "$OUT/observed.txt" "$OUT/observed.txt"
if diff "$OUT/expected.txt" "$OUT/observed.txt" >"$OUT/nodes.diff"; then
    echo "PASS: $(wc -l <"$OUT/observed.txt") verdict lines equal to nodes-expect.txt ($LABELS)"
    exit 0
fi
echo "FAIL: verdict lines differ (< expected, > observed):"
cat "$OUT/nodes.diff"
[ "$worst" != 0 ] && exit $worst
exit 1
