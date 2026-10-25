#!/usr/bin/env bash
# NW.25's oracle (vault spec/items/NW.25.md, section 4): zenoh 1.10.1 carried in the model store's
# workspace with upstream PR #2779 (commit 646f2d1b730e) and nothing else, byte for byte NW.23's copy,
# Arachne's json_crdt patched to it, and no start stall S in the candidate's place.
#
#   tests/nw25/oracle.sh <out-dir>
#
# Five parts, always all five, in this order:
#   1. tests/nw25/structural.sh <out>/structural   S1 to S10 (git, cargo metadata and cargo tree)
#   2. tests/nw25/arachne.sh <out>/arachne         J1 to J4 (git only)
#   3. tests/nw25/image.sh <out>/image             I1 to I4: network_node as the image builds it
#   4. tests/nw25/build.sh <out>/pins              candidate and control model_node, probe; B1, B2
#                                                  (and C1, C2, which qualify the control)
#   5. tests/nw25/load.sh <out>/load <out>/pins    THE LOAD STEP, alone on the machine: CONTROL
#                                                  2,400 and CAND16 4,800 sixteen-node starts
# Exit 0 only when every part passes; otherwise 1 when any part fails, else 2 when a part refused
# to run, else 4 when the load step was inconclusive, else 3 when its control did not qualify.
# The regression (tests/nw25/regression.sh) is run apart, never beside the load step.
set -uo pipefail
export LC_ALL=C
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${1:?out dir}"
mkdir -p "$OUT"
"$HERE/structural.sh" "$OUT/structural"; s=$?
"$HERE/arachne.sh" "$OUT/arachne"; a=$?
"$HERE/image.sh" "$OUT/image"; i=$?
"$HERE/build.sh" "$OUT/pins"; b=$?
if [ "$b" -eq 2 ]; then echo "NW.25 LOAD: not run, the build or the control's qualification failed"; l=2
else "$HERE/load.sh" "$OUT/load" "$OUT/pins"; l=$?; fi
echo "NW.25 ORACLE: structural exit $s, arachne exit $a, image exit $i, build exit $b, load exit $l"
for want in 1 2 4 3; do
    for got in "$s" "$a" "$i" "$b" "$l"; do [ "$got" -eq "$want" ] && exit "$want"; done
done
exit 0
