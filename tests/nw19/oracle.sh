#!/usr/bin/env bash
# NW.19, the whole oracle in its registered order (vault spec/items/NW.19.md, section 6), one
# verdict line per claim point in <out>/verdicts.txt; each command once, nothing rerun.
#
#   tests/nw19/oracle.sh <spec-commit> <baseline-A-dir> <out-dir>
#
#   P0 P1 P7  tests/nw19/static.sh <spec> <out>/static
#   P2 P3 P6  tests/nw19/regression.sh run <out>/regression, compared with baseline A (new failures
#             by part: b/ r1/ r2/ are P2, p3/ is P3, p6/ is P6) and judged by account.py --mode h
#             against tests/nw19/pins.tsv (failing lines by the step they name)
#   then alone (no cargo, rustc or Docker build; the scripts refuse otherwise):
#   P4        NW19_IMAGE_KEEP=1 tests/nw19/containers.sh <out>/containers   (builds :nw19 from HEAD)
#   P5        tests/nw19/stack.sh <out>/stack ZT                            (on :nw19)
#   and :nw19 removed. Exit 0 only when every point passes.
set -uo pipefail
export LC_ALL=C
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SPEC="${1:?spec commit}" BASE_A="${2:?baseline A dir}" OUT="${3:?out dir}"
[ -e "$OUT" ] && { echo "NW.19 ORACLE REFUSED: $OUT exists"; exit 2; }
mkdir -p "$OUT"
V="$OUT/verdicts.txt"
verdict() { printf '%s %s %s\n' "$1" "$2" "$3" | tee -a "$V"; }
"$HERE/static.sh" "$SPEC" "$OUT/static" > "$OUT/static.log" 2>&1; rc=$?
verdict P0+P1+P7 "$([ $rc = 0 ] && echo PASS || echo FAIL)" "$(tail -1 "$OUT/static.log"); $(grep -c '^FAIL' "$OUT/static.log") failing: $(grep '^FAIL' "$OUT/static.log" | cut -c1-120 | tr '\n' '|')"
"$HERE/regression.sh" run "$OUT/regression" > "$OUT/regression.log" 2>&1
"$HERE/regression.sh" compare "$BASE_A" "$OUT/regression" > "$OUT/compare.txt" 2>&1
python3 -I "$HERE/account.py" judge "$OUT/regression/watch/nodes.jsonl" "$HERE/pins.tsv" --mode h > "$OUT/account.txt" 2>&1
for point in "P2:b/ r1/ r2/" "P3:p3/" "P6:p6/"; do
    name="${point%%:*}"; bad=0; lines=""
    for prefix in ${point#*:}; do
        n="$(grep -E "^NEW (STEP|TEST) $prefix" "$OUT/compare.txt" | wc -l)"
        m="$(grep -E "^FAIL W[0-9]+ .*step $prefix" "$OUT/account.txt" | wc -l)"
        bad=$((bad + n + m))
        lines="$lines$(grep -E "^NEW (STEP|TEST) $prefix|^FAIL W[0-9]+ .*step $prefix" "$OUT/compare.txt" "$OUT/account.txt" | cut -c1-160 | tr '\n' '|')"
    done
    verdict "$name" "$([ $bad = 0 ] && echo PASS || echo FAIL)" "$bad failing lines $lines"
done
other="$(grep -E '^FAIL W' "$OUT/account.txt" | grep -vE 'step (b|r1|r2|p3|p6)/' | cut -c1-160 | tr '\n' '|')"
verdict ACCOUNT "$( [ -z "$other" ] && grep -q 'ACCOUNT (h): PASS' "$OUT/account.txt" && echo PASS || echo FAIL)" "$(tail -1 "$OUT/account.txt") $other"
NW19_IMAGE_KEEP=1 "$HERE/containers.sh" "$OUT/containers" > "$OUT/containers.log" 2>&1; rc=$?
verdict P4 "$([ $rc = 0 ] && echo PASS || echo FAIL)" "$(tail -1 "$OUT/containers.log") $(grep '^FAIL' "$OUT/containers.log" | cut -c1-160 | tr '\n' '|')"
"$HERE/stack.sh" "$OUT/stack" ZT > "$OUT/stack.log" 2>&1; rc=$?
verdict P5 "$([ $rc = 0 ] && echo PASS || echo FAIL)" "$(tail -1 "$OUT/stack.log") $(grep '^FAIL' "$OUT/stack.log" | cut -c1-160 | tr '\n' '|')"
docker image rm moirai-json-crdt-model-plane:nw19 > "$OUT/image-rm.log" 2>&1 && echo "image moirai-json-crdt-model-plane:nw19 removed" >> "$V"
if grep -qE '^P[0-9+]* FAIL|^ACCOUNT FAIL' "$V"; then echo "NW.19 ORACLE: FAIL" | tee -a "$V"; exit 1; fi
echo "NW.19 ORACLE: PASS" | tee -a "$V"; exit 0
