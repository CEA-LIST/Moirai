#!/usr/bin/env bash
# NW.25, the load step (vault spec/items/NW.25.md, sections 3 and 4). A LOAD STEP: run it alone,
# with no cargo, rustc or docker process anywhere on the machine; it refuses to start otherwise,
# and the harness looks again before every round. Ported from NW.23's tests/nw23/load.sh at
# 57c3f1c, shortened to two arms.
#
#   tests/nw25/load.sh <load-dir> <pins-dir>       (pins from tests/nw25/build.sh)
#
# Two arms in one session, in six interleaved blocks so that both see the same hours of the
# machine: each block runs CONTROL for 25 sixteen-node clusters, then CAND16 for 50 sixteen-node
# clusters, one cluster at a time; in all CONTROL 2,400 and CAND16 4,800 starts. CONTROL is the
# unpatched model_node built by build.sh from 9fbb6fd; CAND16 is the candidate built by build.sh
# from HEAD. Both use the same witness probe and backtrace preload. NW.22's settings: deadline
# 30 s, late watch 40 s, HTTP_BIND_ADDR=127.0.0.1, LOG_ID as ST.05, every node on loopback with
# LISTEN_PORT=0 and HTTP_PORT=0 (never 7447), scouting left off (the code's default). Every capture
# is symbolized against its own binary (signatures.py), then judge.py gives the verdict and the exit
# code (0 PASS, 1 FAIL, 3 UNVERIFIED, 4 INCONCLUSIVE; 2 when a precondition refuses the run).
set -uo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${1:?load dir}" PINS="${2:?pins dir}"
BLOCKS=6 CONTROL_ROUNDS=25 CAND16_ROUNDS=50
refuse() { echo "NW.25 LOAD: REFUSED $*"; exit 2; }

n="$(pgrep -a '^(cargo|rustc|docker|docker-buildx|docker-compose|buildkitd)$')"
[ -z "$n" ] || refuse "a neighbour is running: $(printf '%s' "$n" | tr '\n' ' ' | head -c 300)"
[ -f "$PINS/pins.txt" ] || refuse "no $PINS/pins.txt (run tests/nw25/build.sh first)"
pin() { awk -v k="$1" '$1 == k {print $2}' "$PINS/pins.txt"; }
while read -r kind path sha; do
    case "$kind" in candidate|control|probe|preload) [ "$(sha256sum "$path" 2>/dev/null | cut -c1-64)" = "$sha" ] || refuse "$kind $path changed since build.sh" ;; esac
done < "$PINS/pins.txt"
CAND="$(pin candidate)" CONTROL_BIN="$(pin control)" PROBE="$(pin probe)" PRELOAD="$(pin preload)"
[ -x "$CAND" ] && [ -x "$CONTROL_BIN" ] && [ -x "$PROBE" ] && [ -f "$PRELOAD" ] || refuse "pins missing"
grep -q '^C1 CONTROL-SOURCE PASS' "$PINS/build.txt" && grep -q '^C2 CONTROL-UNFIXED PASS' "$PINS/build.txt" || refuse "the control was not qualified by build.sh (C1, C2)"
[ -e "$OUT" ] && refuse "$OUT exists (a load directory is used once)"
mkdir -p "$OUT/runs" "$OUT/captures" "$OUT/tmp"

{
    echo "NW.25 LOAD: start $(date -Is) load1 $(cut -d' ' -f1 /proc/loadavg)"
    cat "$PINS/pins.txt"
    echo "blocks $BLOCKS: CONTROL $CONTROL_ROUNDS x 16, CAND16 $CAND16_ROUNDS x 16 per block; deadline 30 s, late 40 s"
} | tee "$OUT/load.txt"
arm() { # arm label bin size rounds
    python3 -I "$HERE/stall_harness.py" --arm "$1" --label "$2" --bin "$3" --size "$4" --rounds "$5" \
        --probe-bin "$PROBE" --preload "$PRELOAD" --out "$OUT/runs/$1" --captures "$OUT/captures" \
        --tmpdir "$OUT/tmp" --deadline 30 --late 40 >> "$OUT/runs/$1.out" 2>&1
    echo "$(date +%H:%M:%S) $2 exit $? $(tail -1 "$OUT/runs/$1.out")" | tee -a "$OUT/load.txt"
}
for b in $(seq 1 "$BLOCKS"); do
    arm CONTROL "CONTROL-b$b" "$CONTROL_BIN" 16 "$CONTROL_ROUNDS"
    arm CAND16 "CAND16-b$b" "$CAND" 16 "$CAND16_ROUNDS"
done
echo "NW.25 LOAD: arms done $(date -Is) load1 $(cut -d' ' -f1 /proc/loadavg)" | tee -a "$OUT/load.txt"

# Symbolize every capture against the binary that produced it.
: > "$OUT/signatures.jsonl"
for bin in "$CONTROL_BIN" "$CAND"; do
    caps=()
    while IFS= read -r c; do caps+=("$c"); done < <(grep -l -F "bin=$bin" "$OUT"/captures/stall-*.txt 2>/dev/null | grep -v '\.sym\.txt$')
    [ "${#caps[@]}" -gt 0 ] && python3 -I "$HERE/signatures.py" "$bin" "${caps[@]}" >> "$OUT/signatures.jsonl"
done
left="$(for b in "$CONTROL_BIN" "$CAND" "$PROBE"; do pgrep -a -f -- "^$b( |$)"; done | head -5)"
[ -z "$left" ] || echo "LEFTOVER processes: $left" | tee -a "$OUT/load.txt"
python3 -I "$HERE/judge.py" "$OUT" | tee "$OUT/judge.txt"
code=${PIPESTATUS[0]}
[ -z "$left" ] || code=1
exit "$code"
