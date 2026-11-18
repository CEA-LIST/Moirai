#!/bin/bash
# EC.03's differential oracle (spec: cea-cdrt-knowledge/spec/items/EC.03.md),
# with EC.03b's joiners by state transfer (spec/items/EC.03b.md).
#
# Records the schedules on the reference interpreter (748a185, a git archive
# of repos/moirai-model-plane under scratch), replays them on the reference
# and on the candidate (this worktree), and compares the two traces line by
# line. Exit 0: every read equal. Exit 1: a read differs (the first
# difference is printed, with both replicas' reads in full). Exit 2: the
# instrument itself failed a check (R1 to R3), so nothing was compared.
#
#   moirai-interp/tests/ec03/run.sh
#
# Cargo runs under the shared lock, niced, two jobs, offline; the reference
# builds into $SCRATCH/target-a, the candidate into $SCRATCH/target.
#
# EC.03b: after the plain comparison, the same schedules are replayed with
# joiners (ec03b_joiners.rs) on the reference twice (R3j: identical) and on
# the candidate, and compared line by line; R4: the reference trace holds at
# least one join under a rebase still waiting for someone. The candidate makes
# the joins the reference made (EC03_JOINS). Exit 1 when either comparison
# differs.
set -uo pipefail
# The schedules are hashed in sort order; pin the locale so the order is the bytes', not the caller's.
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/EC.03/work}
REF_REPO=${EC03_REF_REPO:-$REPO/../moirai-model-plane}
REF=$SCRATCH/a
OUT=$SCRATCH/ec03-run
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock

cargo_in() { # dir target-dir args...
    local dir=$1 target=$2
    shift 2
    (cd "$dir" && CARGO_TARGET_DIR=$target CARGO_BUILD_JOBS=2 \
        flock -w 540 "$LOCK" nice -n 10 cargo "$@")
}

# The reference tree: 748a185 as archived, plus this oracle's files, with the
# reference's replica layer in place of the candidate's.
if [ ! -f "$REF/.ec03-748a185" ]; then
    rm -rf "$REF" && mkdir -p "$REF"
    git -C "$REF_REPO" archive 748a185 | tar -x -C "$REF" || exit 2
    touch "$REF/.ec03-748a185"
fi
rm -rf "$REF/moirai-interp/tests/ec03"
cp -r "$HERE" "$REF/moirai-interp/tests/ec03"
cp "$HERE/side_748a185.rs" "$REF/moirai-interp/tests/ec03/side.rs"
cp "$REPO/moirai-interp/tests/ec03_differential.rs" "$REPO/moirai-interp/tests/ec03b_joiners.rs" \
    "$REF/moirai-interp/tests/"
printf '%s\n' '#[path = "ec03/reads.rs"]' 'mod reads;' '#[path = "ec03/side.rs"]' 'mod side;' \
    '#[path = "ec03/record.rs"]' 'mod record;' > "$REF/moirai-interp/tests/ec03_record.rs"

rm -rf "$OUT" && mkdir -p "$OUT/schedules"
export EC03_SCHEDULES=$OUT/schedules

echo "== record on 748a185"
cargo_in "$REF" "$SCRATCH/target-a" test -j 2 --offline --release -p moirai-interp --features fuzz \
    --test ec03_record -- --exact record::ec03_record || { echo "R0: the recorder failed"; exit 2; }
ls "$OUT"/schedules/*.jsonl >/dev/null 2>&1 || { echo "R0: no schedule was written"; exit 2; }

# R1: the recorder is deterministic: the schedules are the registered ones.
sha=$(cd "$OUT/schedules" && cat $(ls *.jsonl | sort) | sha256sum | cut -d' ' -f1)
want=$(cat "$HERE/schedules.sha256" 2>/dev/null)
echo "schedules: $(ls "$OUT/schedules" | wc -l) files, sha256 $sha"
[ "$sha" = "$want" ] || { echo "R1 FAIL: registered $want"; exit 2; }

echo "== replay on 748a185"
EC03_OUT=$OUT/trace-ref.txt cargo_in "$REF" "$SCRATCH/target-a" test -j 2 --offline --release \
    -p moirai-interp --features fuzz --test ec03_differential -- --ignored --exact ec03_replay \
    || { echo "R2: the reference replay failed"; exit 2; }

# R2: the reference replays its own schedules faithfully: no refused or
# missing step, and every replica ends on the digest the recorder saw.
if grep -q ' refused \| missing ' "$OUT/trace-ref.txt"; then
    echo "R2 FAIL: the reference refused its own schedule"; grep -m3 ' refused \| missing ' "$OUT/trace-ref.txt"; exit 2
fi
python3 -I - "$OUT" <<'EOF' || exit 2
import json, pathlib, sys
out = pathlib.Path(sys.argv[1])
ends = {}
for line in (out / "trace-ref.txt").read_text().splitlines():
    name, step, who, digest = line.split()[:4]
    if step == "end":
        ends[(name, who)] = digest
bad = 0
for path in sorted((out / "schedules").glob("*.jsonl")):
    lines = path.read_text().splitlines()
    name = json.loads(lines[0])["schedule"]
    for who, digest in json.loads(lines[-1])["end"].items():
        if ends.get((name, who)) != digest:
            bad += 1
            print(f"R2 FAIL: {name} {who}: recorded {digest}, replayed {ends.get((name, who))}")
sys.exit(1 if bad else 0)
EOF

# R3: the replay is deterministic on the reference.
EC03_OUT=$OUT/trace-ref2.txt cargo_in "$REF" "$SCRATCH/target-a" test -j 2 --offline --release \
    -p moirai-interp --features fuzz --test ec03_differential -- --ignored --exact ec03_replay \
    >/dev/null 2>&1 || { echo "R3: the second reference replay failed"; exit 2; }
cmp -s "$OUT/trace-ref.txt" "$OUT/trace-ref2.txt" || { echo "R3 FAIL: two reference replays differ"; exit 2; }
echo "reference trace: $(wc -l < "$OUT/trace-ref.txt") lines, R1 R2 R3 ok"

echo "== replay on the candidate ($(git -C "$REPO" rev-parse --short HEAD))"
plain=0
if ! EC03_OUT=$OUT/trace-cand.txt cargo_in "$REPO" "$SCRATCH/target" test -j 2 --offline --release \
    -p moirai-interp --test ec03_differential -- --ignored --exact ec03_replay; then
    echo "FAIL: the candidate replay did not run"; plain=1
elif cmp -s "$OUT/trace-ref.txt" "$OUT/trace-cand.txt"; then
    echo "PASS: $(wc -l < "$OUT/trace-ref.txt") reads equal across $(ls "$OUT/schedules" | wc -l) schedules"
else
    plain=1
    first=$(diff "$OUT/trace-ref.txt" "$OUT/trace-cand.txt" | grep -m1 '^[<>]')
    echo "FAIL: first difference: $first"
    diff "$OUT/trace-ref.txt" "$OUT/trace-cand.txt" | grep -c '^>' | sed 's/^/candidate lines that differ: /'
    point=$(echo "$first" | awk '{print $2 ":" $3}')
    if [ "${point#*:}" != "end" ]; then
        for side in ref:"$REF":"$SCRATCH/target-a":fuzz cand:"$REPO":"$SCRATCH/target":default; do
            IFS=: read -r label dir target feat <<< "$side"
            flags=(); [ "$feat" = fuzz ] && flags=(--features fuzz)
            echo "== $label reads at $point"
            EC03_DUMP=$point EC03_OUT=$OUT/dump-$label.txt cargo_in "$dir" "$target" test -j 2 --release \
                --offline -p moirai-interp "${flags[@]}" --test ec03_differential -- --ignored \
                --exact ec03_replay --nocapture 2>&1 | sed -n '/^EC03-DUMP/,/^}/p'
        done
    fi
fi

echo "== EC.03b: joiners by state transfer, on 748a185"
for n in 1 2; do
    EC03_OUT=$OUT/joiners-ref$n.txt cargo_in "$REF" "$SCRATCH/target-a" test -j 2 --offline --release \
        -p moirai-interp --features fuzz --test ec03b_joiners -- --ignored --exact ec03b_replay_joiners \
        >"$OUT/joiners-ref$n.log" 2>&1 || { echo "R2j: the reference joiner replay failed"; tail -20 "$OUT/joiners-ref$n.log"; exit 2; }
done
cmp -s "$OUT/joiners-ref1.txt" "$OUT/joiners-ref2.txt" || { echo "R3j FAIL: two reference joiner replays differ"; exit 2; }
held=$(grep -c ' join .* held$' "$OUT/joiners-ref1.txt")
[ "$held" -gt 0 ] || { echo "R4 FAIL: no join under a held rebase"; exit 2; }
echo "reference joiner trace: $(wc -l < "$OUT/joiners-ref1.txt") lines, $(grep -c ' join ' "$OUT/joiners-ref1.txt") joins ($(grep -c ' unserializable ' "$OUT/joiners-ref1.txt") not made: the reference cannot write the donor as JSON), $held under a held rebase, R3j R4 ok"

echo "== EC.03b: joiners on the candidate"
joined=0
if ! EC03_JOINS=$OUT/joiners-ref1.txt EC03_OUT=$OUT/joiners-cand.txt cargo_in "$REPO" "$SCRATCH/target" test -j 2 --offline --release \
    -p moirai-interp --test ec03b_joiners -- --ignored --exact ec03b_replay_joiners; then
    echo "FAIL: the candidate joiner replay did not run"; joined=1
elif cmp -s "$OUT/joiners-ref1.txt" "$OUT/joiners-cand.txt"; then
    echo "PASS: $(wc -l < "$OUT/joiners-ref1.txt") joiner reads equal"
else
    joined=1
    echo "FAIL: first joiner difference: $(diff "$OUT/joiners-ref1.txt" "$OUT/joiners-cand.txt" | grep -m1 '^[<>]')"
    diff "$OUT/joiners-ref1.txt" "$OUT/joiners-cand.txt" | grep -c '^>' | sed 's/^/candidate joiner lines that differ: /'
fi
[ $plain = 0 ] && [ $joined = 0 ] && exit 0
exit 1
