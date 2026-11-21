#!/bin/bash
# EC.03c2b's oracle (spec: cea-cdrt-knowledge/spec/items/EC.03c2b.md). Four
# checks, every one run, from this repository:
#   C1 moirai-interp/Cargo.toml has 748a185's [[test]] entries for the six
#      carried files that have no cfg of their own, and no other [[test]]
#      entry; its default features are 748a185's.
#   C2 moirai-interp's tests under its default features build and pass, and
#      none of the carried names runs there.
#   C3 the 16 carried targets under --features network pass, every name with
#      748a185's outcome (names.txt, run on 748a185 itself).
#   C4 moirai-network's ec03c2b_inline passes the nine carried inline names.
# Exit 0 when all four hold, 1 otherwise.
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c2b/check.sh
set -uo pipefail
# names.txt is sorted under C; pin the locale so sort and diff ignore the caller's.
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
OUT=$SCRATCH/ec03c2b-check
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
ENTRIES="si12_fixtures si12_copy_migration si13_concurrent_migrations si14_fixtures si14_rebase si16_rebased_joiner"
TARGETS=$(awk '$1 != "ec03c2b_inline" {print $1}' "$HERE/names.txt" | sort -u)

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

# `<target> :: <name> <ok|FAILED|ignored>`, one line per test a log ran.
names_of() {
    awk '/Running tests\//{split($2,a,"/");t=a[2];sub(/\.rs$/,"",t)}
         /^test .* \.\.\. /{o=$NF; if ($0 ~ /ignored/) o="ignored"; print t" :: "$2" "o}' "$@" | sort
}

rm -rf "$OUT" && mkdir -p "$OUT"
status=0

python3 -I - "$REPO/moirai-interp/Cargo.toml" $ENTRIES <<'EOF' || status=1
import sys, tomllib
manifest = tomllib.load(open(sys.argv[1], "rb"))
want = [{"name": n, "required-features": ["network"]} for n in sys.argv[2:]]
got = manifest.get("test", [])
ok = sorted(got, key=lambda t: t.get("name", "")) == sorted(want, key=lambda t: t["name"])
default = manifest.get("features", {}).get("default")
ok_default = default == ["serde", "test_utils"]
print(f"C1 [[test]] entries: {'ok' if ok else 'FAIL'}: {got}")
print(f"C1 default features: {'ok' if ok_default else 'FAIL'}: {default}")
sys.exit(0 if ok and ok_default else 1)
EOF

cargo_here test -j 2 --offline --no-fail-fast -p moirai-interp >"$OUT/default.log" 2>&1
code=$?
carried=$(names_of "$OUT/default.log" | awk '{print $3}' | grep -Fxf <(awk '{print $3}' "$HERE/names.txt") | wc -l)
echo "C2 default features: exit $code, $(grep -c '^test result: ok' "$OUT/default.log") targets ok, carried names run: $carried"
[ "$code" = 0 ] && [ "$carried" = 0 ] || status=1

args=()
for t in $TARGETS; do args+=(--test "$t"); done
cargo_here test -j 2 --offline --no-fail-fast -p moirai-interp --features network "${args[@]}" \
    >"$OUT/network.log" 2>&1
echo "C3 --features network: exit $?"
cargo_here test -j 2 --offline --no-fail-fast -p moirai-network --test ec03c2b_inline \
    >"$OUT/inline.log" 2>&1
echo "C4 ec03c2b_inline: exit $?"
names_of "$OUT/network.log" "$OUT/inline.log" >"$OUT/names.txt"
if diff "$HERE/names.txt" "$OUT/names.txt" >"$OUT/names.diff"; then
    echo "C3 C4 names: $(wc -l <"$OUT/names.txt") equal to names.txt"
else
    echo "C3 C4 names: FAIL, differences (< expected, > observed):"
    cat "$OUT/names.diff"
    status=1
fi

[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
