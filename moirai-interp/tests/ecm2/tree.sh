#!/bin/bash
# EC.M2's tree oracle (spec: cea-cdrt-knowledge/spec/items/EC.M2.md), O3: the
# merge is resolved as the guide resolved it. Run from this worktree, during the
# uncommitted merge or on the merge commit; reads git only, builds nothing but
# `cargo metadata`.
#
#   ECM2_SCRATCH=<dir> moirai-interp/tests/ecm2/tree.sh
#   T1 no unmerged path;
#   T2 in the six crates below, every path that is not an oracle path is
#      byte for byte the guide's (71384ea), and no other non-oracle path exists;
#   T3 every path outside the six crates is 7ba78b6's, except .gitignore,
#      docs/benchmarking.md and Cargo.lock;
#   T4 .gitignore and docs/benchmarking.md are git's clean three-way merge of
#      7870856 (the merge base), 7ba78b6 and 5be63b1;
#   T5 no untracked file;
#   T6 Cargo.lock agrees with the manifests offline (cargo metadata --locked).
# Exit 0 when all hold, 1 otherwise.
set -uo pipefail
export LC_ALL=C
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
cd "$REPO" || exit 1
SIX="moirai-crdt moirai-protocol moirai-macros moirai-fuzz moirai-interp moirai-network"
ORACLE='(^|/)(tests|benches|testdata|fixtures)/|_test\.rs$|(^|/)tests\.rs$'
status=0
report() { # id lines-file
    if [ -s "$2" ]; then echo "$1 FAIL ($(wc -l <"$2") paths, first 20):"; head -20 "$2"; status=1; else echo "$1 ok"; fi
}
tmp=${ECM2_SCRATCH:?ECM2_SCRATCH names the work folder}/ecm2-tree
rm -rf "$tmp" && mkdir -p "$tmp"

git ls-files -u | awk '{print $4}' | sort -u >"$tmp/t1"
report T1 "$tmp/t1"

excl=(); for c in $SIX; do excl+=(":(exclude)$c"); done
git diff --no-renames --name-status 71384ea -- $SIX | awk -F'\t' '{print $1" "$2}' \
    | grep -Ev " .*($ORACLE)" >"$tmp/t2"
report T2 "$tmp/t2"

git diff --no-renames --name-only 7ba78b6 -- . "${excl[@]}" ':(exclude).gitignore' \
    ':(exclude)docs/benchmarking.md' ':(exclude)Cargo.lock' >"$tmp/t3"
report T3 "$tmp/t3"

: >"$tmp/t4"
for f in .gitignore docs/benchmarking.md; do
    git show "7ba78b6:$f" >"$tmp/ours"; git show "7870856:$f" >"$tmp/base"; git show "5be63b1:$f" >"$tmp/theirs"
    if ! git merge-file -p "$tmp/ours" "$tmp/base" "$tmp/theirs" >"$tmp/want"; then
        echo "$f: the three-way merge is not clean" >>"$tmp/t4"
    elif ! cmp -s "$tmp/want" "$f"; then
        echo "$f: differs from the clean three-way merge" >>"$tmp/t4"
    fi
done
report T4 "$tmp/t4"

git status --porcelain --untracked-files=all | grep '^??' >"$tmp/t5"
report T5 "$tmp/t5"

if cargo metadata --format-version 1 --offline --locked >/dev/null 2>"$tmp/t6.err"; then
    echo "T6 ok"
else
    echo "T6 FAIL:"; tail -5 "$tmp/t6.err"; status=1
fi
[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
