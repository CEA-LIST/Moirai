#!/bin/bash
# EC.M2's in-process oracle (spec: cea-cdrt-knowledge/spec/items/EC.M2.md), O2.
# From this worktree, every step run, each once:
#   S1..S7  the in-process test commands; every test's name and outcome must equal
#           names.txt line for line (measured on the guide, 71384ea), every command exit 0;
#   B1..B3  the workspace, the network feature and the network crate without
#           default features build;
#   L1 L2   cargo fmt --all --check exit 0; cargo clippy --workspace --all-targets exit 0.
# Exit 0 when all hold, 1 otherwise.
#
#   ECM2_SCRATCH=<dir> moirai-interp/tests/ecm2/check.sh
set -uo pipefail
export LC_ALL=C
# Builds only: no debug info and no incremental cache, so the scratch target stays small.
export CARGO_INCREMENTAL=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_RELEASE_DEBUG=0

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${ECM2_SCRATCH:?ECM2_SCRATCH names the work folder}
OUT=$SCRATCH/ecm2-check
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock

P() { (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
    flock -w 3000 "$LOCK" nice -n 10 cargo "$@"); }

# `<suite> <target> :: <name> <ok|FAILED|ignored>`; <target> is the test binary's name without its hash.
names_of() { # suite log
    awk -v s="$1" '/^ *Running .*\(.*\/deps\/[^)]*\)$/{b=$NF; sub(/^\(.*\/deps\//,"",b); sub(/\)$/,"",b); sub(/-[0-9a-f]+$/,"",b); t=b}
        /^test .* \.\.\. /{o=$NF; if ($0 ~ /\.\.\. ignored/) o="ignored"; print s" "t" :: "$2" "o}' "$2"
}

C2B="si01_store_identity si01_store_identity_races si01_store_identity_races_2 si01_store_identity_races_3 si12_copy_migration si12_fixtures si13_concurrent_migrations si14_fixtures si14_rebase si16_rebased_joiner st01_optional_conflict_race st02_emptied_optional_object st04_emptied_optional_attribute st05_recursive_keyed_class st05_recursive_keyed_clear_and_join st05_removed_key"
NET="layering nw15_choice_zenoh nw15_contract_direct nw15_contract_zenoh nw15_default_tcp nw15_nodes_tcp nw15_nodes_zenoh nw15_zenoh_config nw15_zenoh_transport nw16a_paused_broadcast_l2 nw16a_presence_l2 nw16a_wait_before_close_l1 nw16d_publisher_qos_l2 nw16d_publisher_source_l0 nw16e_region_config_l1 nw19_default_l1 nw19_default_zenoh nw27_decode ec03c_refusal ec03c2b_inline"
a6=(); for t in $C2B; do a6+=(--test "$t"); done
a7=(--lib); for t in $NET; do a7+=(--test "$t"); done
T="test -j 2 --offline --no-fail-fast"

rm -rf "$OUT" && mkdir -p "$OUT"
status=0
step() { # id command...
    local id=$1; shift
    "$@" >"$OUT/$id.log" 2>&1
    local code=$?
    echo "$id exit $code"
    [ "$code" = 0 ] || status=1
}
step S1 P $T -p moirai-crdt -p moirai-protocol -p moirai-macros -p moirai-fuzz
step S2 P $T -p moirai-protocol --features test_utils,serde
step S3 P $T -p moirai-semantics -p moirai-interp
step S4 P $T -p moirai-interp --features fuzz
step S5 P $T -p moirai-interp --features sink
step S6 P $T -p moirai-interp --features network "${a6[@]}"
step S7 P $T -p moirai-network "${a7[@]}"
step B1 P build -j 2 --offline --workspace --all-targets
step B2 P build -j 2 --offline -p moirai-interp --features network --all-targets
step B3 P build -j 2 --offline -p moirai-network --no-default-features --lib
step L1 bash -c "cd '$REPO' && cargo fmt --all --check"
step L2 P clippy -j 2 --offline --workspace --all-targets

for s in S1 S2 S3 S4 S5 S6 S7; do names_of "$s" "$OUT/$s.log"; done | sort >"$OUT/names.txt"
if diff "$HERE/names.txt" "$OUT/names.txt" >"$OUT/names.diff"; then
    echo "names: $(wc -l <"$OUT/names.txt") equal to names.txt"
else
    echo "names: FAIL, $(grep -c '^<' "$OUT/names.diff") expected lines missing or changed, $(grep -c '^>' "$OUT/names.diff") observed lines not expected (< expected, > observed; first 40):"
    grep '^[<>]' "$OUT/names.diff" | head -40
    status=1
fi
echo "clippy warnings: $(grep -c '^warning' "$OUT/L2.log") (reported, not judged)"
[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
