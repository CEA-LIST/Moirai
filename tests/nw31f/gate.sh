#!/usr/bin/env bash
# NW.31f gate: moirai-netstack-node catch-up and transfer (S5b) and the
# node-level contract cases of moirai-netstack-testkit, on
# akira/moirai-network-s5b from d1e1c22. Checks, each printed PASS or FAIL: the
# change set stays in the item's paths, and no file of d1e1c22 outside them
# changes (upstream's four crates, moirai-netstack, the transport, vendor and
# every earlier oracle); this item's oracle files unchanged since the commit
# that added the gate; the root manifest unchanged, every d1e1c22 Cargo.lock
# entry kept, the testkit and adapter manifests only gain dependency entries;
# no zenoh in the node, testkit or adapter build; nothing ignored; the node,
# testkit and adapter tests green with every listed test run ok (sockets under
# nodes.lock); moirai-netstack green (regression). Exit 0 only when all pass.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=d1e1c22
LOCK=${NW31F_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
NODES=${NW31F_NODES_LOCK:-$HOME/.cache/cea-crdt-scratch/nodes.lock}
ORACLE=(tests/nw31f moirai-netstack-node/tests/nw31f_node.rs moirai-netstack-testkit/tests/nw31f_contract_node.rs adapters/crdt/tests/nw31f_upstream_node.rs)
NODE_TESTS=(
  a_state_request_for_a_foreign_log_is_refused_naming_both_ids
  mp1_dispatch_hands_a_frame_to_the_log_whose_id_matches
  mp2_a_frame_for_an_unhosted_log_is_filtered_and_counted
  a_frame_for_an_unhosted_log_leaves_its_id_behind_and_hosting_takes_it_back
  the_seen_list_keeps_every_id_and_the_counter_counts_every_frame
  mp16_a_state_request_for_an_unhosted_log_is_answered_unavailable_naming_the_id
  mp17_held_back_frames_for_one_model_do_not_stall_the_other
  mp14_two_nodes_four_models_converge_per_model_with_no_cross_talk
  mp4_the_metrics_expose_the_filter_and_the_refusal_counters
  a_batch_is_applied_in_one_step_and_moves_the_version_by_its_length
  a_batch_computed_before_a_delivery_is_stale_and_applies_nothing
  a_batch_for_a_log_the_node_does_not_host_is_answered_as_such
  a_log_refusal_inside_a_batch_reports_exactly_the_prefix_applied
  a_peer_delivery_cannot_land_inside_a_batch
  a_batch_reaches_a_peer_as_one_frame_delivered_in_one_step
  a_batch_too_large_for_one_frame_goes_out_one_frame_per_operation
  the_batch_route_applies_against_the_etag_it_was_read_at_and_refuses_it_once_moved
  three_toy_nodes_converge_over_the_direct_transport
  z2_three_nodes_converge_partition_heal_converge
  z3_a_late_joiner_adopts_by_state_transfer_and_converges
  s6_late_joiner_catches_up
  t6_a_returning_member_is_refused_a_state_transfer
  a_donor_refuses_in_601c887s_order_and_stamps_its_log
  a_state_response_is_adopted_only_by_a_log_with_no_history
  inbound_frames_with_no_usable_log_id_are_counted_not_dropped_silently
  the_replicas_refusal_reason_is_kept_on_the_log
)
TESTKIT_TESTS=(
  toy_apply_remote_on_an_array_keeps_nothing_when_one_event_is_refused
  toy_passes_exchange_converges toy_passes_refusal_with_cause toy_passes_has_history
  toy_passes_batch_stops_at_first_refusal toy_passes_port_defaults toy_passes_progress_counts
  toy_identity_and_fresh_state toy_exchange_gives_the_sorted_union_and_origin_history
  toy_refuses_a_non_string_op_a_malformed_remote_and_a_foreign_event
  toy_batch_applies_the_prefix_before_the_first_refusal toy_keeps_the_port_defaults
  exchange_fails_when_apply_is_dropped exchange_fails_when_ops_since_sends_nothing
  exchange_fails_when_submit_is_dropped refusal_fails_when_a_refused_op_is_accepted
  refusal_fails_when_a_refused_op_still_applies refusal_fails_when_a_malformed_remote_is_accepted
  refusal_fails_when_a_foreign_event_is_accepted refusal_fails_when_the_cause_is_empty
  history_fails_when_never_held history_fails_when_always_held history_fails_when_counted_by_sender
  batch_fails_when_it_goes_on_after_a_refusal batch_fails_when_events_are_lost
  defaults_fail_when_read_identified_differs defaults_fail_when_a_snapshot_is_offered
  progress_fails_when_always_zero progress_fails_when_delivered_counts_applies
  progress_fails_when_a_reapply_counts progress_fails_when_pending_is_left
  toy_passes_three_nodes_converge toy_passes_catch_up_after_pause toy_passes_late_joiner_by_delta
  toy_passes_foreign_log_filtered toy_passes_returning_member_refused toy_passes_batch_stale_refusal
  offering_toy_passes_late_joiner_by_transfer offering_toy_passes_returning_member_refused
  three_nodes_fails_when_apply_is_dropped catch_up_fails_when_ops_since_sends_nothing
  late_joiner_fails_when_ops_since_sends_nothing
  late_joiner_fails_when_every_donor_holds_history_from_the_joiner
  returning_member_fails_when_history_is_never_held
  returning_member_fails_when_history_is_never_held_and_a_transfer_is_offered
  batch_stale_fails_when_progress_is_always_zero
)
UPSTREAM_TESTS=(
  upstream_passes_three_nodes_converge upstream_passes_catch_up_after_pause
  upstream_passes_late_joiner_with_transfer_not_offered upstream_passes_foreign_log_filtered
  upstream_passes_returning_member_refused_with_transfer_not_offered upstream_passes_batch_stale_refusal
  three_upstream_nodes_converge_add_wins_under_concurrent_ops_and_a_pause
)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
cargo_nodes() { if [ -e "$LOCK" ]; then flock "$LOCK" flock "$NODES" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
names_ok() { local out=$1; shift; local missing=""; for t in "$@"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done; echo "$missing"; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "FAIL: $BASE is not in this repository"; exit 2; }

# 1. Change set since d1e1c22: files of d1e1c22 change only in the node's
# sources and manifest, the testkit's sources and manifest, the adapter's
# manifest and Cargo.lock; new files only there, in the node's sources, and as
# this item's oracle files.
changed=$( { git diff --name-only --diff-filter=MDRTC "$BASE"; git status --porcelain --untracked-files=no | cut -c4-; } | sort -u \
  | grep -Ev '^(moirai-netstack-node/src/|moirai-netstack-node/Cargo\.toml$|moirai-netstack-testkit/src/|moirai-netstack-testkit/Cargo\.toml$|adapters/crdt/Cargo\.toml$|Cargo\.lock$)' )
added=$( { git diff --name-only --diff-filter=A "$BASE"; git status --porcelain --untracked-files=all | grep '^??' | cut -c4-; } | sort -u \
  | grep -Ev '^(moirai-netstack-node/src/|moirai-netstack-testkit/src/|tests/nw31f/|moirai-netstack-node/tests/nw31f_node\.rs$|moirai-netstack-testkit/tests/nw31f_contract_node\.rs$|adapters/crdt/tests/nw31f_upstream_node\.rs$)' )
report change_set_in_scope "$([ -z "$changed$added" ]; echo $?)" "$(echo $changed $added)"

# 2. This item's oracle files: unchanged since the commit that added the gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31f/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31f/gate.sh is not committed"
else
  drift=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$drift" ]; echo $?)" "$(echo $drift)"
fi

# 3. Manifests and lock against d1e1c22.
python3 -I - "$BASE" <<'PY'
import subprocess, sys, tomllib
base = sys.argv[1]
def old(path): return tomllib.loads(subprocess.run(["git", "show", f"{base}:{path}"], check=True, capture_output=True, text=True).stdout)
def new(path):
    with open(path, "rb") as f: return tomllib.load(f)
why = []
if old("Cargo.toml") != new("Cargo.toml"): why.append("the root Cargo.toml changed")
for path in ("moirai-netstack-testkit/Cargo.toml", "adapters/crdt/Cargo.toml"):
    o, n = old(path), new(path)
    for table in ("dependencies", "dev-dependencies"):
        ot, nt = o.pop(table, {}), n.pop(table, {})
        if any(nt.get(k) != v for k, v in ot.items()): why.append(f"{path}: an entry of [{table}] changed or went")
    if o != n: why.append(f"{path}: a table besides the dependency tables changed")
ok = not why
print(("PASS" if ok else "FAIL") + " manifests_only_gain_dependencies" + ("" if ok else ": " + "; ".join(why)))
ol, nl = old("Cargo.lock"), new("Cargo.lock")
now = {(p["name"], p["version"]): p for p in nl["package"]}
lost = []
for p in ol["package"]:
    q = now.get((p["name"], p["version"]))
    if q is None: lost.append(f"{p['name']} {p['version']} gone"); continue
    rest = lambda e: {k: v for k, v in e.items() if k != "dependencies"}
    if rest(p) != rest(q) or not set(p.get("dependencies", [])) <= set(q.get("dependencies", [])): lost.append(f"{p['name']} {p['version']} changed")
ok2 = not lost and ol.get("version") == nl.get("version")
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_d1e1c22_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if ok and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 4. No zenoh in the build of the node, the testkit or the adapter (normal and dev).
tree=$(cargo_ tree --locked -e normal,dev -p moirai-netstack-node -p moirai-netstack-testkit -p moirai-netstack-crdt --prefix none 2>&1); code=$?
zen=$(echo "$tree" | grep -E '^zenoh' | sort -u | head -n 3)
report no_zenoh_in_the_build "$([ $code = 0 ] && [ -z "$zen" ]; echo $?)" "exit $code $zen"

# 5. Nothing ignored in the three packages.
ignored=$(grep -rln '#\[ignore' moirai-netstack-node moirai-netstack-testkit adapters/crdt 2>/dev/null)
report nothing_ignored "$([ -z "$ignored" ]; echo $?)" "$ignored"

# 6 to 8. The three packages' tests, sockets on 127.0.0.1 under nodes.lock.
run() {
  local label=$1 pkg=$2; shift 2
  local out code missing
  out=$(cargo_nodes test -p "$pkg" --locked -j 2 --no-fail-fast -- --test-threads=4 2>&1); code=$?
  missing=$(names_ok "$out" "$@")
  echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 40
  report "$label" "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"
}
run node_tests moirai-netstack-node "${NODE_TESTS[@]}"
run testkit_tests moirai-netstack-testkit "${TESTKIT_TESTS[@]}"
run upstream_tests moirai-netstack-crdt "${UPSTREAM_TESTS[@]}"

# 9. Regression: moirai-netstack's own tests (6 passed, 0 failed at d1e1c22).
out=$(cargo_ test -p moirai-netstack --locked -j 2 --no-fail-fast 2>&1); code=$?
echo "$out" | grep -E '^error|^test result|FAILED' | head -n 20
report moirai_netstack_tests "$code" "exit $code"

echo "NW31F_GATE fail=$fail"
exit $fail
