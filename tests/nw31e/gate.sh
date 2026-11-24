#!/usr/bin/env bash
# NW.31e gate: moirai-netstack-node (S5a, node core: hosting and intake) on
# akira/moirai-network-s5a from 9eed5b8. Checks, each printed PASS or FAIL: the
# change set stays in the item's paths and every earlier crate and oracle is
# unchanged since 9eed5b8; this item's oracle files are unchanged since the commit
# that added them; the root manifest only gains the member; every Cargo.lock entry
# of 9eed5b8 kept; no moirai* package outside the netstack crates in the node's
# closure, the transport taken without default features and zenoh forwarded; the
# node's tests green with each of the 18 oracle tests run, none ignored (under
# nodes.lock); the testkit green with NW.31c's 30 tests and the toy atomicity test;
# moirai-netstack green (regression). Exit 0 only when all pass.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=9eed5b8
CRATE=moirai-netstack-node
LOCK=${NW31E_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
NODES=${NW31E_NODES_LOCK:-$HOME/.cache/cea-crdt-scratch/nodes.lock}
ORACLE=(tests/nw31e moirai-netstack-node/tests/nw31e_node.rs moirai-netstack-testkit/tests/nw31e_toy_batch_atomic.rs)
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
)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
cargo_nodes() { if [ -e "$LOCK" ]; then flock "$LOCK" flock "$NODES" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
names_ok() { local out=$1; shift; local missing=""; for t in "$@"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done; echo "$missing"; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "FAIL: $BASE is not in this repository"; exit 2; }

# 1. The change set since 9eed5b8 stays in the item's paths.
stray=$( { git diff --name-only "$BASE"; git status --porcelain --untracked-files=all | cut -c4-; } | sort -u \
  | grep -Ev '^(moirai-netstack-node/|moirai-netstack-testkit/src/lib\.rs$|Cargo\.toml$|Cargo\.lock$|tests/nw31e/|moirai-netstack-testkit/tests/nw31e_toy_batch_atomic\.rs$)' )
report change_set_in_scope "$([ -z "$stray" ]; echo $?)" "$(echo $stray)"

# 2. This item's oracle files: unchanged since the commit that added the gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31e/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31e/gate.sh is not committed"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 3 and 4. Root manifest and lock against 9eed5b8.
python3 -I - "$BASE" "$CRATE" <<'PY'
import subprocess, sys, tomllib
base, crate = sys.argv[1], sys.argv[2]
def old(path): return tomllib.loads(subprocess.run(["git", "show", f"{base}:{path}"], check=True, capture_output=True, text=True).stdout)
def new(path):
    with open(path, "rb") as f: return tomllib.load(f)
om, nm = old("Cargo.toml"), new("Cargo.toml")
members = nm.get("workspace", {}).get("members", [])
ok_members = sorted(members) == sorted(om["workspace"]["members"] + [crate])
om["workspace"]["members"] = members
ok_rest = om == nm
ok = ok_members and ok_rest
why = ("" if ok_members else f" members {members}") + ("" if ok_rest else " a key besides the members list changed")
print(("PASS" if ok else "FAIL") + " root_manifest_only_gains_the_member" + ("" if ok else ":" + why))
ol, nl = old("Cargo.lock"), new("Cargo.lock")
now = {(p["name"], p["version"]): p for p in nl["package"]}
lost = []
for p in ol["package"]:
    q = now.get((p["name"], p["version"]))
    if q is None: lost.append(f"{p['name']} {p['version']} gone"); continue
    rest = lambda e: {k: v for k, v in e.items() if k != "dependencies"}
    if p["name"].startswith("moirai") and p != q: lost.append(f"{p['name']} changed")
    elif rest(p) != rest(q) or not set(p.get("dependencies", [])) <= set(q.get("dependencies", [])): lost.append(f"{p['name']} {p['version']} changed")
ok2 = not lost and ol.get("version") == nl.get("version")
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_9eed5b8_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if ok and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 5. The node's closure (normal, dev, build): only netstack crates; transport without default features; zenoh forwarded, off by default.
meta=$(cargo_ metadata --format-version 1 --locked 2>/dev/null)
echo "$meta" | python3 -I -c '
import json, sys
crate = sys.argv[1]
allowed = {crate, "moirai-netstack", "moirai-netstack-transport", "moirai-netstack-testkit"}
try:
    m = json.loads(sys.stdin.read())
except ValueError:
    print("FAIL node_dependencies: cargo metadata --locked failed (a manifest error, or Cargo.lock out of date)"); sys.exit(1)
pkgs = {p["id"]: p for p in m["packages"]}
nodes = {n["id"]: n for n in m["resolve"]["nodes"]}
roots = [i for i, p in pkgs.items() if p["name"] == crate and i in m["workspace_members"]]
if not roots:
    print(f"FAIL node_dependencies: {crate} is not a workspace member"); sys.exit(1)
seen, todo = set(), list(roots)
while todo:
    i = todo.pop()
    if i in seen: continue
    seen.add(i)
    todo += [d["pkg"] for d in nodes[i]["deps"]]
bad = sorted({pkgs[i]["name"] for i in seen if pkgs[i]["name"].startswith("moirai") and pkgs[i]["name"] not in allowed})
node = pkgs[roots[0]]
why = []
if bad: why.append("moirai packages outside the netstack crates: " + ", ".join(bad))
tr = [d for d in node["dependencies"] if d["name"] == "moirai-netstack-transport" and d["kind"] is None]
if not tr: why.append("no normal dependency on moirai-netstack-transport")
elif any(d["uses_default_features"] for d in tr): why.append("moirai-netstack-transport taken with default features")
feats = node.get("features", {})
if "moirai-netstack-transport/zenoh" not in feats.get("zenoh", []): why.append("no zenoh feature forwarding moirai-netstack-transport/zenoh")
if "zenoh" in feats.get("default", []): why.append("zenoh on by default")
print("PASS node_dependencies" if not why else "FAIL node_dependencies: " + "; ".join(why))
sys.exit(0 if not why else 1)
' "$CRATE" || fail=1

# 6. The node's tests, every oracle test among the passes, none ignored; sockets on 127.0.0.1 under nodes.lock.
ignored=$(grep -rln '#\[ignore' "$CRATE" 2>/dev/null)
report node_nothing_ignored "$([ -z "$ignored" ]; echo $?)" "$ignored"
out=$(cargo_nodes test -p "$CRATE" --locked -j 2 2>&1); code=$?
missing=$(names_ok "$out" "${NODE_TESTS[@]}")
echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 40
report node_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"

# 7. The testkit: NW.31c's 30 oracle tests and the toy atomicity test.
out=$(cargo_ test -p moirai-netstack-testkit --locked -j 2 --no-fail-fast 2>&1); code=$?
missing=$(names_ok "$out" "${TESTKIT_TESTS[@]}")
echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 20
report testkit_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"

# 8. Regression: moirai-netstack's own tests (6 passed, 0 failed at 9eed5b8; see the spec).
out=$(cargo_ test -p moirai-netstack --locked -j 2 --no-fail-fast 2>&1); code=$?
echo "$out" | grep -E '^error|^test result|FAILED' | head -n 20
report moirai_netstack_tests "$code" "exit $code"

echo "NW31E_GATE fail=$fail"
exit $fail
