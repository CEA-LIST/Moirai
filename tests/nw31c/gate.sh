#!/usr/bin/env bash
# NW.31c gate: moirai-netstack-testkit (toy replica and port-level contract suite)
# beside moirai-netstack at b57ffb2. Checks, each printed PASS or FAIL: upstream's
# four crates, moirai-netstack and the NW.31a oracle unchanged since b57ffb2; the
# root manifest only gains the member; every Cargo.lock entry of b57ffb2 kept;
# no moirai* package but moirai-netstack in the testkit's dependency closure; the
# testkit's tests green with every NW.31c oracle test run; moirai-netstack's tests
# green (regression). Exit 0 only when all pass. Cargo runs under the shared lock.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=b57ffb2
CRATE=moirai-netstack-testkit
KEEP=(moirai-crdt moirai-protocol moirai-macros moirai-fuzz moirai-netstack tests/nw31a)
LOCK=${NW31C_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
ORACLE_TESTS=(
  toy_passes_exchange_converges
  toy_passes_refusal_with_cause
  toy_passes_has_history
  toy_passes_batch_stops_at_first_refusal
  toy_passes_port_defaults
  toy_passes_progress_counts
  toy_identity_and_fresh_state
  toy_exchange_gives_the_sorted_union_and_origin_history
  toy_refuses_a_non_string_op_a_malformed_remote_and_a_foreign_event
  toy_batch_applies_the_prefix_before_the_first_refusal
  toy_keeps_the_port_defaults
  exchange_fails_when_apply_is_dropped
  exchange_fails_when_ops_since_sends_nothing
  exchange_fails_when_submit_is_dropped
  refusal_fails_when_a_refused_op_is_accepted
  refusal_fails_when_a_refused_op_still_applies
  refusal_fails_when_a_malformed_remote_is_accepted
  refusal_fails_when_a_foreign_event_is_accepted
  refusal_fails_when_the_cause_is_empty
  history_fails_when_never_held
  history_fails_when_always_held
  history_fails_when_counted_by_sender
  batch_fails_when_it_goes_on_after_a_refusal
  batch_fails_when_events_are_lost
  defaults_fail_when_read_identified_differs
  defaults_fail_when_a_snapshot_is_offered
  progress_fails_when_always_zero
  progress_fails_when_delivered_counts_applies
  progress_fails_when_a_reapply_counts
  progress_fails_when_pending_is_left
)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
cd "$ROOT" || exit 2

# 1. Upstream's crates, the core crate and the NW.31a oracle: no change since b57ffb2, no new file.
git cat-file -e "$BASE^{commit}" || { echo "FAIL: $BASE is not in this repository"; exit 2; }
changed=$(git diff --name-only "$BASE" -- "${KEEP[@]}"; git status --porcelain --untracked-files=all -- "${KEEP[@]}")
report kept_paths_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"

# 2 and 3. Root manifest and lock against b57ffb2 (NW.31a's rule, base moved).
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
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_b57ffb2_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if ok and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 4. Dependency closure of the testkit (normal, dev and build): moirai-netstack is the only moirai* package.
meta=$(cargo_ metadata --format-version 1 --locked 2>/dev/null)
echo "$meta" | python3 -I -c '
import json, sys
crate = sys.argv[1]
try:
    m = json.loads(sys.stdin.read())
except ValueError:
    print("FAIL only_moirai_netstack_dependency: cargo metadata --locked failed (a manifest error, or Cargo.lock out of date)"); sys.exit(1)
names = {p["id"]: p["name"] for p in m["packages"]}
nodes = {n["id"]: n for n in m["resolve"]["nodes"]}
roots = [i for i, n in names.items() if n == crate and i in m["workspace_members"]]
if not roots:
    print(f"FAIL only_moirai_netstack_dependency: {crate} is not a workspace member"); sys.exit(1)
seen, todo = set(), list(roots)
while todo:
    i = todo.pop()
    if i in seen: continue
    seen.add(i)
    todo += [d["pkg"] for d in nodes[i]["deps"]]
got = {names[i] for i in seen}
bad = sorted(n for n in got if n.startswith("moirai") and n not in (crate, "moirai-netstack"))
miss = "" if "moirai-netstack" in got else " (moirai-netstack missing)"
ok = not bad and not miss
print("PASS only_moirai_netstack_dependency" if ok else "FAIL only_moirai_netstack_dependency: " + ", ".join(bad) + miss)
sys.exit(0 if ok else 1)
' "$CRATE" || fail=1

# 5. The testkit's tests, every NW.31c oracle test among the passes.
out=$(cargo_ test -p "$CRATE" --locked -j 2 2>&1); code=$?
missing=""
for t in "${ORACLE_TESTS[@]}"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done
echo "$out" | grep -E '^error|^test result|FAILED' | head -n 40
report testkit_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"

# 6. Regression: moirai-netstack's own tests (6 passed, 0 failed at b57ffb2; see the spec).
out=$(cargo_ test -p moirai-netstack --locked -j 2 --no-fail-fast 2>&1); code=$?
echo "$out" | grep -E '^error|^test result|FAILED' | head -n 20
report moirai_netstack_tests "$code" "exit $code"

echo "NW31C_GATE fail=$fail"
exit $fail
