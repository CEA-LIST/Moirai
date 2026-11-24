#!/usr/bin/env bash
# NW.31d gate: the upstream Moirai adapter (adapters/crdt, package
# moirai-netstack-crdt) on akira/moirai-network at 9eed5b8. Checks, each
# printed PASS or FAIL: every path of 9eed5b8 but the root manifest and lock is
# unchanged (upstream's four crates, moirai-netstack, the testkit with its
# contract, the transport crate, earlier oracles); the root manifest only gains
# the member; every Cargo.lock entry of 9eed5b8 kept; the adapter's dependency
# closure holds moirai-netstack, moirai-protocol and moirai-crdt from this
# workspace and no moirai-netstack-transport; the adapter's tests green with
# every NW.31d oracle test run; the testkit's and moirai-netstack's tests green
# (regression). Exit 0 only when all pass. Cargo runs under the shared lock.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=9eed5b8
CRATE=moirai-netstack-crdt
CRATE_DIR=adapters/crdt
LOCK=${NW31D_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
ORACLE_TESTS=(
  upstream_passes_exchange_converges
  upstream_passes_refusal_with_cause
  upstream_passes_has_history
  upstream_passes_batch_stops_at_first_refusal
  upstream_passes_port_defaults
  upstream_passes_progress_counts
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
  three_replicas_converge_under_concurrent_ops
  forged_origin_single_event_is_refused
  forged_origin_relayed_batch_is_refused
  forged_origin_ops_since_answer_is_refused
)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
cd "$ROOT" || exit 2

# 1. Nothing of 9eed5b8 changes but the root manifest and lock; new files only in the adapter and tests/nw31d.
git cat-file -e "$BASE^{commit}" || { echo "FAIL: $BASE is not in this repository"; exit 2; }
changed=$( (git diff --name-only "$BASE"; git ls-files --others --exclude-standard) | sort -u \
  | grep -v -x -e Cargo.toml -e Cargo.lock | grep -v -e "^$CRATE_DIR/" -e '^tests/nw31d/')
report base_paths_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"

# 2 and 3. Root manifest gains only the member; the lock keeps every 9eed5b8 entry (NW.31c's rule, base moved).
python3 -I - "$BASE" "$CRATE_DIR" <<'PY'
import subprocess, sys, tomllib
base, member = sys.argv[1], sys.argv[2]
def old(path): return tomllib.loads(subprocess.run(["git", "show", f"{base}:{path}"], check=True, capture_output=True, text=True).stdout)
def new(path):
    with open(path, "rb") as f: return tomllib.load(f)
om, nm = old("Cargo.toml"), new("Cargo.toml")
members = nm.get("workspace", {}).get("members", [])
ok_members = sorted(members) == sorted(om["workspace"]["members"] + [member])
om["workspace"]["members"] = members
ok_rest = om == nm
ok = ok_members and ok_rest
why = ("" if ok_members else f" members {members}") + ("" if ok_rest else " a key besides the members list changed")
print(("PASS" if ok else "FAIL") + " root_manifest_only_gains_the_member" + ("" if ok else ":" + why))
ol, nl = old("Cargo.lock"), new("Cargo.lock")
now = {(p["name"], p["version"]): p for p in nl["package"]}
lost = []
rest = lambda e: {k: v for k, v in e.items() if k != "dependencies"}
for p in ol["package"]:
    q = now.get((p["name"], p["version"]))
    if q is None: lost.append(f"{p['name']} {p['version']} gone"); continue
    if p["name"].startswith("moirai") and p != q: lost.append(f"{p['name']} changed")
    elif rest(p) != rest(q) or not set(p.get("dependencies", [])) <= set(q.get("dependencies", [])): lost.append(f"{p['name']} {p['version']} changed")
ok2 = not lost and ol.get("version") == nl.get("version")
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_9eed5b8_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if ok and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 4. The adapter's closure (normal, dev, build): moirai-netstack, moirai-protocol and moirai-crdt present,
#    every moirai* package a member of this workspace, no moirai-netstack-transport.
meta=$(cargo_ metadata --format-version 1 --locked 2>/dev/null)
echo "$meta" | python3 -I -c '
import json, sys
crate = sys.argv[1]
try:
    m = json.loads(sys.stdin.read())
except ValueError:
    print("FAIL adapter_dependency_closure: cargo metadata --locked failed (no such crate yet, a manifest error, or Cargo.lock out of date)"); sys.exit(1)
names = {p["id"]: p["name"] for p in m["packages"]}
nodes = {n["id"]: n for n in m["resolve"]["nodes"]}
ws = set(m["workspace_members"])
roots = [i for i, n in names.items() if n == crate and i in ws]
if not roots:
    print(f"FAIL adapter_dependency_closure: {crate} is not a workspace member"); sys.exit(1)
seen, todo = set(), list(roots)
while todo:
    i = todo.pop()
    if i in seen: continue
    seen.add(i)
    todo += [d["pkg"] for d in nodes[i]["deps"]]
got = {names[i] for i in seen}
why = [f"{n} missing" for n in ("moirai-netstack", "moirai-protocol", "moirai-crdt") if n not in got]
why += [f"{names[i]} not from this workspace" for i in seen if names[i].startswith("moirai") and i not in ws]
why += ["moirai-netstack-transport present"] if "moirai-netstack-transport" in got else []
print("PASS adapter_dependency_closure" if not why else "FAIL adapter_dependency_closure: " + ", ".join(sorted(why)))
sys.exit(0 if not why else 1)
' "$CRATE" || fail=1

# 5. The adapter's tests, every NW.31d oracle test among the passes.
out=$(cargo_ test -p "$CRATE" --locked -j 2 2>&1); code=$?
missing=""
for t in "${ORACLE_TESTS[@]}"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done
echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 60
report adapter_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"

# 6. Regression: the testkit (NW.31c oracle) and moirai-netstack (NW.31a oracle); counts at 9eed5b8 in the spec.
out=$(cargo_ test -p moirai-netstack-testkit -p moirai-netstack --locked -j 2 --no-fail-fast 2>&1); code=$?
echo "$out" | grep -E '^error|^test result|FAILED' | head -n 20
report netstack_and_testkit_tests "$code" "exit $code"

echo "NW31D_GATE fail=$fail"
exit $fail
