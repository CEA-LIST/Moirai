#!/usr/bin/env bash
# NW.31g gate: moirai-netstack-http (NW.31 step S6, HTTP) on akira/moirai-network-s6
# from d1e1c22. Checks, each printed PASS or FAIL: the change set stays in the
# item's paths (upstream's crates and every earlier netstack crate unchanged);
# moirai-netstack-node/src only gains a module and its one mod line; this item's
# oracle files are unchanged since the commit that added them; the root manifest
# only gains the member and every Cargo.lock entry of d1e1c22 is kept; the HTTP
# crate's closure holds no moirai crate but the netstack ones and no zenoh; no
# 0.0.0.0 and no #[ignore] in the crate; its 15 oracle tests ok by name (under
# nodes.lock); moirai-netstack, -node and -testkit green with 55 passed
# (regression, as at d1e1c22). Exit 0 only when all pass.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=d1e1c22
CRATE=moirai-netstack-http
LOCK=${NW31G_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
NODES=${NW31G_NODES_LOCK:-$HOME/.cache/cea-crdt-scratch/nodes.lock}
ORACLE=(tests/nw31g moirai-netstack-http/tests/nw31g_http.rs)
HTTP_TESTS=(
  health_endpoint_names_the_replica_and_its_log
  log_id_endpoint_serves_the_id_on_its_own
  state_endpoint_is_unchanged_beside_the_metamodel_route
  op_endpoint_is_unchanged_beside_the_metamodel_route
  both_op_routes_take_an_operation_nested_past_the_default_recursion_limit
  the_path_parser_names_the_model_routes_and_nothing_by_prefix
  mp3_the_model_routes_parse_the_id_and_reject_a_malformed_one
  the_state_routes_serve_the_version_they_read_at_as_the_etag
  a_batch_is_one_submission_and_its_outcome_sets_the_status
  a_batch_body_of_the_wrong_shape_is_400_and_reaches_nothing
  a_batch_the_loop_did_not_take_in_time_is_expired_and_never_applied
  a_batch_the_loop_took_in_time_is_answered_even_past_the_deadline
  an_operation_the_loop_did_not_take_in_time_is_expired_and_never_applied
  an_application_route_is_served_through_the_extension_point
  the_other_generic_routes_answer_as_on_601c887_and_the_later_routes_are_not_served
)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
cargo_nodes() { if [ -e "$LOCK" ]; then flock "$LOCK" flock "$NODES" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
names_ok() { local out=$1; shift; local missing=""; for t in "$@"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done; echo "$missing"; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "FAIL: $BASE is not in this repository"; exit 2; }
changes() { { git diff --name-only "$BASE" -- "$@"; git status --porcelain --untracked-files=all -- "$@" | cut -c4-; } | sort -u; }

# 1. The change set since d1e1c22 stays in the item's paths.
stray=$(changes . | grep -Ev '^(moirai-netstack-http/|moirai-netstack-node/src/|Cargo\.toml$|Cargo\.lock$|tests/nw31g/)')
report change_set_in_scope "$([ -z "$stray" ]; echo $?)" "$(echo $stray)"

# 2. moirai-netstack-node/src: new files, and lib.rs gains at most one mod line and loses nothing.
why=""
for f in $(changes moirai-netstack-node/src); do
  [ "$f" = moirai-netstack-node/src/lib.rs ] && continue
  git cat-file -e "$BASE:$f" 2>/dev/null && why+=" $f changed"
done
diff_lib=$(git diff "$BASE" -- moirai-netstack-node/src/lib.rs | grep -E '^[-+]' | grep -Ev '^(\+\+\+|---) ')
added=$(echo "$diff_lib" | grep -c '^+'); removed=$(echo "$diff_lib" | grep -c '^-')
[ "$removed" = 0 ] || why+=" lib.rs loses $removed lines"
[ "$added" -le 1 ] || why+=" lib.rs gains $added lines"
[ "$added" = 0 ] || echo "$diff_lib" | grep -Eq '^\+(pub )?mod [a-z_0-9]+;$' || why+=" lib.rs gains a line that is not a mod line"
report node_only_gains_an_accessor_module "$([ -z "$why" ]; echo $?)" "$why"

# 3. This item's oracle files: unchanged since the commit that added the gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31g/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31g/gate.sh is not committed"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 4. Root manifest and lock against d1e1c22.
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
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_d1e1c22_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if ok and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 5. The HTTP crate's closure: only netstack crates among moirai*, a normal dependency
#    on the node, the transport (when named) without default features, and no zenoh
#    in its own build (normal and dev edges, features resolved for this crate alone).
meta=$(cargo_ metadata --format-version 1 --locked 2>/dev/null)
echo "$meta" | python3 -I -c '
import json, sys
crate = sys.argv[1]
allowed = {crate, "moirai-netstack", "moirai-netstack-node", "moirai-netstack-transport", "moirai-netstack-testkit"}
try:
    m = json.loads(sys.stdin.read())
except ValueError:
    print("FAIL http_dependencies: cargo metadata --locked failed (a manifest error, or Cargo.lock out of date)"); sys.exit(1)
pkgs = {p["id"]: p for p in m["packages"]}
nodes = {n["id"]: n for n in m["resolve"]["nodes"]}
roots = [i for i, p in pkgs.items() if p["name"] == crate and i in m["workspace_members"]]
if not roots:
    print(f"FAIL http_dependencies: {crate} is not a workspace member"); sys.exit(1)
seen, todo = set(), list(roots)
while todo:
    i = todo.pop()
    if i in seen: continue
    seen.add(i)
    todo += [d["pkg"] for d in nodes[i]["deps"]]
bad = sorted({pkgs[i]["name"] for i in seen if pkgs[i]["name"].startswith("moirai") and pkgs[i]["name"] not in allowed})
http = pkgs[roots[0]]
why = []
if bad: why.append("moirai packages outside the netstack crates: " + ", ".join(bad))
normal = [d for d in http["dependencies"] if d["kind"] is None]
if not any(d["name"] == "moirai-netstack-node" for d in normal): why.append("no normal dependency on moirai-netstack-node")
if any(d["name"] == "moirai-netstack-transport" and d["uses_default_features"] for d in http["dependencies"]): why.append("moirai-netstack-transport taken with default features")
if "zenoh" in http.get("features", {}).get("default", []): why.append("zenoh on by default")
print("PASS http_dependencies" if not why else "FAIL http_dependencies: " + "; ".join(why))
sys.exit(0 if not why else 1)
' "$CRATE" || fail=1
tree=$(cargo_ tree -p "$CRATE" -e normal,dev --locked --prefix none 2>&1); code=$?
zen=$(echo "$tree" | grep -E '^zenoh(-[a-z0-9_-]+)? ' | sort -u | head -n 3)
report http_builds_without_zenoh "$([ $code = 0 ] && [ -z "$zen" ]; echo $?)" "tree exit $code $(echo $zen)"

# 6. Loopback only, nothing ignored.
wild=$(grep -rln '0\.0\.0\.0' "$CRATE/src" 2>/dev/null)
ignored=$(grep -rln '#\[ignore' "$CRATE" 2>/dev/null)
report http_no_wildcard_bind_and_nothing_ignored "$([ -z "$wild$ignored" ]; echo $?)" "$wild $ignored"

# 7. The HTTP crate's tests, every oracle test among the passes; sockets on 127.0.0.1 under nodes.lock.
out=$(cargo_nodes test -p "$CRATE" --locked -j 2 2>&1); code=$?
missing=$(names_ok "$out" "${HTTP_TESTS[@]}")
echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 40
report http_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"

# 8. Regression: moirai-netstack 6, moirai-netstack-node 18, moirai-netstack-testkit 31 (55 passed, 0 failed at d1e1c22).
out=$(cargo_nodes test -p moirai-netstack -p moirai-netstack-node -p moirai-netstack-testkit --locked -j 2 --no-fail-fast 2>&1); code=$?
passed=$(echo "$out" | sed -n 's/^test result: [a-zA-Z]*\. \([0-9]*\) passed.*/\1/p' | paste -sd+ | bc)
echo "$out" | grep -E '^error|FAILED|panicked' | head -n 20
report regression_55_passed "$([ $code = 0 ] && [ "${passed:-0}" = 55 ]; echo $?)" "exit $code, passed ${passed:-0}"

echo "NW31G_GATE fail=$fail"
exit $fail
