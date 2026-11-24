#!/usr/bin/env bash
# NW.31a gate: the core network crate moirai-netstack beside upstream Moirai 7fe5ea2.
# Checks, each printed PASS or FAIL: upstream's four crates unchanged; the root
# manifest only gains the member; every Cargo.lock entry of 7fe5ea2 kept;
# no moirai* package in the crate's dependency closure; no interpreter,
# descriptor, metamodel or governance word in its source; its tests green with
# every NW.31a oracle test run; the workspace suite as green as at 7fe5ea2.
# Exit 0 only when all pass. Cargo runs under the shared lock when it exists.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=7fe5ea2
CRATE=moirai-netstack
UPSTREAM=(moirai-crdt moirai-protocol moirai-macros moirai-fuzz)
LOCK=${NW31A_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
ORACLE_TESTS=(
  every_601c887_frame_decodes_to_its_variant_and_re_encodes_byte_identical
  every_frame_601c887_refuses_is_refused
  a_frame_of_a_type_wire_v1_does_not_name_passes_through_whole
  the_deflated_log_decodes_to_the_donors_export
  the_port_defaults_follow_the_design
  an_extension_that_writes_no_method_admits_local_ops
)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
cd "$ROOT" || exit 2

# 1. Upstream's crates: no tracked change since 7fe5ea2, no new file.
git cat-file -e "$BASE^{commit}" || { echo "FAIL: $BASE is not in this repository"; exit 2; }
changed=$(git diff --name-only "$BASE" -- "${UPSTREAM[@]}"; git status --porcelain --untracked-files=all -- "${UPSTREAM[@]}")
report upstream_crates_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"

# 2 and 3. Root manifest and lock against 7fe5ea2.
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
# Every 7fe5ea2 entry stays: same version, source and checksum. Upstream's own
# entries (moirai-*) stay whole; another entry may only gain dependencies (a
# feature the new crate turns on, e.g. serde_json's indexmap).
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
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_7fe5ea2_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if ok and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 4. Dependency closure of the crate (normal, dev and build): no moirai* package but itself.
meta=$(cargo_ metadata --format-version 1 --locked 2>/dev/null)
echo "$meta" | python3 -I -c '
import json, sys
crate = sys.argv[1]
try:
    m = json.loads(sys.stdin.read())
except ValueError:
    print("FAIL no_moirai_dependency: cargo metadata --locked failed (a manifest error, or Cargo.lock out of date)"); sys.exit(1)
names = {p["id"]: p["name"] for p in m["packages"]}
nodes = {n["id"]: n for n in m["resolve"]["nodes"]}
roots = [i for i, n in names.items() if n == crate and i in m["workspace_members"]]
if not roots:
    print(f"FAIL no_moirai_dependency: {crate} is not a workspace member"); sys.exit(1)
seen, todo = set(), list(roots)
while todo:
    i = todo.pop()
    if i in seen: continue
    seen.add(i)
    todo += [d["pkg"] for d in nodes[i]["deps"]]
bad = sorted({names[i] for i in seen if names[i].startswith("moirai") and names[i] != crate})
print(("FAIL no_moirai_dependency: " + ", ".join(bad)) if bad else "PASS no_moirai_dependency")
sys.exit(1 if bad else 0)
' "$CRATE" || fail=1

# 5. Words: every .rs file of the crate outside tests/.
sources=$(find "$CRATE" -path "$CRATE/tests" -prune -o -type f -name '*.rs' -print 2>/dev/null)
if [ -z "$sources" ]; then
  report no_notion_words 1 "no source under $CRATE"
else
  hits=$(echo "$sources" | xargs grep -niP 'interp(?!olat)|descriptor|meta[-_ ]?model|govern')
  report no_notion_words "$([ -z "$hits" ]; echo $?)" "$hits"
fi

# 6. The crate's tests, every NW.31a oracle test among the passes.
out=$(cargo_ test -p "$CRATE" --locked -j 2 2>&1); code=$?
missing=""
for t in "${ORACLE_TESTS[@]}"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done
echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 40
report crate_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"

# 7. The workspace suite: 0 failed at 7fe5ea2 (183 passed, 29 ignored), so 0 failed now.
out=$(cargo_ test --workspace --locked -j 2 --no-fail-fast 2>&1); code=$?
echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 40
report workspace_tests "$code" "exit $code"

echo "NW31A_GATE fail=$fail"
exit $fail
