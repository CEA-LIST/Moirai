#!/usr/bin/env bash
# NW.31i gate: moirai-netstack-start (the launcher: a node started from the
# environment as 601c887's model_node does) on akira/moirai-network-s7 from
# aefd881. Checks, each printed PASS or FAIL: the change set stays in the
# item's paths; the oracle files are unchanged since the commit that added this
# gate; the root manifest only gains the member; Cargo.lock keeps every aefd881
# entry and gains only the start crate; the README changes only in its
# "Network stack" section, which names the start crate; the router changes in
# doc text only and no longer says the peers keep converging or that it
# carries client-mode nodes; the start crate's dependencies; its 4 tests ok by
# name (the router binary built, under nodes.lock); cargo fmt on the crate;
# and the regression, the router's 8 tests and the transport's
# nw16b_zenoh_pin_l0. Exit 0 only when all pass, 2 on a set-up error.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=true CARGO_INCREMENTAL=0
# No debug info: the Zenoh build is large and the disk is short; nothing reads it.
export CARGO_PROFILE_DEV_DEBUG=0
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=aefd881
CRATE=moirai-netstack-start
ROUTER=moirai-netstack-router
LOCKS=${NW31I_LOCKS:-$HOME/.cache/cea-crdt-scratch}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31i/target}
export NW31I_DIR=${NW31I_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31i/run}
ORACLE=(tests/nw31i "$CRATE/tests")
START_TESTS=(
  nw31i_node
  nw31i_tcp_port_0_nodes_report_their_ports_read_a_late_peers_file_and_converge
  h_p_a_port_0_node_with_a_peers_file_holds_its_reported_port_and_meshes_zenoh
  nw31i_three_zenoh_nodes_told_only_the_router_converge
)
ROUTER_TESTS=(
  the_router_opens_router_mode_on_the_port_it_is_given_with_multicast_scouting_off
  the_default_port_is_7447
  ra_without_a_list_the_router_opens_the_configuration_it_opened_before
  rb_with_a_list_the_router_adds_one_subregion_per_name_then_a_catch_all
  rc_router_regions_reads_its_names_in_order_as_written
  rd_router_regions_refuses_an_empty_name_a_name_over_32_bytes_and_a_duplicate
  the_router_is_a_workspace_crate_on_the_pinned_zenoh
  nw31h_the_router_depends_on_no_moirai_crate
)
cargo_() { flock "$LOCKS/cargo.lock" nice -n 10 cargo "$@"; }
cargo_nodes() { flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" nice -n 10 cargo "$@"; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
names_ok() { local out=$1; shift; local missing=""; for t in "$@"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done; echo "$missing"; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "SETUP: $BASE is not in this repository"; exit 2; }
mkdir -p "$NW31I_DIR" "$CARGO_TARGET_DIR" || exit 2
changes() { { git diff --name-only "$BASE" -- "$@"; git status --porcelain --untracked-files=all -- "$@" | cut -c4-; } | sort -u; }

# 1. The change set since aefd881 stays in the item's paths.
stray=$(changes . | grep -Ev "^($CRATE/|Cargo\.toml$|Cargo\.lock$|README\.md$|tests/nw31i/|$ROUTER/src/lib\.rs$|$ROUTER/Cargo\.toml$)")
report change_set_in_scope "$([ -z "$stray" ]; echo $?)" "$(echo $stray)"

# 2. This item's oracle files: unchanged since the commit that added the gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31i/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31i/gate.sh is not committed"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 3 to 6. Root manifest, lock, README and the router's doc text against aefd881.
python3 -I - "$BASE" "$CRATE" "$ROUTER" <<'PY' || fail=1
import re, subprocess, sys, tomllib
base, crate, router = sys.argv[1], sys.argv[2], sys.argv[3]
def show(path): return subprocess.run(["git", "show", f"{base}:{path}"], check=True, capture_output=True, text=True).stdout
def read(path):
    with open(path, encoding="utf-8") as f: return f.read()
bad = 0
def report(name, why):
    global bad
    print(f"FAIL {name}: {why}" if why else f"PASS {name}")
    bad |= bool(why)
om, nm = tomllib.loads(show("Cargo.toml")), tomllib.loads(read("Cargo.toml"))
members = nm.get("workspace", {}).get("members", [])
why = []
if sorted(members) != sorted(om["workspace"]["members"] + [crate]): why.append(f"members {members}")
om["workspace"]["members"] = members
if om != nm: why.append("a key besides the members changed")
report("root_manifest_only_gains_the_member", "; ".join(why))
key = lambda p: (p["name"], p["version"], p.get("source"))
ol = {key(p): p for p in tomllib.loads(show("Cargo.lock"))["package"]}
nl = {key(p): p for p in tomllib.loads(read("Cargo.lock"))["package"]}
why = [f"{k} changed or gone" for k in ol if ol[k] != nl.get(k)]
gained = sorted(k for k in nl if k not in ol)
if [k[0] for k in gained] != [crate] or gained[0][2] is not None: why.append(f"gained {gained}")
report("lock_keeps_every_aefd881_entry_and_gains_only_the_start_crate", "; ".join(why[:8]))
def sections(text):
    out, cur = {}, "(preamble)"
    for line in text.splitlines():
        if line.startswith("## "): cur = line
        out.setdefault(cur, []).append(line)
    return out
NET = "## Network stack (moirai-netstack)"
os_, ns = sections(show("README.md")), sections(read("README.md"))
why = []
if list(os_) != list(ns): why.append(f"headings {list(ns)}")
why += [f"{h} changed" for h in os_ if h != NET and os_[h] != ns.get(h)]
if crate not in "\n".join(ns.get(NET, [])): why.append(f"the Network stack section does not name {crate}")
report("readme_changes_only_the_network_section_and_names_the_start_crate", "; ".join(why))
why = []
lib = f"{router}/src/lib.rs"
diff = subprocess.run(["git", "diff", "-U0", base, "--", lib], check=True, capture_output=True, text=True).stdout
code = [l for l in diff.splitlines() if l[:1] in "+-" and not l.startswith(("+++", "---")) and not re.match(r"^[+-]\s*(//|$)", l)]
if code: why.append(f"{lib} changes a line that is not a comment: {code[0]!r}")
mod_doc = "\n".join(l for l in read(lib).splitlines() if l.startswith("//!"))
if re.search(r"keep\s+converging", mod_doc, re.I): why.append(f"{lib} still says keep converging")
if "client" in mod_doc.lower() and not re.search(r"untested|not tested|not covered", mod_doc, re.I): why.append(f"{lib} module doc names client mode without saying it is untested")
ot, nt = tomllib.loads(show(f"{router}/Cargo.toml")), tomllib.loads(read(f"{router}/Cargo.toml"))
desc = nt.get("package", {}).get("description", "")
ot["package"].pop("description", None); nt.get("package", {}).pop("description", None)
if ot != nt: why.append(f"{router}/Cargo.toml changes a key besides the description")
if re.search(r"keep\s+converging", desc, re.I): why.append("the router description says keep converging")
if "client" in desc.lower() and not re.search(r"untested|not tested|not covered", desc, re.I): why.append("the router description names client mode without saying it is untested")
report("router_changes_doc_text_only", "; ".join(why))
sys.exit(bad)
PY

# 7. The start crate's dependencies: among moirai crates only the four netstack
# crates as normal dependencies; nothing of Moirai's own crates, the crdt
# adapter or the router anywhere in its closure (dev included, every feature).
meta=$(cargo metadata --format-version 1 --locked --offline 2>&1); code=$?
if [ $code != 0 ]; then
  report start_dependencies 1 "cargo metadata exit $code: $(echo "$meta" | grep -E '^error' | head -n 1)"
else
  why=$(echo "$meta" | python3 -I -c '
import json, sys
m = json.load(sys.stdin); crate = sys.argv[1]
pkg = [p for p in m["packages"] if p["name"] == crate]
if not pkg: print(f"no package {crate}"); sys.exit()
allowed = {"moirai-netstack", "moirai-netstack-node", "moirai-netstack-transport", "moirai-netstack-http"}
normal = {d["name"] for d in pkg[0]["dependencies"] if d["kind"] is None and d["name"].startswith("moirai")}
if normal - allowed: print(f"normal moirai dependencies {sorted(normal - allowed)}")
' "$CRATE")
  tree=$(cargo tree -p "$CRATE" --all-features -e normal,build,dev --prefix none -f '{p}' --locked --offline 2>&1); code=$?
  [ $code != 0 ] && why+=" cargo tree exit $code"
  bad=$(echo "$tree" | grep -oE '^(moirai-crdt|moirai-protocol|moirai-macros|moirai-fuzz|moirai-netstack-crdt|moirai-netstack-router) ' | sort -u | tr '\n' ' ')
  [ -n "$bad" ] && why+=" closure holds $bad"
  report start_dependencies "$([ -z "$why" ]; echo $?)" "$why"
fi

# 8. The start crate's tests, by name, under nodes.lock, with the router binary built.
bin="$CARGO_TARGET_DIR/debug/$ROUTER"
rm -f "$bin"
out=$(cargo_ build -j 2 --locked -p "$ROUTER" --bin "$ROUTER" 2>&1); code=$?
if [ $code != 0 ] || [ ! -x "$bin" ]; then
  echo "SETUP: no router binary $bin (build exit $code): $(echo "$out" | grep -E '^error' | head -n 2)"; exit 2
fi
out=$(NW31I_ROUTER_BIN="$bin" cargo_nodes test -j 2 --locked --no-fail-fast -p "$CRATE" -- --test-threads=1 2>&1); code=$?
echo "$out" > "$NW31I_DIR/start-tests.log"
missing=$(names_ok "$out" "${START_TESTS[@]}")
echo "$out" | grep -E 'NW31I [a-z-]+: [0-9]+ bind rounds' | sed 's/^/  /'
report start_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked|NW31I .*:|set-up:' | head -n 4)"

# 9. Formatting of the start crate (its oracle tests included).
out=$(cargo fmt --check -p "$CRATE" 2>&1); code=$?
report fmt "$code" "$(echo "$out" | head -n 3)"

# 10. Regression: the router's 8 tests (its doc text changes, its pin test reads
# Cargo.lock) and the transport's pin test (reads Cargo.lock).
out=$(cargo_nodes test -j 2 --locked --no-fail-fast -p "$ROUTER" -- --test-threads=1 2>&1); code=$?
missing=$(names_ok "$out" "${ROUTER_TESTS[@]}")
report regression_router_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"
out=$(cargo_nodes test -j 2 --locked -p moirai-netstack-transport --test nw16b_zenoh_pin_l0 2>&1); code=$?
missing=$(names_ok "$out" the_transport_crate_is_on_the_pinned_zenoh)
report regression_transport_pin "$([ $code = 0 ] && [ -z "$missing" ] && echo "$out" | grep -q 'test result: ok. 1 passed; 0 failed'; echo $?)" "exit $code $(echo "$out" | grep -E '^error|panicked|test result' | head -n 3)"

echo "NW31I_GATE fail=$fail"
exit $fail
