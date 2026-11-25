#!/usr/bin/env bash
# NW.31h gate: moirai-netstack-router (the in-tree Zenoh router as a library
# crate and a binary) on akira/moirai-network-router from 5485f5f. Checks, each
# printed PASS or FAIL: the change set stays in the item's paths; this item's
# oracle files are unchanged since the commit that added this gate; the root
# manifest only gains the member; Cargo.lock keeps every 5485f5f entry and gains
# only the router's; the README changes only in its "Network stack" section,
# which names the router crate; the router's 8 tests ok by name (under
# nodes.lock); the end-to-end run (tests/nw31h/e2e.rs, a consuming workspace in
# scratch: the built router on port 0, three transports told only the router,
# exchange before and after the router is killed); cargo fmt on the crate; and
# the regression, moirai-netstack-transport's nw16b_zenoh_pin_l0 (1 passed at
# 5485f5f). Exit 0 only when all pass, 2 on a set-up error.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=true CARGO_INCREMENTAL=0
# No debug info: the Zenoh build is large and the disk is short; nothing reads it.
export CARGO_PROFILE_DEV_DEBUG=0
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=5485f5f
CRATE=moirai-netstack-router
LOCKS=${NW31H_LOCKS:-$HOME/.cache/cea-crdt-scratch}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31h/target}
SCRATCH=${NW31H_SCRATCH:-$HOME/.cache/cea-crdt-scratch/NW.31h/gate}
ORACLE=(tests/nw31h "$CRATE/tests")
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
mkdir -p "$SCRATCH" "$CARGO_TARGET_DIR" || exit 2
changes() { { git diff --name-only "$BASE" -- "$@"; git status --porcelain --untracked-files=all -- "$@" | cut -c4-; } | sort -u; }

# 1. The change set since 5485f5f stays in the item's paths.
stray=$(changes . | grep -Ev "^($CRATE/|Cargo\.toml$|Cargo\.lock$|README\.md$|tests/nw31h/)")
report change_set_in_scope "$([ -z "$stray" ]; echo $?)" "$(echo $stray)"

# 2. This item's oracle files: unchanged since the commit that added the gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31h/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31h/gate.sh is not committed"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 3 to 5. Root manifest, lock and README against 5485f5f.
python3 -I - "$BASE" "$CRATE" <<'PY' || fail=1
import subprocess, sys, tomllib
base, crate = sys.argv[1], sys.argv[2]
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
report("lock_keeps_every_5485f5f_entry_and_gains_only_the_router", "; ".join(why[:8]))
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
report("readme_changes_only_the_network_section_and_names_the_router", "; ".join(why))
sys.exit(bad)
PY

# 6. The router crate's tests, by name, under nodes.lock.
out=$(cargo_nodes test -j 2 --locked --no-fail-fast -p "$CRATE" -- --test-threads=1 2>&1); code=$?
missing=$(names_ok "$out" "${ROUTER_TESTS[@]}")
report router_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 7. End to end: the built router binary and three transports in a consuming workspace.
bin="$CARGO_TARGET_DIR/debug/$CRATE"
rm -f "$bin"
out=$(cargo_ build -j 2 --locked -p "$CRATE" --bin "$CRATE" 2>&1); code=$?
if [ $code != 0 ] || [ ! -x "$bin" ]; then
  report e2e_router_then_killed 1 "no router binary $bin (build exit $code): $(echo "$out" | grep -E '^error' | head -n 2)"
else
  e2e="$SCRATCH/e2e"; rm -rf "$e2e"; mkdir -p "$e2e/src" "$e2e/run" || exit 2
  cp tests/nw31h/e2e.rs "$e2e/src/main.rs" && cp moirai-netstack-transport/tests/nw15_common/contract.rs "$e2e/src/contract.rs" && cp Cargo.lock "$e2e/Cargo.lock" || exit 2
  printf '[package]\nname = "nw31h-e2e"\nversion = "0.0.0"\nedition = "2024"\npublish = false\n\n[dependencies]\nmoirai-netstack-transport = { path = "%s/moirai-netstack-transport" }\nserde_json = "1.0.133"\n\n[workspace]\n\n[patch.crates-io]\nzenoh = { path = "%s/vendor/zenoh-1.10.1" }\n' "$ROOT" "$ROOT" > "$e2e/Cargo.toml"
  out=$(cd "$e2e" && cargo_ build -j 2 -q 2>&1); code=$?
  if [ $code != 0 ]; then
    echo "SETUP: the e2e consumer does not build: $(echo "$out" | grep -E '^error' | head -n 3)"; exit 2
  fi
  out=$(cd "$e2e" && NW31H_ROUTER_BIN="$bin" NW31H_DIR="$e2e/run" flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" timeout 300 "$CARGO_TARGET_DIR/debug/nw31h-e2e" 2>&1); code=$?
  echo "$out" | grep -E '^NW31H' | grep -v 'E2E OK' | sed 's/^/  /'
  report e2e_router_then_killed "$([ $code = 0 ] && [ "$(echo "$out" | grep -E '^NW31H' | tail -n 1)" = "NW31H E2E OK" ]; echo $?)" "exit $code: $(echo "$out" | grep -E 'panicked|NW31H (before|after):|set-up:' | head -n 3)"
fi

# 8. Formatting of the router crate (its oracle tests included).
out=$(cargo fmt --check -p "$CRATE" 2>&1); code=$?
report fmt "$code" "$(echo "$out" | head -n 3)"

# 9. Regression: the transport crate is still on the pinned zenoh (it reads Cargo.lock).
out=$(cargo_nodes test -j 2 --locked -p moirai-netstack-transport --test nw16b_zenoh_pin_l0 2>&1); code=$?
missing=$(names_ok "$out" the_transport_crate_is_on_the_pinned_zenoh)
report regression_transport_pin "$([ $code = 0 ] && [ -z "$missing" ] && echo "$out" | grep -q 'test result: ok. 1 passed; 0 failed'; echo $?)" "exit $code $(echo "$out" | grep -E '^error|panicked|test result' | head -n 3)"

echo "NW31H_GATE fail=$fail"
exit $fail
