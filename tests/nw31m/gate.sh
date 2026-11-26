#!/usr/bin/env bash
# NW.31m gate (vault spec/items/NW.31m.md): the ask-back on SyncRequest in
# moirai-netstack-node, as 388715b on the Detune line. Checks, each printed
# PASS or FAIL. Exit 0 only when all pass, 2 on a set-up error.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=true CARGO_INCREMENTAL=0
# No debug info: the Zenoh build is large and the disk is short; nothing reads it.
export CARGO_PROFILE_DEV_DEBUG=0
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=8c7aa98
LOCKS=${NW31M_LOCKS:-$HOME/.cache/cea-crdt-scratch}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31m/target}
SCRATCH=${NW31M_SCRATCH:-$HOME/.cache/cea-crdt-scratch/NW.31m/gate}
ORACLE=(tests/nw31m moirai-netstack-node/tests/nw31m_ask_back.rs adapters/crdt/tests/nw31m_upstream_ask_back.rs)
CRATES=(-p moirai-netstack -p moirai-netstack-node -p moirai-netstack-testkit -p moirai-netstack-crdt -p moirai-netstack-http -p moirai-netstack-router -p moirai-netstack-start)
NEW_TESTS=(
  "tests/nw31m_ask_back.rs nw31m_toy_asks_back_once_per_unchanged_lead"
  "tests/nw31m_ask_back.rs nw31m_toy_no_ask_back_when_the_frontier_is_covered"
  "tests/nw31m_ask_back.rs nw31m_toy_a_log_with_no_history_does_not_ask_back"
  "tests/nw31m_ask_back.rs nw31m_toy_one_sided_pause_then_resume"
  "tests/nw31m_upstream_ask_back.rs nw31m_upstream_asks_back_once_per_unchanged_lead"
  "tests/nw31m_upstream_ask_back.rs nw31m_upstream_no_ask_back_when_the_frontier_is_covered"
  "tests/nw31m_upstream_ask_back.rs nw31m_upstream_a_log_with_no_history_does_not_ask_back"
  "tests/nw31m_upstream_ask_back.rs nw31m_upstream_one_sided_pause_then_resume"
)
# NW.31k's gate, every check but its change set (this item's paths are not its).
NW31K_KEEP=(oracle_unchanged lock_packages_unchanged upstream_commits_stand_alone in_process_transfer log_serde_round_trip composite_transfer snapshot_refusals upstream_diff_applies upstream_alone_in_process_transfer upstream_alone_log_serde upstream_alone_composite_transfer nw31j_http_tests nw31j_crdt_tests nw31j_start_tests nw31j_node_builds nw31j_e2e_three_generated_nodes_router_pause_resume nw31j_generated_crate_is_arachne_output nw31j_fmt e2e_late_awset_node_joins_by_state_transfer readme_network_section_states_state_transfer fmt)
cargo_() { flock "$LOCKS/cargo.lock" nice -n 10 cargo "$@"; }
cargo_nodes() { flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" nice -n 10 cargo "$@"; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "SETUP: $BASE is not in this repository"; exit 2; }
mkdir -p "$SCRATCH" "$CARGO_TARGET_DIR" || exit 2
changes() { { git diff --name-only "$BASE" -- "$@"; git status --porcelain --untracked-files=all -- "$@" | cut -c4-; } | sort -u; }

# 1. The change set since 8c7aa98 stays in the item's paths (the oracle's own
# files are checked by 2); upstream's crates, the transport, http, router and
# start crates and vendor are untouched.
stray=$(changes . | grep -Ev '^(moirai-netstack/src/|moirai-netstack-node/src/|moirai-netstack-testkit/src/|adapters/crdt/src/|tests/nw31m/|moirai-netstack-node/tests/nw31m_ask_back\.rs$|adapters/crdt/tests/nw31m_upstream_ask_back\.rs$|Cargo\.lock$|README\.md$)')
report change_set_in_scope "$([ -z "$stray" ]; echo $?)" "$(echo $stray)"

# 2. The oracle files: unchanged since the commit that added this gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31m/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31m/gate.sh is not committed"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 3. Wire v1 unchanged: moirai-netstack's frame, payload, JSON and log id
# files are 8c7aa98's, and moirai-netstack/src only gains lines (a port method).
wire=$(changes moirai-netstack/src/frame.rs moirai-netstack/src/log_payload.rs moirai-netstack/src/json.rs moirai-netstack/src/log_id.rs)
removed=$(git diff -U0 "$BASE" -- moirai-netstack/src | grep -E '^-' | grep -Ev '^---' | head -n 3)
report wire_v1_unchanged_port_additive "$([ -z "$wire" ] && [ -z "$removed" ]; echo $?)" "changed: $wire removed: $removed"

# 4. No package enters or leaves Cargo.lock.
pkgs() { grep -E '^name = ' | sort; }
lock=$(diff <(git show "$BASE:Cargo.lock" | pkgs) <(pkgs < Cargo.lock))
report lock_packages_unchanged "$([ -z "$lock" ]; echo $?)" "$(echo $lock | head -c 300)"

# 5. The router binary, then the netstack crates' tests in one run: the
# oracle's 8 by name, every test of tests/nw31m/baseline-tests.txt (each
# test that passed at 8c7aa98, by file and name) still ok, nothing failing.
router="$CARGO_TARGET_DIR/debug/moirai-netstack-router"
rm -f "$router"
out=$(cargo_ build -j 2 --locked -p moirai-netstack-router --bin moirai-netstack-router 2>&1); code=$?
[ $code = 0 ] && [ -x "$router" ] || { echo "SETUP: no router binary (exit $code): $(echo "$out" | grep -E '^error' | head -n 2)"; exit 2; }
out=$(NW31I_ROUTER_BIN="$router" NW31I_DIR="$SCRATCH/start-run" cargo_nodes test -j 2 --locked --no-fail-fast "${CRATES[@]}" -- --test-threads=1 2>&1); code=$?
echo "$out" > "$SCRATCH/tests.log"
oks=$(echo "$out" | awk '/^ *Running / {f=$2} /^ *Doc-tests / {f="doc:"$2} /^test .* \.\.\. ok$/ {print f, $2}' | sort -u)
missing=""; for t in "${NEW_TESTS[@]}"; do echo "$oks" | grep -qxF "$t" || missing+=" ${t#* }"; done
report ask_back_oracle_tests "$([ -z "$missing" ]; echo $?)" "not ok:$missing $(echo "$out" | grep -E 'panicked' | head -n 4)"
lost=$(comm -23 tests/nw31m/baseline-tests.txt <(echo "$oks"))
report existing_tests_green_by_name "$([ $code = 0 ] && [ -z "$lost" ]; echo $?)" "exit $code; not ok: $(echo $lost | head -c 400)"

# 6. NW.31k's gate, its checks but the change set: each named one PASS, and
# no FAIL line but its change set. It runs NW.31j's
# gate, which builds arachne-cli at NW.31ka's 693dcb2, the generated json
# crate, its node (tests/nw31j/node.rs) and the router into CARGO_TARGET_DIR.
out=$(NW31K_SCRATCH="$SCRATCH/nw31k" NW31K_LOCKS="$LOCKS" bash tests/nw31k/gate.sh 2>&1); code=$?
echo "$out" > "$SCRATCH/nw31k-gate.log"
echo "$out" | grep -E '^(PASS|FAIL|SETUP)' | sed 's/^/  nw31k: /'
if [ $code = 2 ]; then
  report nw31k_gate_kept_checks 1 "set-up failed: $(echo "$out" | grep -E '^SETUP' | head -n 2)"
else
  bad=""; for check in "${NW31K_KEEP[@]}"; do echo "$out" | grep -qx "PASS $check" || bad+=" $check"; done
  other=$(echo "$out" | grep -E '^FAIL ' | grep -v '^FAIL change_set_in_scope' | cut -c1-120)
  report nw31k_gate_kept_checks "$([ -z "$bad" ] && [ -z "$other" ]; echo $?)" "not PASS:$bad; other FAIL: $(echo $other)"
fi

# 7. End to end, under nodes.lock, 300 s at most: the router and NW.31j's
# generated json node from step 6, this item's driver built in a consuming
# workspace (serde_json only, the lane's lock).
node="$CARGO_TARGET_DIR/debug/nw31j-node"
ws="$SCRATCH/e2e"; rm -rf "$ws"; mkdir -p "$ws/src" || exit 2
cp tests/nw31m/e2e.rs "$ws/src/e2e.rs" && grep -v '^//!' tests/nw31j/e2e.rs > "$ws/src/nw31j_e2e.rs" && cp Cargo.lock "$ws/Cargo.lock" || exit 2
cat > "$ws/Cargo.toml" <<EOF
[package]
name = "nw31m-e2e"
version = "0.0.0"
edition = "2024"
publish = false

[[bin]]
name = "nw31m-e2e"
path = "src/e2e.rs"

[dependencies]
serde_json = "1.0"

[workspace]
EOF
e2e="$CARGO_TARGET_DIR/debug/nw31m-e2e"; rm -f "$e2e"
out=$(cd "$ws" && cargo_ build -j 2 2>&1); code=$?
if [ $code != 0 ] || [ ! -x "$e2e" ]; then
  report e2e_one_sided_pause_generated_nodes 1 "not run: the driver does not build (exit $code): $(echo "$out" | grep -E '^error' -A 3 | head -n 6)"
elif [ ! -x "$node" ] || [ ! -x "$router" ]; then
  report e2e_one_sided_pause_generated_nodes 1 "not run: no node or router binary from NW.31j's gate"
else
  sha256sum "$node" "$router" "$e2e" > "$SCRATCH/e2e-binaries.sha256"
  out=$(NW31M_ROUTER_BIN="$router" NW31M_NODE_BIN="$node" NW31M_DIR="$SCRATCH/run" flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" timeout 300 "$e2e" 2>&1); code=$?
  echo "$out" > "$SCRATCH/e2e.log"
  echo "$out" | grep -E '^NW31M' | grep -v 'E2E OK' | sed 's/^/  /'
  report e2e_one_sided_pause_generated_nodes "$([ $code = 0 ] && [ "$(echo "$out" | grep -E '^NW31M' | tail -n 1)" = "NW31M E2E OK" ]; echo $?)" "exit $code: $(echo "$out" | grep -E 'panicked|NW31J [a-z-]+:|set-up:' | head -n 3)"
fi

# 8. README changes only in its Network stack section, and the lines added
# there name the ask-back on SyncRequest.
python3 -I - "$BASE" <<'PY' || fail=1
import re, subprocess, sys
base = sys.argv[1]
old = subprocess.run(["git", "show", f"{base}:README.md"], check=True, capture_output=True, text=True).stdout
with open("README.md", encoding="utf-8") as f: new = f.read()
def sections(text):
    out, cur = {}, "(preamble)"
    for line in text.splitlines():
        if line.startswith("## "): cur = line
        out.setdefault(cur, []).append(line)
    return out
NET = "## Network stack (moirai-netstack)"
os_, ns = sections(old), sections(new)
why = []
if list(os_) != list(ns): why.append(f"headings {list(ns)}")
why += [f"{h} changed" for h in os_ if h != NET and os_[h] != ns.get(h)]
added = "\n".join(l for l in ns.get(NET, []) if l not in os_.get(NET, []))
for word, pattern in [("asking back", r"asks?[- ]back|asking back"), ("SyncRequest", r"SyncRequest")]:
    if not re.search(pattern, added, re.I): why.append(f"the lines added to the Network stack section do not name {word}")
print(f"FAIL readme_names_the_ask_back: {'; '.join(why)}" if why else "PASS readme_names_the_ask_back")
sys.exit(bool(why))
PY

# 9. Formatting: every Rust file changed since 8c7aa98 is rustfmt-clean at the defaults.
unclean=""
for f in $(changes moirai-netstack moirai-netstack-node moirai-netstack-testkit adapters/crdt tests/nw31m | grep '\.rs$'); do
  [ -f "$f" ] || continue
  rustfmt --check --edition 2024 "$f" >/dev/null 2>&1 || unclean+=" $f"
done
report fmt "$([ -z "$unclean" ]; echo $?)" "$unclean"

echo "NW31M_GATE fail=$fail"
exit $fail
