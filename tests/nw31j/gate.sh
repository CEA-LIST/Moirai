#!/usr/bin/env bash
# NW.31j gate (vault spec/items/NW.31j.md): the generated path. A crate that
# upstream Arachne's arachne-cli 3c8c2bf generates from its examples/json.ecore
# (serde on) runs as a node through moirai-netstack-start and a generic
# moirai-netstack-crdt factory, with no network code of its own
# (tests/nw31j/node.rs); three such nodes and the built moirai-netstack-router
# run tests/nw31j/e2e.rs; the two resume routes of 601c887 join
# moirai-netstack-http. Checks, each printed PASS or FAIL. Exit 0 only when all
# pass, 2 on a set-up error.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=true CARGO_INCREMENTAL=0
# No debug info: the Zenoh build is large and the disk is short; nothing reads it.
export CARGO_PROFILE_DEV_DEBUG=0
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=fa082c4
# NW.31k round 3 sets NW31J_ARACHNE_COMMIT to NW.31ka's 693dcb2 (3c8c2bf plus serde on the
# generated log types), which state transfer's factory bound needs.
ARACHNE_COMMIT=${NW31J_ARACHNE_COMMIT:-3c8c2bf}
ARACHNE_REPO=${NW31J_ARACHNE_REPO:-$HOME/Documents/Projects/cea-crdt/repos/arachne}
LOCKS=${NW31J_LOCKS:-$HOME/.cache/cea-crdt-scratch}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31j/target}
ARACHNE_TARGET=${NW31J_ARACHNE_TARGET:-$HOME/.cache/cea-crdt-scratch/NW.31j/target-arachne}
SCRATCH=${NW31J_SCRATCH:-$HOME/.cache/cea-crdt-scratch/NW.31j/gate}
ORACLE=(tests/nw31j moirai-netstack-http/tests)
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
  nw31j_resume_peer_hands_the_buffer_over_and_asks_the_peer_for_a_delta
  nw31j_resume_all_resumes_every_paused_peer_and_asks_each_for_a_delta
  nw31j_resume_of_a_peer_not_paused_answers_the_transport_refusal
)
START_TESTS=(
  nw31i_node
  nw31i_tcp_port_0_nodes_report_their_ports_read_a_late_peers_file_and_converge
  h_p_a_port_0_node_with_a_peers_file_holds_its_reported_port_and_meshes_zenoh
  nw31i_three_zenoh_nodes_told_only_the_router_converge
  nw31i_start_error_node
  nw31i_unsupported_variables_are_start_errors
)
cargo_() { flock "$LOCKS/cargo.lock" nice -n 10 cargo "$@"; }
cargo_nodes() { flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" nice -n 10 cargo "$@"; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
names_ok() { local out=$1; shift; local missing=""; for t in "$@"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done; echo "$missing"; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "SETUP: $BASE is not in this repository"; exit 2; }
git -C "$ARACHNE_REPO" cat-file -e "$ARACHNE_COMMIT^{commit}" || { echo "SETUP: $ARACHNE_COMMIT is not in $ARACHNE_REPO"; exit 2; }
mkdir -p "$SCRATCH" "$CARGO_TARGET_DIR" "$ARACHNE_TARGET" || exit 2
changes() { { git diff --name-only "$BASE" -- "$@"; git status --porcelain --untracked-files=all -- "$@" | cut -c4-; } | sort -u; }

# 1. The change set since fa082c4 stays in the item's paths; upstream's crates
# and every other netstack crate are untouched.
stray=$(changes . | grep -Ev '^(adapters/crdt/src/|moirai-netstack-http/src/|moirai-netstack-http/tests/|tests/nw31j/|Cargo\.lock$|README\.md$)')
report change_set_in_scope "$([ -z "$stray" ]; echo $?)" "$(echo $stray)"

# 2. The oracle files: unchanged since the commit that added this gate.
first=$(git log --diff-filter=A --format=%H -- tests/nw31j/gate.sh | tail -n 1)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "tests/nw31j/gate.sh is not committed"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 3. adapters/crdt/src only gains lines; awset.rs is byte for byte fa082c4's.
removed=$(git diff -U0 "$BASE" -- adapters/crdt/src | grep -E '^-' | grep -Ev '^---' | head -n 3)
awset=$(changes adapters/crdt/src/awset.rs)
report crdt_additions_only "$([ -z "$removed" ] && [ -z "$awset" ]; echo $?)" "removed: $removed awset: $awset"

# 4. Cargo.lock is fa082c4's (no dependency is added: adapters/crdt/Cargo.toml is out of scope).
lock=$(changes Cargo.lock)
report lock_unchanged "$([ -z "$lock" ]; echo $?)" "$lock"

# 5. README changes only in its Network stack section, and the lines added
# there name the generated path (3c8c2bf) and the late-joiner limit.
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
for word, pattern in [("3c8c2bf", r"3c8c2bf"), ("late join", r"late[- ]join"), ("state transfer", r"state\s+transfer")]:
    if not re.search(pattern, added, re.I): why.append(f"the lines added to the Network stack section do not name {word}")
print(f"FAIL readme_network_section_states_the_generated_path_and_its_limit: {'; '.join(why)}" if why else "PASS readme_network_section_states_the_generated_path_and_its_limit")
sys.exit(bool(why))
PY

# 6. moirai-netstack-http's tests by name: NW.31g's 15 and this item's 3.
out=$(cargo_nodes test -j 2 --locked --no-fail-fast -p moirai-netstack-http -- --test-threads=1 2>&1); code=$?
missing=$(names_ok "$out" "${HTTP_TESTS[@]}")
report http_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 7. Regression: moirai-netstack-crdt's contract tests, as at fa082c4: nw31d_upstream
# 29 passed and nw31f_upstream_node 7 passed, nothing failing in the crate.
out=$(cargo_nodes test -j 2 --locked --no-fail-fast -p moirai-netstack-crdt -- --test-threads=1 2>&1); code=$?
result_of() { echo "$out" | awk -v t="Running tests/$1.rs" 'index($0, t) {f=1} f && /^test result:/ {print; exit}'; }
d=$(result_of nw31d_upstream); f=$(result_of nw31f_upstream_node)
report crdt_tests "$([ $code = 0 ] && [[ "$d" == "test result: ok. 29 passed; 0 failed"* ]] && [[ "$f" == "test result: ok. 7 passed; 0 failed"* ]]; echo $?)" "exit $code; nw31d: $d; nw31f: $f $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 8. The router binary, then the regression: moirai-netstack-start's 6 tests by name.
router="$CARGO_TARGET_DIR/debug/moirai-netstack-router"
rm -f "$router"
out=$(cargo_ build -j 2 --locked -p moirai-netstack-router --bin moirai-netstack-router 2>&1); code=$?
if [ $code != 0 ] || [ ! -x "$router" ]; then
  echo "SETUP: no router binary $router (build exit $code): $(echo "$out" | grep -E '^error' | head -n 2)"; exit 2
fi
out=$(NW31I_ROUTER_BIN="$router" NW31I_DIR="$SCRATCH/start-run" cargo_nodes test -j 2 --locked --no-fail-fast -p moirai-netstack-start -- --test-threads=1 2>&1); code=$?
missing=$(names_ok "$out" "${START_TESTS[@]}")
report start_tests "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 9. arachne-cli from a git archive of upstream Arachne 3c8c2bf, then the crate
# it generates from examples/json.ecore, its Moirai crates this lane's by path.
arch="$SCRATCH/arachne-$ARACHNE_COMMIT"; gen="$SCRATCH/gen"
rm -rf "$arch" "$gen" "$SCRATCH/gen-again"; mkdir -p "$arch" || exit 2
git -C "$ARACHNE_REPO" archive "$ARACHNE_COMMIT" | tar -x -C "$arch" || { echo "SETUP: git archive $ARACHNE_COMMIT"; exit 2; }
out=$(cd "$arch" && CARGO_TARGET_DIR="$ARACHNE_TARGET" cargo_ build -j 2 --locked -p arachne-cli 2>&1); code=$?
arachne="$ARACHNE_TARGET/debug/arachne"
[ $code = 0 ] && [ -x "$arachne" ] || { echo "SETUP: arachne-cli does not build (exit $code): $(echo "$out" | grep -E '^error' | head -n 2)"; exit 2; }
generate() { "$arachne" generate "$arch/examples/json.ecore" -o "$1" -p nw31j-json --moirai-path "$ROOT"; }
out=$(generate "$gen" 2>&1); code=$?
[ $code = 0 ] && [ -f "$gen/Cargo.toml" ] || { echo "SETUP: arachne generate exit $code: $(echo "$out" | tail -n 3)"; exit 2; }
grep -q '^serde = \[' "$gen/Cargo.toml" || { echo "SETUP: the generated crate has no serde feature"; exit 2; }

# 10. The consuming workspace: the generated crate as generated, the node main
# and the e2e driver, the lane's crates by path, the lane's lock as the start.
ws="$SCRATCH/node"; rm -rf "$ws"; mkdir -p "$ws/src" || exit 2
cp tests/nw31j/node.rs "$ws/src/node.rs" && cp tests/nw31j/e2e.rs "$ws/src/e2e.rs" && cp Cargo.lock "$ws/Cargo.lock" || exit 2
cat > "$ws/Cargo.toml" <<EOF
[package]
name = "nw31j-node"
version = "0.0.0"
edition = "2024"
publish = false

[[bin]]
name = "nw31j-node"
path = "src/node.rs"

[[bin]]
name = "nw31j-e2e"
path = "src/e2e.rs"

[dependencies]
nw31j-json = { path = "$gen", default-features = false, features = ["serde"] }
moirai-netstack-start = { path = "$ROOT/moirai-netstack-start" }
moirai-netstack-crdt = { path = "$ROOT/adapters/crdt" }
serde_json = "1.0"

[workspace]

[patch.crates-io]
zenoh = { path = "$ROOT/vendor/zenoh-1.10.1" }
EOF
node="$CARGO_TARGET_DIR/debug/nw31j-node"; e2e="$CARGO_TARGET_DIR/debug/nw31j-e2e"
rm -f "$node" "$e2e"
out=$(cd "$ws" && cargo_ build -j 2 2>&1); code=$?
built=$([ $code = 0 ] && [ -x "$node" ] && [ -x "$e2e" ]; echo $?)
report node_builds "$built" "exit $code: $(echo "$out" | grep -E '^error' -A 3 | head -n 8)"

# 11. End to end, under nodes.lock, 300 s at most.
if [ "$built" != 0 ]; then
  report e2e_three_generated_nodes_router_pause_resume 1 "not run: the node does not build"
else
  out=$(NW31J_ROUTER_BIN="$router" NW31J_NODE_BIN="$node" NW31J_DIR="$SCRATCH/run" flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" timeout 300 "$e2e" 2>&1); code=$?
  echo "$out" | grep -E '^NW31J' | grep -v 'E2E OK' | sed 's/^/  /'
  report e2e_three_generated_nodes_router_pause_resume "$([ $code = 0 ] && [ "$(echo "$out" | grep -E '^NW31J' | tail -n 1)" = "NW31J E2E OK" ]; echo $?)" "exit $code: $(echo "$out" | grep -E 'panicked|NW31J [a-z]+:|set-up:' | head -n 3)"
fi

# 12. The crate the node was built from is byte for byte what arachne-cli
# 3c8c2bf writes: generated again after the build and the run, and compared.
out=$(generate "$SCRATCH/gen-again" 2>&1 && diff -r "$gen" "$SCRATCH/gen-again" 2>&1); code=$?
report generated_crate_is_arachne_output "$code" "$(echo "$out" | head -n 3)"

# 13. Formatting: moirai-netstack-http whole (its oracle tests included) and
# adapters/crdt/src (the crate's tests were not rustfmt-clean at fa082c4 and are out of scope).
out=$(cargo fmt --check -p moirai-netstack-http 2>&1 && rustfmt --check --edition 2024 adapters/crdt/src/lib.rs 2>&1); code=$?
report fmt "$code" "$(echo "$out" | head -n 3)"

echo "NW31J_GATE fail=$fail"
exit $fail
