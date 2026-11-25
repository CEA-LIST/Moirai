#!/usr/bin/env bash
# NW.31k gate, round 2 (vault spec/items/NW.31k.md): state transfer.
# Upstream's protocol gains a snapshot and an adopt (serde behind the
# existing serde features); every log type moirai-macros emits and
# moirai-crdt's log types are serde and round-trip; the change to upstream's
# crates stands alone on upstream 7fe5ea2; through moirai-netstack-crdt's
# snapshot_for a fourth awset node that starts after three nodes' events are
# stable converges over the router and then exchanges new writes. Checks,
# each printed PASS or FAIL. Exit 0 only when all pass, 2 on a set-up error.
# Oracle fix (orchestrator, 2026-11-25): moirai-crdt/tests/nw31k_log_serde.rs bound
# `let s = String::from;` outside a closure taking &str (E0521, hidden behind
# the E0277 errors of the red proof); it is now a closure. No assertion changed.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=true CARGO_INCREMENTAL=0
# No debug info: the Zenoh build is large and the disk is short; nothing reads it.
export CARGO_PROFILE_DEV_DEBUG=0
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=1b5b876
UPSTREAM_BASE=7fe5ea2
LOCKS=${NW31K_LOCKS:-$HOME/.cache/cea-crdt-scratch}
export CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31k/target}
SCRATCH=${NW31K_SCRATCH:-$HOME/.cache/cea-crdt-scratch/NW.31k/gate}
UPSTREAM=(moirai-protocol moirai-crdt moirai-macros moirai-fuzz)
ORACLE=(tests/nw31k moirai-protocol/tests/nw31k_state_transfer.rs moirai-crdt/tests/nw31k_log_serde.rs adapters/crdt/tests/nw31d_upstream.rs adapters/crdt/tests/nw31f_upstream_node.rs)
IN_PROCESS=(nw31k_late_joiner_adopts_a_snapshot_vec_log nw31k_late_joiner_adopts_a_snapshot_graph_log)
LOG_SERDE=(nw31k_every_log_type_is_serde nw31k_a_record_of_a_nested_list_of_unions_and_references_round_trips nw31k_a_record_of_moirai_crdt_logs_round_trips)
# Upstream's own tests at 7fe5ea2 (spec, baseline): passed counts summed over
# the crate's test binaries and doc tests, nothing failing.
declare -A UP_BASE=(
  ["-p moirai-protocol"]=10
  ["-p moirai-protocol --features serde"]=12
  ["-p moirai-crdt"]=154
  ["-p moirai-crdt --features serde"]=155
  ["-p moirai-macros"]=0
  ["-p moirai-macros --features serde"]=0
)
# NW.31j's checks that still apply (the others are superseded, spec section Superseded).
NW31J_KEEP=(oracle_unchanged http_tests crdt_tests start_tests node_builds e2e_three_generated_nodes_router_pause_resume generated_crate_is_arachne_output fmt)
cargo_() { flock "$LOCKS/cargo.lock" nice -n 10 cargo "$@"; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
names_ok() { local out=$1; shift; local missing=""; for t in "$@"; do echo "$out" | grep -q "^test $t \.\.\. ok$" || missing+=" $t"; done; echo "$missing"; }
passed_sum() { echo "$1" | awk '/^test result:/ {for (i = 1; i <= NF; i++) if ($i == "passed;") s += $(i - 1)} END {print s + 0}'; }
failed_any() { echo "$1" | grep -E '^test result:' | grep -vqE ' 0 failed;'; }
cd "$ROOT" || exit 2
for c in "$BASE" "$UPSTREAM_BASE"; do git cat-file -e "$c^{commit}" || { echo "SETUP: $c is not in this repository"; exit 2; }; done
mkdir -p "$SCRATCH" "$CARGO_TARGET_DIR" || exit 2
changes() { { git diff --name-only "$BASE" -- "$@"; git status --porcelain --untracked-files=all -- "$@" | cut -c4-; } | sort -u; }

# 1. The change set since 1b5b876 stays in the item's paths; moirai-fuzz and
# every netstack crate but adapters/crdt are untouched.
stray=$(changes . | grep -Ev '^(moirai-protocol/|moirai-crdt/src/|moirai-crdt/Cargo\.toml$|moirai-macros/|moirai-crdt/tests/nw31k_log_serde\.rs$|adapters/crdt/src/|adapters/crdt/tests/nw31d_upstream\.rs$|adapters/crdt/tests/nw31f_upstream_node\.rs$|tests/nw31k/|Cargo\.lock$|README\.md$)')
awset=$(changes adapters/crdt/src/awset.rs tests/nw31j/node.rs)
report change_set_in_scope "$([ -z "$stray$awset" ]; echo $?)" "$(echo $stray $awset)"

# 2. The oracle files: unchanged since the round 2 oracle commit of this gate.
first=$(git log -n 1 --format=%H -E --grep='^NW\.31k oracle round 2' -- tests/nw31k/gate.sh)
if [ -z "$first" ]; then
  report oracle_unchanged 1 "no round 2 oracle commit of tests/nw31k/gate.sh"
else
  changed=$(git diff --name-only "$first" -- "${ORACLE[@]}"; git status --porcelain --untracked-files=all -- "${ORACLE[@]}")
  report oracle_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"
fi

# 3. No package enters or leaves Cargo.lock (an edge between known packages may).
pkgs() { awk '/^name = / {n = $3} /^version = / {print n, $3}'; }
lock_diff=$(diff <(git show "$BASE:Cargo.lock" | pkgs | sort) <(pkgs < Cargo.lock | sort))
report lock_packages_unchanged "$([ -z "$lock_diff" ]; echo $?)" "$(echo $lock_diff | head -c 300)"

# 4. Every commit since 1b5b876 that touches an upstream crate touches that
# one crate only (Cargo.lock aside), so each can be offered upstream alone.
mixed=""
for c in $(git rev-list "$BASE..HEAD"); do
  files=$(git diff-tree --no-commit-id --name-only -r "$c" | grep -v '^Cargo\.lock$')
  crates=$(echo "$files" | cut -d/ -f1 | sort -u)
  if echo "$crates" | grep -qxE 'moirai-(protocol|crdt|macros|fuzz)' && [ "$(echo "$crates" | wc -l)" != 1 ]; then
    mixed+=" $(git rev-parse --short "$c"):$(echo $crates | tr ' ' ,)"
  fi
done
report upstream_commits_stand_alone "$([ -z "$mixed" ]; echo $?)" "$mixed"

# 5. Part (1) in this workspace: the in-process transfer test, by name.
out=$(cargo_ test -j 2 --locked -p moirai-protocol --features serde,test_utils --test nw31k_state_transfer 2>&1); code=$?
missing=$(names_ok "$out" "${IN_PROCESS[@]}")
report in_process_transfer "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 5b. Part (2) in this workspace, as a node builds moirai-crdt (no default
# features, serde; test_utils for Replica::into_state): the log round trips, by name.
out=$(cargo_ test -j 2 --locked -p moirai-crdt --no-default-features --features serde,test_utils --test nw31k_log_serde 2>&1); code=$?
missing=$(names_ok "$out" "${LOG_SERDE[@]}")
report log_serde_round_trip "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 6. Part (3): the diff since 7fe5ea2 restricted to upstream's crates, applied
# to a git archive of 7fe5ea2 (no netstack crate there): upstream's own tests
# of moirai-protocol and moirai-crdt with default features and with serde,
# moirai-macros with default features and with serde, then parts (1) and (2)
# by name, each by its own command.
up="$SCRATCH/upstream-$UPSTREAM_BASE"
rm -rf "$up"; mkdir -p "$up" || exit 2
git archive "$UPSTREAM_BASE" | tar -x -C "$up" || { echo "SETUP: git archive $UPSTREAM_BASE"; exit 2; }
git diff --binary "$UPSTREAM_BASE" HEAD -- "${UPSTREAM[@]}" > "$SCRATCH/upstream.patch" || exit 2
if [ ! -s "$SCRATCH/upstream.patch" ]; then
  report upstream_diff_applies 1 "no change to upstream's crates since $UPSTREAM_BASE"
else
  out=$(git -C "$up" apply --binary "$SCRATCH/upstream.patch" 2>&1); code=$?
  report upstream_diff_applies "$code" "$out"
fi
for args in "-p moirai-protocol" "-p moirai-protocol --features serde" "-p moirai-crdt" "-p moirai-crdt --features serde" "-p moirai-macros" "-p moirai-macros --features serde"; do
  out=$(cd "$up" && cargo_ test -j 2 --no-fail-fast $args 2>&1); code=$?
  n=$(passed_sum "$out")
  report "upstream_alone_tests[$args]" "$([ $code = 0 ] && ! failed_any "$out" && [ "$n" -ge "${UP_BASE[$args]}" ]; echo $?)" "exit $code, $n passed (7fe5ea2: ${UP_BASE[$args]}) $(echo "$out" | grep -E '^error|panicked|FAILED' | head -n 3)"
done
out=$(cd "$up" && cargo_ test -j 2 -p moirai-protocol --features serde,test_utils --test nw31k_state_transfer 2>&1); code=$?
missing=$(names_ok "$out" "${IN_PROCESS[@]}")
report upstream_alone_in_process_transfer "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"
out=$(cd "$up" && cargo_ test -j 2 -p moirai-crdt --features serde --test nw31k_log_serde 2>&1); code=$?
missing=$(names_ok "$out" "${LOG_SERDE[@]}")
report upstream_alone_log_serde "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing $(echo "$out" | grep -E '^error|panicked' | head -n 3)"

# 7. NW.31j's gate, its checks that still apply by name. It builds the router,
# arachne-cli 3c8c2bf, the generated crate (this lane's upstream crates by
# path, serde on) and the node from tests/nw31j/node.rs, unchanged.
j="$SCRATCH/nw31j"
out=$(NW31J_SCRATCH="$j" NW31J_LOCKS="$LOCKS" NW31J_ARACHNE_TARGET="$SCRATCH/../target-arachne" bash tests/nw31j/gate.sh 2>&1); code=$?
echo "$out" | grep -E '^(PASS|FAIL|SETUP)' | sed 's/^/  nw31j: /'
[ $code = 2 ] && { echo "SETUP: tests/nw31j/gate.sh set-up failed"; exit 2; }
for check in "${NW31J_KEEP[@]}"; do
  echo "$out" | grep -qx "PASS $check"
  report "nw31j_$check" $? "$(echo "$out" | grep -E "^FAIL $check" | head -c 300)"
done

# 8. Part (4), end to end, under nodes.lock, 400 s at most: the router
# NW.31j's gate built, and awset nodes from tests/nw31k/node.rs built with the
# driver in a consuming workspace (the lane's crates by path, its lock).
router="$CARGO_TARGET_DIR/debug/moirai-netstack-router"
ws="$SCRATCH/e2e"; rm -rf "$ws"; mkdir -p "$ws/src" || exit 2
cp tests/nw31k/e2e.rs "$ws/src/e2e.rs" && cp tests/nw31k/node.rs "$ws/src/node.rs" && cp Cargo.lock "$ws/Cargo.lock" || exit 2
cat > "$ws/Cargo.toml" <<EOF
[package]
name = "nw31k-e2e"
version = "0.0.0"
edition = "2024"
publish = false

[[bin]]
name = "nw31k-node"
path = "src/node.rs"

[[bin]]
name = "nw31k-e2e"
path = "src/e2e.rs"

[dependencies]
moirai-netstack-start = { path = "$ROOT/moirai-netstack-start" }
moirai-netstack-crdt = { path = "$ROOT/adapters/crdt" }
serde_json = "1.0"

[workspace]

[patch.crates-io]
zenoh = { path = "$ROOT/vendor/zenoh-1.10.1" }
EOF
node="$CARGO_TARGET_DIR/debug/nw31k-node"; e2e="$CARGO_TARGET_DIR/debug/nw31k-e2e"; rm -f "$node" "$e2e"
out=$(cd "$ws" && cargo_ build -j 2 2>&1); code=$?
if [ $code != 0 ] || [ ! -x "$e2e" ] || [ ! -x "$node" ]; then
  report e2e_late_awset_node_joins_by_state_transfer 1 "not run: the awset node or the driver does not build (exit $code): $(echo "$out" | grep -E '^error' -A 3 | head -n 6)"
elif [ ! -x "$router" ]; then
  report e2e_late_awset_node_joins_by_state_transfer 1 "not run: no router binary from NW.31j's gate"
else
  out=$(NW31K_ROUTER_BIN="$router" NW31K_NODE_BIN="$node" NW31K_DIR="$SCRATCH/run" flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" timeout 400 "$e2e" 2>&1); code=$?
  echo "$out" | grep -E '^NW31K' | grep -v 'E2E OK' | sed 's/^/  /'
  report e2e_late_awset_node_joins_by_state_transfer "$([ $code = 0 ] && [ "$(echo "$out" | grep -E '^NW31K' | tail -n 1)" = "NW31K E2E OK" ]; echo $?)" "exit $code: $(echo "$out" | grep -E 'panicked|NW31K [a-z]+:|set-up:' | head -n 3)"
fi

# 9. README changes only in its Network stack section; the late-joiner limit
# sentence is gone and the lines added there name state transfer and the snapshot.
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
section = "\n".join(ns.get(NET, []))
if re.search(r"upstream has no state transfer|late joiner is not covered", section, re.I): why.append("the section still states the late-joiner limit")
added = "\n".join(l for l in ns.get(NET, []) if l not in os_.get(NET, []))
for word in ["state transfer", "snapshot"]:
    if not re.search(word, added, re.I): why.append(f"the lines added to the Network stack section do not name {word}")
print(f"FAIL readme_network_section_states_state_transfer: {'; '.join(why)}" if why else "PASS readme_network_section_states_state_transfer")
sys.exit(bool(why))
PY

# 10. Formatting at the defaults (upstream's crates are not rustfmt-clean at
# 7fe5ea2): every Rust file changed since 1b5b876 in the item's crates that
# is new, or was clean at 1b5b876, is clean now (rustfmt on stdin, file by file).
clean() { rustfmt --edition 2024 2>/dev/null | cmp -s - "$1"; }
unclean=""
for f in $(changes moirai-protocol moirai-crdt/src moirai-crdt/tests moirai-macros adapters/crdt tests/nw31k | grep '\.rs$'); do
  [ -f "$f" ] || continue
  if ! git cat-file -e "$BASE:$f" 2>/dev/null || git show "$BASE:$f" | clean <(git show "$BASE:$f"); then
    clean "$f" < "$f" || unclean+=" $f"
  fi
done
report fmt "$([ -z "$unclean" ]; echo $?)" "$unclean"

echo "NW31K_GATE fail=$fail"
exit $fail
