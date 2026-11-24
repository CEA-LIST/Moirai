#!/usr/bin/env bash
# NW.31b gate: the transport crate moirai-netstack-transport on akira/moirai-network-s2
# (base b57ffb2 = upstream Moirai 7fe5ea2 plus the core crate moirai-netstack).
# Each check prints PASS or FAIL; exit 0 only when all pass, 2 on a set-up error.
# Cargo runs under the shared cargo lock; tests that open sockets or Zenoh
# sessions also hold the nodes lock, taken inside the cargo lock.
set -uo pipefail
export LC_ALL=C
export CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-true}
export CARGO_INCREMENTAL=${CARGO_INCREMENTAL:-0}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BASE=b57ffb2
UPSTREAM_BASE=7fe5ea2
CRATE=moirai-netstack-transport
UPSTREAM=(moirai-crdt moirai-protocol moirai-macros moirai-fuzz)
VENDOR_TREE_601C887=88cf8327c91b52d3ea416243d5105d5de741ae3e
LOCKS=${NW31B_LOCKS:-$HOME/.cache/cea-crdt-scratch}
SCRATCH=${NW31B_SCRATCH:-$HOME/.cache/cea-crdt-scratch/NW.31b/gate}
cargo_() { flock "$LOCKS/cargo.lock" nice -n 10 cargo "$@"; }
cargo_nodes() { flock "$LOCKS/cargo.lock" flock "$LOCKS/nodes.lock" nice -n 10 cargo "$@"; }
fail=0
report() { if [ "$2" = 0 ]; then echo "PASS $1"; else echo "FAIL $1${3:+: $3}"; fail=1; fi; }
cd "$ROOT" || exit 2
git cat-file -e "$BASE^{commit}" || { echo "SETUP: $BASE is not in this repository"; exit 2; }

# 1. Upstream's four crates: no change since 7fe5ea2, committed or not.
changed=$(git diff --name-only "$UPSTREAM_BASE" -- "${UPSTREAM[@]}"; git status --porcelain --untracked-files=all -- "${UPSTREAM[@]}")
report upstream_crates_unchanged "$([ -z "$changed" ]; echo $?)" "$(echo $changed)"

# 2. Every path changed since b57ffb2 (committed, staged, unstaged or untracked) is in scope.
outside=$( { git diff --name-only "$BASE"; git ls-files --others --exclude-standard; } | sort -u \
  | grep -vE '^(moirai-netstack-transport/|vendor/zenoh-1\.10\.1/|moirai-netstack/src/|moirai-netstack/tests/|tests/nw31b/|Cargo\.toml$|Cargo\.lock$)')
report paths_in_scope "$([ -z "$outside" ]; echo $?)" "$(echo $outside)"

# 3 and 4. Root manifest: b57ffb2's plus the member, the zenoh patch line and at most
# exclude = ["vendor"]; the lock keeps every b57ffb2 entry.
python3 -I - "$BASE" "$CRATE" <<'PY'
import subprocess, sys, tomllib
base, crate = sys.argv[1], sys.argv[2]
def old(path): return tomllib.loads(subprocess.run(["git", "show", f"{base}:{path}"], check=True, capture_output=True, text=True).stdout)
def new(path):
    with open(path, "rb") as f: return tomllib.load(f)
om, nm = old("Cargo.toml"), new("Cargo.toml")
why = []
ws = nm.get("workspace", {})
if sorted(ws.get("members", [])) != sorted(om["workspace"]["members"] + [crate]): why.append(f"members {ws.get('members')}")
if ws.get("exclude", ["vendor"]) != ["vendor"]: why.append(f"exclude {ws.get('exclude')}")
if nm.get("patch") != {"crates-io": {"zenoh": {"path": "vendor/zenoh-1.10.1"}}}: why.append(f"patch {nm.get('patch')}")
rest_old = {k: v for k, v in om.items() if k != "patch"}
rest_new = {k: v for k, v in nm.items() if k != "patch"}
rest_old["workspace"] = {k: v for k, v in om["workspace"].items() if k not in ("members", "exclude")}
rest_new["workspace"] = {k: v for k, v in ws.items() if k not in ("members", "exclude")}
if rest_old != rest_new: why.append("a key besides members, exclude and the zenoh patch changed")
print(("FAIL root_manifest_member_and_patch_only: " + "; ".join(why)) if why else "PASS root_manifest_member_and_patch_only")
ol, nl = old("Cargo.lock"), new("Cargo.lock")
now = {(p["name"], p["version"]): p for p in nl["package"]}
lost = []
for p in ol["package"]:
    q = now.get((p["name"], p["version"]))
    if q is None: lost.append(f"{p['name']} {p['version']} gone"); continue
    rest = lambda e: {k: v for k, v in e.items() if k != "dependencies"}
    if p["name"].startswith("moirai") and p["name"] != "moirai-netstack" and p != q: lost.append(f"{p['name']} changed")
    elif rest(p) != rest(q) or not set(p.get("dependencies", [])) <= set(q.get("dependencies", [])): lost.append(f"{p['name']} {p['version']} changed")
ok2 = not lost and ol.get("version") == nl.get("version")
print(("PASS" if ok2 else "FAIL") + " lock_keeps_every_b57ffb2_entry" + ("" if ok2 else f": {lost}"))
sys.exit(0 if not why and ok2 else 1)
PY
[ $? = 0 ] || fail=1

# 5. vendor/zenoh-1.10.1 is 601c887's copy, committed whole and unchanged.
tree=$(git rev-parse -q --verify "HEAD:vendor/zenoh-1.10.1" 2>/dev/null)
dirty=$(git status --porcelain --untracked-files=all -- vendor)
report vendor_is_601c887s_patched_zenoh "$([ "$tree" = "$VENDOR_TREE_601C887" ] && [ -z "$dirty" ]; echo $?)" "HEAD tree ${tree:-none}, want $VENDOR_TREE_601C887; uncommitted: $(echo $dirty)"

# 6. README: the patch line a consuming workspace repeats, and stacker named.
readme="$CRATE/README.md"
missing=""
for word in '[patch.crates-io]' 'vendor/zenoh-1.10.1' 'stacker'; do grep -qF -- "$word" "$readme" 2>/dev/null || missing+=" $word"; done
report readme_names_the_patch_line_and_stacker "$([ -z "$missing" ]; echo $?)" "$readme lacks:$missing"

# 7. Dependency closure of the crate, all features, normal, dev and build: no moirai* but the core.
meta=$(cargo_ metadata --format-version 1 --locked --all-features 2>/dev/null)
echo "$meta" | python3 -I -c '
import json, sys
crate = sys.argv[1]
try:
    m = json.loads(sys.stdin.read())
except ValueError:
    print("FAIL no_moirai_dependency_but_the_core: cargo metadata --locked failed"); sys.exit(1)
names = {p["id"]: p["name"] for p in m["packages"]}
nodes = {n["id"]: n for n in m["resolve"]["nodes"]}
roots = [i for i, n in names.items() if n == crate and i in m["workspace_members"]]
if not roots:
    print(f"FAIL no_moirai_dependency_but_the_core: {crate} is not a workspace member"); sys.exit(1)
seen, todo = set(), list(roots)
while todo:
    i = todo.pop()
    if i in seen: continue
    seen.add(i)
    todo += [d["pkg"] for d in nodes[i]["deps"]]
bad = sorted({names[i] for i in seen if names[i].startswith("moirai") and names[i] not in (crate, "moirai-netstack")})
print(("FAIL no_moirai_dependency_but_the_core: " + ", ".join(bad)) if bad else "PASS no_moirai_dependency_but_the_core")
sys.exit(1 if bad else 0)
' "$CRATE" || fail=1

# 8. The crate's oracle tests by name, default features (zenoh on), then without features.
check_target() { # build-flags... -- target names...
  local flags=() t names=() out code missing=""
  while [ "$1" != "--" ]; do flags+=("$1"); shift; done; shift; t=$1; shift; names=("$@")
  out=$(cargo_nodes test -p "$CRATE" --locked -j 2 "${flags[@]}" --test "$t" -- --test-threads=1 2>&1); code=$?
  for n in "${names[@]}"; do echo "$out" | grep -q "^test $n \.\.\. ok$" || missing+=" $n"; done
  echo "$out" | grep -E '^error|^test result|FAILED|panicked' | head -n 20
  report "tests_${flags[*]:+lite_}$t" "$([ $code = 0 ] && [ -z "$missing" ]; echo $?)" "exit $code, not ok:$missing"
}
Z1=(z1_send_reaches_the_named_peer_once z1_broadcast_reaches_every_peer_once_and_not_its_sender z1_one_sided_pause_holds_both_directions_at_the_pauser z1_pause_all_and_resume_all_keep_the_buffers z1_unknown_peers_are_peer_not_found z1_a_receiver_that_does_not_read_loses_nothing z4_a_128_mib_message_arrives_whole)
N27_TCP=(nw27_g1_tcp_deep nw27_g1_relay_deep nw27_g1_reassembled_deep nw27_g1_exact_limit_tcp nw27_g1_tcp_refusal_1032 nw27_g1_tcp_refusal_2048 nw27_g1_relay_refusal_1032 nw27_g1_relay_refusal_2048 nw27_g1_chunks_refusal_1032 nw27_g1_chunks_refusal_2048 nw27_g5_ordinary_bytes_and_shallow_boundaries)
N27_ZENOH=(nw27_g1_zenoh_deep nw27_g1_exact_limit_zenoh nw27_g1_zenoh_refusal_1032 nw27_g1_zenoh_refusal_2048)
N19=(nw19_l1_default_transport_is_zenoh nw19_l1_unset_reads_zenoh_and_an_explicit_choice_is_kept nw19_l1_a_bootnode_with_the_transport_unset_is_refused_naming_tcp)
N19_LITE=(lite::nw19_lite_unset_refuses_naming_the_feature_and_tcp lite::nw19_lite_explicit_zenoh_refuses_naming_the_feature lite::nw19_lite_explicit_tcp_builds_tcp)
SHAPE=(nw31b_every_transport_is_a_crdt_transport_with_no_op_type nw31b_the_frame_is_the_core_crates_transport_message)
N16D_SRC=(the_reader_finds_a_call_across_lines_and_ignores_comments_and_literals the_zenoh_transport_declares_publishers_and_calls_no_session_put)
mkdir -p "$SCRATCH"
if cargo_ test -p "$CRATE" --locked -j 2 --no-run >"$SCRATCH/build.log" 2>&1 \
  && cargo_ test -p "$CRATE" --locked -j 2 --no-default-features --no-run >>"$SCRATCH/build.log" 2>&1; then
  check_target -- nw15_contract_direct "${Z1[@]}"
  check_target -- nw15_contract_zenoh "${Z1[@]}"
  check_target -- nw16a_paused_broadcast_l2 c2_a_paused_broadcast_reports_a_failed_put_as_send_does
  check_target -- nw16a_presence_l2 c3_a_token_appearing_for_a_paused_peer_is_kept_in_its_buffer p4_a_same_id_successor_that_appears_while_the_old_session_lives_is_greeted
  check_target -- nw16a_wait_before_close_l1 c1_every_configuration_moirai_builds_sets_wait_before_close
  check_target -- nw16b_zenoh_pin_l0 the_transport_crate_is_on_the_pinned_zenoh
  check_target -- nw16d_publisher_source_l0 "${N16D_SRC[@]}"
  check_target -- nw16d_publisher_qos_l2 every_put_reads_block_data_and_not_express_on_the_wire
  check_target -- nw16e_region_config_l1 na_zenoh_region_beside_a_router_in_peer_mode_is_the_region_name_and_changes_nothing_else nb_without_zenoh_region_the_region_name_stays_unset nc_peers_zenoh_region_is_refused_beside_peers nc_client_zenoh_region_is_refused_beside_client_mode nc_config_zenoh_region_is_refused_beside_zenoh_config nc_router_zenoh_region_is_refused_without_a_router nc_empty_zenoh_region_is_refused_when_set_but_empty nc_long_zenoh_region_is_refused_over_32_bytes
  check_target -- nw19_default_l1 "${N19[@]}"
  check_target -- nw27_decode "${N27_TCP[@]}" "${N27_ZENOH[@]}"
  check_target -- nw31b_shape "${SHAPE[@]}"
  check_target -- nw31b_zenoh_patch nw31b_the_linked_zenoh_is_the_vendored_patched_copy
  check_target --no-default-features -- nw15_contract_direct "${Z1[@]}"
  check_target --no-default-features -- nw19_default_l1 "${N19[@]}" "${N19_LITE[@]}"
  check_target --no-default-features -- nw27_decode "${N27_TCP[@]}"
  check_target --no-default-features -- nw31b_shape "${SHAPE[@]}"
else
  grep -E '^error' "$SCRATCH/build.log" | head -n 20
  report crate_tests_build 1 "cargo test -p $CRATE --no-run failed, with or without features (log $SCRATCH/build.log)"
fi

# 9. A consuming workspace: without the patch line the check refuses stock zenoh loudly,
# naming the line; with the README's line it answers Ok.
consumer() { # dir patched
  local dir=$1
  rm -rf "$dir"; mkdir -p "$dir/src"
  cp Cargo.lock "$dir/Cargo.lock"
  { printf '[package]\nname = "nw31b-consumer"\nversion = "0.0.0"\nedition = "2021"\npublish = false\n\n[dependencies]\nmoirai-netstack-transport = { path = "%s/%s" }\n\n[workspace]\n' "$ROOT" "$CRATE"
    [ "$2" = patched ] && printf '\n[patch.crates-io]\nzenoh = { path = "%s/vendor/zenoh-1.10.1" }\n' "$ROOT"; } > "$dir/Cargo.toml"
  cat > "$dir/src/main.rs" <<'RS'
fn main() {
    match moirai_netstack_transport::zenoh_patch::check() {
        Ok(()) => println!("NW31B PATCHED"),
        Err(why) => {
            eprintln!("{why}");
            std::process::exit(3);
        }
    }
}
RS
}
zenoh_source() { # dir -> the source of the zenoh package the consumer resolves ("path" or the registry)
  (cd "$1" && cargo_ metadata --format-version 1 --offline 2>/dev/null) | python3 -I -c '
import json, sys
m = json.loads(sys.stdin.read())
print(next((p["source"] or "path") for p in m["packages"] if p["name"] == "zenoh"))' 2>/dev/null
}
if [ ! -f "$CRATE/Cargo.toml" ]; then
  report stock_zenoh_is_refused_naming_the_patch_line 1 "no $CRATE/Cargo.toml"
  report patched_zenoh_is_accepted 1 "no $CRATE/Cargo.toml"
else
CT=${CARGO_TARGET_DIR:-$SCRATCH/target}
consumer "$SCRATCH/stock" stock
consumer "$SCRATCH/patched" patched
src_stock=$(zenoh_source "$SCRATCH/stock"); src_patched=$(zenoh_source "$SCRATCH/patched")
if [[ "$src_stock" != registry+* ]] || [ "$src_patched" != path ]; then
  echo "SETUP: the consumers resolve zenoh from '$src_stock' (want the registry) and '$src_patched' (want a path)"; exit 2
fi
out=$(cd "$SCRATCH/stock" && CARGO_TARGET_DIR="$CT" cargo_ run --offline -q -j 2 2>&1); code=$?
loud=$([ $code != 0 ] && [ $code != 101 ] && echo "$out" | grep -qF '[patch.crates-io]' && echo "$out" | grep -qi zenoh; echo $?)
report stock_zenoh_is_refused_naming_the_patch_line "$loud" "exit $code: $(echo "$out" | tail -n 3)"
out=$(cd "$SCRATCH/patched" && CARGO_TARGET_DIR="$CT" cargo_ run --offline -q -j 2 2>&1); code=$?
report patched_zenoh_is_accepted "$([ $code = 0 ] && echo "$out" | grep -q '^NW31B PATCHED$'; echo $?)" "exit $code: $(echo "$out" | tail -n 3)"
fi

# 10. Regression: the rest of the workspace suite, no failure (b57ffb2: see the spec's
# baseline). The crate's own tests ran one at a time in section 8.
out=$(cargo_nodes test --workspace --exclude "$CRATE" --locked -j 2 --no-fail-fast 2>&1); code=$?
echo "$out" | grep -E '^error|FAILED|panicked' | head -n 20
p=$(echo "$out" | grep -E '^test result' | sed -E 's/.* ([0-9]+) passed; ([0-9]+) failed.*/\1 \2/' | awk '{p+=$1; f+=$2} END {print p+0, f+0}')
report workspace_tests "$([ $code = 0 ]; echo $?)" "exit $code, passed and failed: $p"

[ $fail = 0 ] && echo "NW31B GATE: PASS" || echo "NW31B GATE: FAIL"
exit $fail
