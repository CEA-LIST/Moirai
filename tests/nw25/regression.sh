#!/usr/bin/env bash
# NW.25, the regression (vault spec/items/NW.25.md, section 6). Runs the model store's workspace
# tests on the current default (TCP), the Zenoh items' suites NW.17 carried (without their two
# container steps), NW.17's own suites, and the json_crdt crate's tests, then compares a run with
# the baseline recorded at 9fbb6fd (moirai-model-plane) and 7dc783c (arachne-model-plane).
#
#   tests/nw25/regression.sh run <out-dir>
#   tests/nw25/regression.sh compare <base-out-dir> <run-out-dir>
#
# run: every cargo call under the shared flock, nice'd, two jobs; each step once, in order; no
# ZENOH_*, MOIRAI_TRANSPORT or MOIRAI_E2E_* variable may be set (the steps set what they need).
#   R1, TCP default (MOIRAI_TRANSPORT unset): model_node, moirai-relay, moirai-bootnode pinned from
#       HEAD; Arachne's network_node built from Arachne's HEAD beside this checkout; moirai-semantics,
#       moirai-protocol (serde), moirai-crdt, the unit tests of moirai-fuzz, moirai-macros,
#       moirai-bootnode, moirai-relay and moirai-dashboard; moirai-interp (network) lib and the 56
#       targets NW.17's P1 names, against the pins; moirai-network lib and its four TCP targets;
#       e2e_convergence under the process backend against network_node, one thread.
#   R2, the Zenoh items' suites and NW.17's (NW.17 section 4.2 and 4.3 without nw16b/container.sh
#       and nw16a/p7_container.sh): nw16b_router_config_l1 first (if red, no step that starts a
#       router runs); moirai-router's tests; NW.15's eight and the seven NW.16 moirai-network
#       targets; NW.16a, NW.16b, NW.16d, NW.16e moirai-interp targets (nw16b_isolated_l3, a
#       container target, excluded); nw17_large_message_l3; nw16a/p6.sh zenoh and tcp;
#       nw17/modelstore_l3.sh tcp and zenoh and nw17/compare.sh; every L3 target one thread.
#   R3, json_crdt: Arachne's HEAD extracted whole beside a link to this checkout, cargo test
#       --offline --no-fail-fast from generated/json_crdt (the layout's lock copy may be rewritten
#       by cargo, never the repository's; recorded).
# Writes <out>/steps.txt, <out>/summary.txt (STEP and TEST lines), <out>/heads.txt and every log.
#
# compare: a TEST ok in the base and FAILED or absent in the run, or a STEP with exit 0 in the base
# and not 0 in the run, is a new failure; exit 0 when there is none, 1 otherwise.
set -uo pipefail
export LC_ALL=C
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
AR="${NW25_ARACHNE_GIT:-$ROOT/../arachne-model-plane}"
SCR="${NW25_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.25}"
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
MODE="${1:?run|compare}"

summarize() { # out-dir: STEP lines from steps.txt, TEST lines from every log
    python3 -I - "$1" <<'PY'
import glob, os, re, sys
out = sys.argv[1]
lines = []
for l in open(os.path.join(out, "steps.txt")):
    part, name, code = l.split()
    lines.append(f"STEP {part}/{name} exit={code}")
run_re = re.compile(r"^\s+Running (unittests )?(\S+)(?: \((\S+)\))?")
doc_re = re.compile(r"^\s+Doc-tests (\S+)")
test_re = re.compile(r"^test (.+?) \.\.\. (ok|FAILED|ignored)\b")
def parse(step, path):
    target = "?"
    for l in open(path, errors="replace"):
        l = l.rstrip("\n")
        m = run_re.match(l)
        if m:
            src, binpath = m.group(2), m.group(3) or ""
            b = re.sub(r"-[0-9a-f]{16}$", "", os.path.basename(binpath))
            target = f"unittests:{b}" if m.group(1) else src
            continue
        m = doc_re.match(l)
        if m:
            target = f"doctests:{m.group(1)}"
            continue
        m = test_re.match(l)
        if m:
            lines.append(f"TEST {step}|{target}|{m.group(1)} {m.group(2)}")
for log in sorted(glob.glob(os.path.join(out, "*", "*.log"))):
    part = os.path.basename(os.path.dirname(log))
    parse(f"{part}/{os.path.basename(log)[:-4]}", log)
for log in sorted(glob.glob(os.path.join(out, "r2", "p6", "*", "*", "run.log")) + glob.glob(os.path.join(out, "r2", "ms", "*", "*", "run.log"))):
    suite, transport, kind = log.split(os.sep)[-2], log.split(os.sep)[-3], log.split(os.sep)[-4]
    parse(f"r2/{kind}-{transport}/{suite}", log)
with open(os.path.join(out, "summary.txt"), "w") as f:
    f.write("\n".join(lines) + "\n")
tests = [l for l in lines if l.startswith("TEST ")]
print(f"summary: {sum(1 for l in lines if l.startswith('STEP '))} steps "
      f"({sum(1 for l in lines if l.startswith('STEP ') and not l.endswith('exit=0'))} not exit 0), "
      f"{len(tests)} test lines: {sum(l.endswith(' ok') for l in tests)} ok, "
      f"{sum(l.endswith(' FAILED') for l in tests)} FAILED, {sum(l.endswith(' ignored') for l in tests)} ignored")
PY
}

if [ "$MODE" = compare ]; then
    B="${2:?base out}" R="${3:?run out}"
    python3 -I - "$B/summary.txt" "$R/summary.txt" <<'PY'
import sys
def load(p):
    steps, tests = {}, {}
    for l in open(p):
        l = l.rstrip("\n")
        if l.startswith("STEP "):
            name, code = l[5:].rsplit(" exit=", 1)
            steps[name] = code
        elif l.startswith("TEST "):
            ident, res = l[5:].rsplit(" ", 1)
            tests[ident] = res
    return steps, tests
bs, bt = load(sys.argv[1]); rs, rt = load(sys.argv[2])
new = []
for s, c in bs.items():
    if c == "0" and rs.get(s) != "0":
        new.append(f"NEW STEP {s}: exit 0 in the base, {rs.get(s, 'absent')} in the run")
for t, res in bt.items():
    if res == "ok" and rt.get(t) != "ok":
        new.append(f"NEW TEST {t}: ok in the base, {rt.get(t, 'absent')} in the run")
for t, res in rt.items():
    if t not in bt:
        print(f"info: test only in the run: {t} {res}")
print(f"base: {len(bs)} steps, {len(bt)} tests ({sum(r == 'ok' for r in bt.values())} ok); "
      f"run: {len(rs)} steps, {len(rt)} tests ({sum(r == 'ok' for r in rt.values())} ok)")
print("\n".join(new))
print(f"NW.25 REGRESSION COMPARE: {len(new)} new failures")
sys.exit(1 if new else 0)
PY
    exit $?
fi

[ "$MODE" = run ] || { echo "usage: regression.sh run <out> | compare <base> <run>"; exit 2; }
OUT="${2:?out dir}"
[ -e "$OUT" ] && { echo "NW.25 REGRESSION: REFUSED $OUT exists"; exit 2; }
for v in $(env | cut -d= -f1 | grep -E '^(ZENOH_|MOIRAI_TRANSPORT$|MOIRAI_E2E_|NW16A_|NW17_)'); do
    echo "NW.25 REGRESSION: REFUSED $v is set"; exit 2
done
mkdir -p "$OUT/r1" "$OUT/r2" "$OUT/r3" "$OUT/bin" "$SCR/tmp"
export TMPDIR="$SCR/tmp" CARGO_BUILD_JOBS=2
C=(flock "$LOCK" nice -n 10 cargo)
{
    echo "moirai $(git -C "$ROOT" rev-parse HEAD) $(git -C "$ROOT" branch --show-current) changed-tracked $(git -C "$ROOT" status --porcelain --untracked-files=no | wc -l)"
    echo "arachne $(git -C "$AR" rev-parse HEAD) $(git -C "$AR" branch --show-current) changed-tracked $(git -C "$AR" status --porcelain --untracked-files=no | wc -l)"
    rustc --version; echo "start $(date -Is)"
} | tee "$OUT/heads.txt"
: > "$OUT/steps.txt"
step() { # part name command...
    local part="$1" name="$2"; shift 2
    echo "=== $part/$name START $(date -Is) :: $*" >> "$OUT/driver.txt"
    (cd "$ROOT" && "$@") > "$OUT/$part/$name.log" 2>&1
    local rc=$?
    echo "$part $name $rc" >> "$OUT/steps.txt"
    echo "=== $part/$name END $(date -Is) exit $rc" | tee -a "$OUT/driver.txt"
    return $rc
}
skip() { echo "$1 $2 99" >> "$OUT/steps.txt"; echo "=== $1/$2 SKIPPED: $3" | tee -a "$OUT/driver.txt"; }

P1_TARGETS="ar02_declarations_served_l3 e2e_interp ed10_umlclass_u2_l3 ed16a_notation_diagrams_l3 ed20_join_without_metamodel_l3 nw02_loopback_ports nw03_joiner_writes_l3 nw04_joiner_with_history_l3 nw07_named_model_ids_l3 nw11_http_origin_policy_l2 si01_store_identity si01_store_identity_l3 si01_store_identity_races si01_store_identity_races_2 si01_store_identity_races_3 si05_references_by_identity si05_references_l3 si05_round3_be10455 si07_h1_harness_read si07_i1_intake si07_over_http si07_p1_dead_source si08_r2_nested_targets si08_target_side si08_target_side_l3 si12_copy_migration si12_copy_migration_l3 si12_fixtures si12_scale_l3 si13_concurrent_migrations si13_copy_in_progress si13_migration_l3 si13_scale_l3 si14_fixtures si14_rebase si14_rebase_l3 si16_rebased_joiner si16_rebased_joiner_l3 st01_optional_conflict_race st01_optional_conflict_race_l3 st02_emptied_optional_object st02_emptied_optional_object_l3 st04_emptied_optional_attribute st04_emptied_optional_attribute_l3 st05_recursive_keyed_class st05_recursive_keyed_class_l3 st05_recursive_keyed_clear_and_join st05_removed_key st05_removed_key_l3 st10_json_removed_key st11_fuzz_generator st12_fuzz_followups st13_deep_size st13_optional_null st14_m5_build st14_sequence_order_cache"
tflags() { local f=(); for t in "$@"; do f+=(--test "$t"); done; printf '%s\n' "${f[@]}"; }
mapfile -t P1F < <(tflags $P1_TARGETS)
mapfile -t NW15F < <(tflags nw15_contract_zenoh nw15_zenoh_transport nw15_nodes_zenoh nw15_zenoh_config nw15_choice_zenoh nw15_contract_direct nw15_nodes_tcp nw15_default_tcp)
mapfile -t NW16NF < <(tflags nw16a_presence_l2 nw16a_wait_before_close_l1 nw16a_paused_broadcast_l2 nw16b_router_pin_l0 nw16d_publisher_source_l0 nw16d_publisher_qos_l2 nw16e_region_config_l1)
TGT="$ROOT/target"
ART="$SCR/target-arachne"

# R1
export -n MOIRAI_TRANSPORT 2>/dev/null
step r1 build-node "${C[@]}" build -j 2 --locked -p moirai-interp --features network --example model_node && cp "$TGT/debug/examples/model_node" "$OUT/bin/model_node"
step r1 build-relay "${C[@]}" build -j 2 --locked -p moirai-relay -p moirai-bootnode && cp "$TGT/debug/moirai-relay" "$TGT/debug/moirai-bootnode" "$OUT/bin/"
mkdir -p "$OUT/ar/arachne-model-plane"
git -C "$AR" archive HEAD | tar -x -C "$OUT/ar/arachne-model-plane"
ln -s "$ROOT" "$OUT/ar/moirai-model-plane"
cp "$OUT/ar/arachne-model-plane/generated/json_crdt/Cargo.lock" "$OUT/r1/json_crdt.Cargo.lock.committed"
step r1 build-network-node env CARGO_TARGET_DIR="$ART" "${C[@]}" build -j 2 --offline --manifest-path "$OUT/ar/arachne-model-plane/generated/json_crdt/Cargo.toml" --example network_node \
    && cp "$ART/debug/examples/network_node" "$OUT/bin/network_node"
sha256sum "$OUT"/bin/* > "$OUT/bin/sha256.txt" 2>/dev/null
step r1 semantics "${C[@]}" test -j 2 -p moirai-semantics --no-fail-fast
step r1 protocol "${C[@]}" test -j 2 -p moirai-protocol --features serde --no-fail-fast
step r1 crdt "${C[@]}" test -j 2 -p moirai-crdt --no-fail-fast
step r1 other "${C[@]}" test -j 2 -p moirai-fuzz -p moirai-macros -p moirai-bootnode -p moirai-relay -p moirai-dashboard --no-fail-fast
step r1 interp env MOIRAI_E2E_INTERP_NODE_BIN="$OUT/bin/model_node" MOIRAI_E2E_RELAY_BIN="$OUT/bin/moirai-relay" MOIRAI_E2E_BOOTNODE_BIN="$OUT/bin/moirai-bootnode" \
    "${C[@]}" test -j 2 -p moirai-interp --features network --no-fail-fast --lib "${P1F[@]}"
step r1 network "${C[@]}" test -j 2 -p moirai-network --no-fail-fast --lib --test layering --test nw04_member_tables --test st11_workload_doc --test st12_e2e_comment
step r1 e2e-convergence env MOIRAI_E2E_BACKEND=process MOIRAI_E2E_NODE_BIN="$OUT/bin/network_node" MOIRAI_E2E_BOOTNODE_BIN="$OUT/bin/moirai-bootnode" \
    "${C[@]}" test -j 2 -p moirai-network --test e2e_convergence -- --test-threads=1

# R2
if step r2 router-config "${C[@]}" test -j 2 -p moirai-router --test nw16b_router_config_l1; then router_ok=1; else router_ok=0; fi
step r2 build-node "${C[@]}" build -j 2 --locked -p moirai-interp --features network --example model_node
step r2 build-router "${C[@]}" build -j 2 --locked -p moirai-router
if [ "$router_ok" = 1 ]; then step r2 router "${C[@]}" test -j 2 -p moirai-router --no-fail-fast
else skip r2 router "nw16b_router_config_l1 red"; fi
step r2 nw15 "${C[@]}" test -j 2 -p moirai-network --no-fail-fast "${NW15F[@]}" -- --test-threads=1
step r2 nw16-net "${C[@]}" test -j 2 -p moirai-network --no-fail-fast "${NW16NF[@]}" -- --test-threads=1
step r2 nw16a "${C[@]}" test -j 2 -p moirai-interp --features network --no-fail-fast --test nw16a_peer_mode_l3 --test nw16a_scouting_l3 --test nw16a_opcount_l3 --test nw16a_state_transfer_size_l3 --test nw16a_witness_probe -- --test-threads=1
for s in "nw16b:nw16b_router_l3 nw16b_observations_l3" "nw16d:nw16d_router_bytes_l3 nw16d_first_write_l2" "nw16e:nw16e_refusals_l2 nw16e_regions_l3"; do
    name="${s%%:*}"; mapfile -t F < <(tflags ${s#*:})
    if [ "$router_ok" = 1 ]; then step r2 "$name" "${C[@]}" test -j 2 -p moirai-interp --features network --no-fail-fast "${F[@]}" -- --test-threads=1
    else skip r2 "$name" "nw16b_router_config_l1 red"; fi
done
step r2 nw17-large "${C[@]}" test -j 2 -p moirai-interp --features network --no-fail-fast --test nw17_large_message_l3 -- --test-threads=1
step r2 p6-zenoh moirai-interp/tests/nw16a/p6.sh zenoh "$OUT/r2/p6"
step r2 p6-tcp moirai-interp/tests/nw16a/p6.sh tcp "$OUT/r2/p6"
step r2 ms-tcp moirai-interp/tests/nw17/modelstore_l3.sh tcp "$OUT/r2/ms"
step r2 ms-zenoh moirai-interp/tests/nw17/modelstore_l3.sh zenoh "$OUT/r2/ms"
step r2 ms-compare moirai-interp/tests/nw17/compare.sh "$OUT/r2/ms/tcp" "$OUT/r2/ms/zenoh"

# R3
mkdir -p "$OUT/r3/layout/arachne-model-plane"
git -C "$AR" archive HEAD | tar -x -C "$OUT/r3/layout/arachne-model-plane"
ln -s "$ROOT" "$OUT/r3/layout/moirai-model-plane"
JC="$OUT/r3/layout/arachne-model-plane/generated/json_crdt"
cp "$JC/Cargo.lock" "$OUT/r3/Cargo.lock.committed"
step r3 json-crdt bash -c "cd '$JC' && CARGO_TARGET_DIR='$ART' flock '$LOCK' nice -n 10 cargo test -j 2 --offline --no-fail-fast"
cmp -s "$OUT/r3/Cargo.lock.committed" "$JC/Cargo.lock" && w=no || w=yes
echo "r3 json_crdt lock rewritten by cargo: $w" | tee -a "$OUT/heads.txt"
cmp -s "$OUT/r1/json_crdt.Cargo.lock.committed" "$OUT/ar/arachne-model-plane/generated/json_crdt/Cargo.lock" && w=no || w=yes
echo "r1 json_crdt lock rewritten by cargo: $w" | tee -a "$OUT/heads.txt"
left="$(pgrep -a -f -- "$OUT/bin/|$TGT/debug/examples/model_node|$TGT/debug/moirai-router|$ART/debug/examples/network_node" | head -5)"
[ -z "$left" ] || echo "LEFTOVER processes: $left" | tee -a "$OUT/heads.txt"
echo "end $(date -Is)" | tee -a "$OUT/heads.txt"
summarize "$OUT" | tee -a "$OUT/heads.txt"
exit 0
