#!/usr/bin/env bash
# NW.19, P2, P3 and P6 (vault spec/items/NW.19.md, sections 1 and 6): the model store's regression
# on the default, with every node a run starts seen and witnessed from outside, compared with the
# baseline recorded at the TCP-default parent Pp. NW.25's regression driver (tests/nw25/regression.sh
# at 69753ce), copied, with these changes and no other:
#   - tests/nw19/watch.py runs beside the whole run and records every model_node and network_node
#     process it starts, whatever started it, with the step it ran in; NW19_WATCH=probe (the
#     default) witnesses every unpinned node by a raw Zenoh session at its reported port,
#     NW19_WATCH=record only records (baseline A at Pp, where a Zenoh probe would dial TCP
#     listeners and stall their HTTP for about 2.5 s, NW.20's R0.13);
#   - `rehearse` (baseline Z) exports MOIRAI_TRANSPORT=zenoh for the whole run, so every process
#     that inherits the environment runs Zenoh unless its test pins TCP, and probes only the nodes
#     that say zenoh; NW.17's modelstore_l3.sh, which refuses an exported variable and sets its
#     own per arm, runs with it removed in both modes;
#   - every process of the run gets tests/nw25/btdump.c preloaded and armed (inert unless the
#     watcher sends SIGUSR2 to a node that reported and stays silent); NW19_BTDUMP=0 turns it off;
#   - the moirai-interp node targets run with one test thread;
#   - P3 is the e2e_convergence step under the process backend (without --nocapture: a scenario's
#     route line would break libtest's `test ... ok` line, and the scenario would drop out of the
#     comparison; the watcher is P3's witness), and opcount_double_delivery is run once more with
#     --ignored, recorded and never compared (step p3/e2e-opcount);
#   - P6 is NW.25's R3, json_crdt's tests in a git-archive layout beside this tree;
#   - steps are named by claim point, and the run never builds, starts or reads a container.
#
#   tests/nw19/regression.sh run <out-dir>          P2, P3, P6 at H; baseline A at Pp (NW19_WATCH=record)
#   tests/nw19/regression.sh rehearse <out-dir>     baseline Z at Pp
#   tests/nw19/regression.sh compare <base-out> <run-out>
#   then tests/nw19/account.py judges <out-dir>/watch/nodes.jsonl against tests/nw19/pins.tsv.
#
# compare: a TEST ok in the base and FAILED or absent in the run, or a STEP with exit 0 in the base
# and not 0 in the run, is a new failure; p3/e2e-opcount is left out; exit 0 when there is none.
# Knobs: NW19_ARACHNE (the Arachne repository), NW19_ARACHNE_REV (default bd7b647), NW19_SCRATCH,
# NW19_WATCH, NW19_BTDUMP, NW19_STEPS (a subset of the parts r1 r2 p3 p6, for a control arm).
set -uo pipefail
export LC_ALL=C
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
AR="${NW19_ARACHNE:-$ROOT/../arachne-model-plane}"
AR_REV="${NW19_ARACHNE_REV:-bd7b647}"
SCR="${NW19_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.19}"
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
MODE="${1:?run|rehearse|compare}"

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
OBSERVED = "p3/e2e-opcount"   # recorded, never compared (NW.19 spec, P3)
def load(p):
    steps, tests = {}, {}
    for l in open(p):
        l = l.rstrip("\n")
        if l.startswith("STEP "):
            name, code = l[5:].rsplit(" exit=", 1)
            if name != OBSERVED:
                steps[name] = code
        elif l.startswith("TEST "):
            ident, res = l[5:].rsplit(" ", 1)
            if not ident.startswith(OBSERVED + "|"):
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
print(f"NW.19 REGRESSION COMPARE: {len(new)} new failures")
sys.exit(1 if new else 0)
PY
    exit $?
fi

case "$MODE" in run|rehearse) ;; *) echo "usage: regression.sh run|rehearse <out> | compare <base> <run>"; exit 2 ;; esac
OUT="${2:?out dir}"
[ -e "$OUT" ] && { echo "NW.19 REGRESSION: REFUSED $OUT exists"; exit 2; }
for v in $(env | cut -d= -f1 | grep -E '^(ZENOH_|MOIRAI_TRANSPORT$|MOIRAI_E2E_|NW16A_|NW17_|ROUTER_REGIONS$)'); do
    echo "NW.19 REGRESSION: REFUSED $v is set"; exit 2
done
WATCH_MODE="${NW19_WATCH:-probe}"
PROBE_INTENDED=(zenoh default)
if [ "$MODE" = rehearse ]; then export MOIRAI_TRANSPORT=zenoh; PROBE_INTENDED=(zenoh); fi
mkdir -p "$OUT/b" "$OUT/r1" "$OUT/r2" "$OUT/p3" "$OUT/p6" "$OUT/bin" "$OUT/watch" "$SCR/tmp"
export TMPDIR="$SCR/tmp" CARGO_BUILD_JOBS=2
C=(flock "$LOCK" nice -n 10 cargo)
PARTS=" ${NW19_STEPS:-r1 r2 p3 p6} "
want() { case "$PARTS" in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
{
    echo "mode $MODE watch $WATCH_MODE MOIRAI_TRANSPORT ${MOIRAI_TRANSPORT:-unset}"
    echo "moirai $(git -C "$ROOT" rev-parse HEAD) $(git -C "$ROOT" branch --show-current) changed $(git -C "$ROOT" status --porcelain | wc -l)"
    echo "arachne $(git -C "$AR" rev-parse "$AR_REV") (archive of $AR_REV)"
    rustc --version; echo "start $(date -Is)"
} | tee "$OUT/heads.txt"
: > "$OUT/steps.txt"
step() { # part name command...
    local part="$1" name="$2"; shift 2
    printf '%s\n' "$part/$name" > "$OUT/watch/step"
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

# ------------------------------------------------------------------ builds, the probe, the watcher
step b build-node "${C[@]}" build -j 2 --locked -p moirai-interp --features network --example model_node && cp "$TGT/debug/examples/model_node" "$OUT/bin/model_node"
step b build-relay "${C[@]}" build -j 2 --locked -p moirai-relay -p moirai-bootnode && cp "$TGT/debug/moirai-relay" "$TGT/debug/moirai-bootnode" "$OUT/bin/"
mkdir -p "$OUT/ar/arachne-model-plane"
git -C "$AR" archive "$AR_REV" | tar -x -C "$OUT/ar/arachne-model-plane"
ln -s "$ROOT" "$OUT/ar/moirai-model-plane"
step b build-network-node env CARGO_TARGET_DIR="$ART" "${C[@]}" build -j 2 --offline --locked --manifest-path "$OUT/ar/arachne-model-plane/generated/json_crdt/Cargo.toml" --example network_node \
    && cp "$ART/debug/examples/network_node" "$OUT/bin/network_node"
PROBE="$( (cd "$ROOT" && "${C[@]}" test -j 2 --locked -p moirai-interp --features network --test nw16a_witness_probe \
    --no-run --message-format=json 2>/dev/null) | python3 -I -c 'import json,sys
last=""
for l in sys.stdin:
    try: m=json.loads(l)
    except Exception: continue
    if m.get("reason")=="compiler-artifact" and m.get("executable"): last=m["executable"]
print(last)')"
[ -x "$PROBE" ] && cp "$PROBE" "$OUT/bin/nw16a_witness_probe"
sha256sum "$OUT"/bin/* > "$OUT/bin/sha256.txt" 2>/dev/null
if [ "${NW19_BTDUMP:-1}" = 1 ]; then
    mkdir -p "$OUT/bt" "$SCR/lib"
    if cc -shared -fPIC -O2 -o "$SCR/lib/libbtdump.so" "$ROOT/tests/nw25/btdump.c" 2> "$OUT/btdump-cc.log"; then
        export LD_PRELOAD="$SCR/lib/libbtdump.so${LD_PRELOAD:+:$LD_PRELOAD}" NW22_BT_DIR="$OUT/bt"
        echo "btdump preloaded and armed: $LD_PRELOAD" | tee -a "$OUT/heads.txt"
    else echo "btdump did not build: $(head -3 "$OUT/btdump-cc.log" | tr '\n' ' ')" | tee -a "$OUT/heads.txt"; fi
fi
python3 -I "$HERE/watch.py" --out "$OUT/watch" --stop "$OUT/watch/STOP" --mode "$WATCH_MODE" \
    --probe "$OUT/bin/nw16a_witness_probe" --names model_node network_node \
    --exe-dir "$OUT/bin" "$TGT" "$ART" --step-file "$OUT/watch/step" \
    --probe-intended "${PROBE_INTENDED[@]}" > "$OUT/watch/watch.log" 2>&1 &
WATCHER=$!
echo "watcher pid $WATCHER mode $WATCH_MODE probing intended ${PROBE_INTENDED[*]}" | tee -a "$OUT/heads.txt"

# ------------------------------------------------------------------ P2: r1, the workspace's tests
if want r1; then
step r1 semantics "${C[@]}" test -j 2 -p moirai-semantics --no-fail-fast
step r1 protocol "${C[@]}" test -j 2 -p moirai-protocol --features serde --no-fail-fast
step r1 crdt "${C[@]}" test -j 2 -p moirai-crdt --no-fail-fast
step r1 other "${C[@]}" test -j 2 -p moirai-fuzz -p moirai-macros -p moirai-bootnode -p moirai-relay -p moirai-dashboard --no-fail-fast
step r1 interp env MOIRAI_E2E_INTERP_NODE_BIN="$OUT/bin/model_node" MOIRAI_E2E_RELAY_BIN="$OUT/bin/moirai-relay" MOIRAI_E2E_BOOTNODE_BIN="$OUT/bin/moirai-bootnode" \
    "${C[@]}" test -j 2 -p moirai-interp --features network --no-fail-fast --lib "${P1F[@]}" -- --test-threads=1
step r1 network "${C[@]}" test -j 2 -p moirai-network --no-fail-fast --lib --test layering --test nw04_member_tables --test st11_workload_doc --test st12_e2e_comment
fi

# ------------------------------------------------------------------ P2: r2, the Zenoh items' suites and NW.17's
if want r2; then
if step r2 router-config "${C[@]}" test -j 2 -p moirai-router --test nw16b_router_config_l1; then router_ok=1; else router_ok=0; fi
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
step r2 ms-tcp env -u MOIRAI_TRANSPORT moirai-interp/tests/nw17/modelstore_l3.sh tcp "$OUT/r2/ms"
step r2 ms-zenoh env -u MOIRAI_TRANSPORT moirai-interp/tests/nw17/modelstore_l3.sh zenoh "$OUT/r2/ms"
step r2 ms-compare moirai-interp/tests/nw17/compare.sh "$OUT/r2/ms/tcp" "$OUT/r2/ms/zenoh"
fi

# ------------------------------------------------------------------ P3: e2e_convergence on processes
if want p3; then
step p3 e2e env MOIRAI_E2E_BACKEND=process MOIRAI_E2E_NODE_BIN="$OUT/bin/network_node" MOIRAI_E2E_BOOTNODE_BIN="$OUT/bin/moirai-bootnode" \
    "${C[@]}" test -j 2 --locked -p moirai-network --test e2e_convergence -- --test-threads=1
step p3 e2e-opcount env MOIRAI_E2E_BACKEND=process MOIRAI_E2E_NODE_BIN="$OUT/bin/network_node" MOIRAI_E2E_BOOTNODE_BIN="$OUT/bin/moirai-bootnode" \
    "${C[@]}" test -j 2 --locked -p moirai-network --test e2e_convergence -- --ignored --exact opcount_double_delivery
fi

# ------------------------------------------------------------------ P6: json_crdt's tests
if want p6; then
mkdir -p "$OUT/p6/layout/arachne-model-plane"
git -C "$AR" archive "$AR_REV" | tar -x -C "$OUT/p6/layout/arachne-model-plane"
ln -s "$ROOT" "$OUT/p6/layout/moirai-model-plane"
JC="$OUT/p6/layout/arachne-model-plane/generated/json_crdt"
cp "$JC/Cargo.lock" "$OUT/p6/Cargo.lock.committed"
step p6 json-crdt bash -c "cd '$JC' && CARGO_TARGET_DIR='$ART' flock '$LOCK' nice -n 10 cargo test -j 2 --offline --no-fail-fast"
cmp -s "$OUT/p6/Cargo.lock.committed" "$JC/Cargo.lock" && w=no || w=yes
echo "p6 json_crdt lock rewritten by cargo: $w" | tee -a "$OUT/heads.txt"
fi

# ------------------------------------------------------------------ the end
touch "$OUT/watch/STOP"
wait "$WATCHER"
echo "watcher exit $? $(cat "$OUT/watch/summary.json" 2>/dev/null | tr -d '\n ')" | tee -a "$OUT/heads.txt"
left="$(pgrep -a -f -- "$OUT/bin/|$TGT/debug/examples/model_node|$TGT/debug/moirai-router|$ART/debug/examples/network_node" | grep -v pgrep | head -5)"
[ -z "$left" ] || echo "LEFTOVER processes: $left" | tee -a "$OUT/heads.txt"
echo "end $(date -Is)" | tee -a "$OUT/heads.txt"
summarize "$OUT" | tee -a "$OUT/heads.txt"
python3 -I "$HERE/account.py" table "$OUT/watch/nodes.jsonl" > "$OUT/watch/nodes-table.tsv" 2>&1
exit 0
