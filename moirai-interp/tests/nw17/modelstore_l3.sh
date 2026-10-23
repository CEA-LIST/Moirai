#!/bin/bash
# NW.17, claim point 3 (vault spec/items/NW.17.md): the eight L3 suites
# akira/moirai-modelstore added since d996427, run one at a time on one
# transport, one test thread each, every test's result kept for
# nw17/compare.sh, which reads a zenoh run against a tcp run.
#
#   moirai-interp/tests/nw17/modelstore_l3.sh <tcp|zenoh> <out-dir>
#
# tcp:   MOIRAI_TRANSPORT unset (the default), model_node as built; every
#        test runs, those bound to TCP by design included.
# zenoh: MOIRAI_TRANSPORT=zenoh, inherited by every node (no launcher of
#        these suites clears the environment or sets the variable), and
#        model_node reached through nw16a/node_wrapper.sh, NW.16a's P6
#        wrapper, unchanged: it records every start and has
#        nw16a_witness_probe look for the node's Zenoh token at the port the
#        node reported. A test bound to TCP by design is skipped (--skip) and
#        listed N/A with its reason in na.txt, never dropped: compare.sh
#        counts it.
#
# The tests bound to TCP by design. si13_migration_l3's two tests run on
# SI.13's cluster (si13/cluster.rs): a moirai-bootnode and a moirai-relay,
# every node started with BOOTNODE_URL, which a node on Zenoh refuses
# (NW.15, TransportConfig), and node si13-c reaching the others only
# through the relay, a topology the tests check before anything else.
# Ignored on both transports, not N/A: si12_scale_l3's two tests and
# si13_scale_l3's 5,000-object test (long evaluations, #[ignore]; the last
# one also relayed). si13_scale_l3's premise test starts no node.
#
# Every cargo call runs under the shared flock (NW17_CARGO_LOCK), nice'd,
# two jobs. NW17_NODE_BIN names another model_node outright (a control
# arm's build), which is then not built here; NW17_PROBE_BIN names a witness
# probe built elsewhere (the red proof's, at a tree whose dev-dependencies
# lack zenoh); NW17_SUITES narrows the run to some of the eight (an arm or a
# rehearsal; the claim is the eight). Writes <out-dir>/<transport>/<suite>/
# {run.log,time.txt,results.txt,na.txt,starts,*.witness} and
# <out-dir>/<transport>/summary.txt. Exit 0 when every suite run passed
# (and, on zenoh, every node started was witnessed), 1 otherwise, 2 when the
# run could not start; the comparison is compare.sh's, not this exit status.
set -u
T=${1:?usage: modelstore_l3.sh <tcp|zenoh> <out-dir>}
O=${2:?usage: modelstore_l3.sh <tcp|zenoh> <out-dir>}
case "$T" in tcp|zenoh) ;; *) echo "transport must be tcp or zenoh" >&2; exit 2 ;; esac
HERE=$(cd "$(dirname "$0")" && pwd)
Z=$(cd "$HERE/../../.." && pwd)
L=${NW17_CARGO_LOCK:-/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock}
TARGET=${CARGO_TARGET_DIR:-$Z/target}
NODE=${NW17_NODE_BIN:-$TARGET/debug/examples/model_node}
WRAPPER=$HERE/../nw16a/node_wrapper.sh
SUITES=(si05_references_l3 si08_target_side_l3 si12_copy_migration_l3 si12_scale_l3
    si13_migration_l3 si13_scale_l3 si14_rebase_l3 si16_rebased_joiner_l3)
if [ -n "${NW17_SUITES:-}" ]; then read -r -a SUITES <<< "$NW17_SUITES"; fi
# suite<TAB>test<TAB>reason, for every test bound to TCP by design.
NA=$(printf '%s\t%s\t%s\n' \
    si13_migration_l3 si13_l3_two_concurrent_migrations_name_one_successor_on_every_node \
    "a moirai-bootnode and a moirai-relay: every node gets BOOTNODE_URL, which Zenoh refuses (NW.15), and si13-c reaches a and b only through the relay (si13/cluster.rs)" \
    si13_migration_l3 si13_l3_a_migration_answers_in_time_reports_progress_and_reaches_a_late_joiner \
    "a moirai-bootnode and a moirai-relay: every node gets BOOTNODE_URL, which Zenoh refuses (NW.15), and si13-c and si13-d reach the others only through the relay (si13/cluster.rs)")
cd "$Z" || exit 2
for name in ZENOH_SCOUTING ZENOH_CONFIG ZENOH_ROUTER ZENOH_MODE ZENOH_REGION MOIRAI_TRANSPORT \
    MOIRAI_E2E_INTERP_NODE_BIN MOIRAI_E2E_RELAY_BIN MOIRAI_E2E_BOOTNODE_BIN; do
    if [ -n "${!name:-}" ]; then echo "refusing: $name is set" >&2; exit 2; fi
done

if [ -z "${NW17_NODE_BIN:-}" ]; then
    CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo build -j 2 -p moirai-interp --features network \
        --example model_node || exit 2
fi
[ -x "$NODE" ] || { echo "no model_node at $NODE" >&2; exit 2; }
CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo build -j 2 -p moirai-relay -p moirai-bootnode || exit 2
RELAY=$TARGET/debug/moirai-relay
BOOTNODE=$TARGET/debug/moirai-bootnode
[ -x "$RELAY" ] && [ -x "$BOOTNODE" ] || { echo "no relay or bootnode under $TARGET" >&2; exit 2; }
PROBE=""
if [ "$T" = zenoh ]; then
    if [ -n "${NW17_PROBE_BIN:-}" ]; then
        PROBE=$NW17_PROBE_BIN
    else
        PROBE=$(CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo test -j 2 -p moirai-interp \
            --features network --test nw16a_witness_probe --no-run --message-format=json 2>/dev/null \
            | python3 -c 'import sys, json
out = [m["executable"] for m in map(json.loads, sys.stdin)
       if m.get("reason") == "compiler-artifact" and m.get("executable")]
print(out[-1] if out else "")')
    fi
    [ -n "$PROBE" ] && [ -x "$PROBE" ] || { echo "no witness probe built" >&2; exit 2; }
fi
for s in "${SUITES[@]}"; do
    CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo test -j 2 -p moirai-interp --features network \
        --test "$s" --no-run >/dev/null 2>&1 || { echo "suite $s does not build" >&2; exit 2; }
done

mkdir -p "$O/$T"
SUMMARY=$O/$T/summary.txt
: > "$SUMMARY"
{
    echo "# transport=$T node=$NODE sha256=$(sha256sum "$NODE" | cut -c1-64)"
    [ -n "$PROBE" ] && echo "# probe=$PROBE sha256=$(sha256sum "$PROBE" | cut -c1-64)"
    echo "# tree=$(git -C "$Z" rev-parse HEAD 2>/dev/null) status=[$(git -C "$Z" status --porcelain 2>/dev/null | wc -l)] started=$(date -Iseconds)"
} | tee -a "$SUMMARY"
status=0
for s in "${SUITES[@]}"; do
    W=$O/$T/$s
    rm -rf "$W"; mkdir -p "$W"; : > "$W/starts"; : > "$W/na.txt"
    skips=()
    if [ "$T" = zenoh ]; then
        while IFS=$'\t' read -r suite test reason; do
            [ "$suite" = "$s" ] || continue
            printf '%s\t%s\n' "$test" "$reason" >> "$W/na.txt"
            skips+=(--skip "$test")
        done <<< "$NA"
        env MOIRAI_TRANSPORT=zenoh MOIRAI_E2E_INTERP_NODE_BIN="$WRAPPER" \
            MOIRAI_E2E_RELAY_BIN="$RELAY" MOIRAI_E2E_BOOTNODE_BIN="$BOOTNODE" \
            NW16A_REAL_NODE="$NODE" NW16A_PROBE_BIN="$PROBE" NW16A_WITNESS_DIR="$W" \
            CARGO_BUILD_JOBS=2 flock "$L" /usr/bin/time -v -o "$W/time.txt" nice -n 10 \
            cargo test -j 2 -p moirai-interp --features network --no-fail-fast --test "$s" \
            -- --test-threads=1 "${skips[@]}" > "$W/run.log" 2>&1
        rc=$?
    else
        env -u MOIRAI_TRANSPORT MOIRAI_E2E_INTERP_NODE_BIN="$NODE" \
            MOIRAI_E2E_RELAY_BIN="$RELAY" MOIRAI_E2E_BOOTNODE_BIN="$BOOTNODE" \
            CARGO_BUILD_JOBS=2 flock "$L" /usr/bin/time -v -o "$W/time.txt" nice -n 10 \
            cargo test -j 2 -p moirai-interp --features network --no-fail-fast --test "$s" \
            -- --test-threads=1 > "$W/run.log" 2>&1
        rc=$?
    fi
    # Probes outlive their node by at most their own timeouts; wait for them.
    if [ -n "$PROBE" ]; then
        for _ in $(seq 150); do
            pgrep -f -- "$PROBE --ignored --exact probe" >/dev/null || break
            sleep 1
        done
    fi
    # One line per test: name, then ok, FAILED or ignored.
    grep -E '^test [^ ]+ \.\.\. (ok|FAILED|ignored)' "$W/run.log" \
        | sed -E 's/^test ([^ ]+) \.\.\. (ok|FAILED|ignored).*/\1 \2/' | sort > "$W/results.txt"
    passed=$(grep -c ' ok$' "$W/results.txt")
    failed=$(grep -c ' FAILED$' "$W/results.txt")
    ignored=$(grep -c ' ignored$' "$W/results.txt")
    filtered=$(grep -h '^test result' "$W/run.log" | grep -oE '[0-9]+ filtered out' \
        | awk '{f+=$1} END {print f+0}')
    na=$(grep -c . "$W/na.txt")
    failing=$(grep ' FAILED$' "$W/results.txt" | cut -d' ' -f1 | tr '\n' ' ')
    na_tests=$(cut -f1 "$W/na.txt" | tr '\n' ' ')
    wall=$(grep 'Elapsed (wall clock)' "$W/time.txt" | awk '{print $NF}')
    rss=$(grep 'Maximum resident set size' "$W/time.txt" | awk '{print $NF}')
    e2e_skips=$(grep -c 'E2E-SKIP' "$W/run.log")
    line="$s rc=$rc passed=$passed failed=$failed ignored=$ignored filtered=$filtered na=$na e2e_skips=$e2e_skips wall=$wall max_rss_kb=$rss failing=[${failing% }] na_tests=[${na_tests% }]"
    if [ "$T" = zenoh ]; then
        started=$(grep -c '^pid=' "$W/starts")
        witnessed=0; missing=""
        while read -r start; do
            pid=${start#pid=}; pid=${pid%% *}
            if grep -q ' token=moirai/' "$W/$pid.witness" 2>/dev/null; then
                witnessed=$((witnessed + 1))
            else
                missing="$missing $pid:$(head -c 200 "$W/$pid.witness" 2>/dev/null | tr '\n' ' ')"
            fi
        done < "$W/starts"
        line="$line started=$started witnessed=$witnessed missing=[${missing# }]"
        [ "$witnessed" -eq "$started" ] || status=1
    fi
    [ "$rc" -eq 0 ] || status=1
    [ "$e2e_skips" -eq 0 ] || status=1
    echo "$line" | tee -a "$SUMMARY"
done
echo "status=$status ended=$(date -Iseconds)" | tee -a "$SUMMARY"
exit $status
