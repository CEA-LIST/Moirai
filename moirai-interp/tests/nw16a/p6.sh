#!/bin/bash
# NW.16a, P6 (vault spec/items/NW.16a.md): the twelve transport-agnostic L3
# suites present at d996427, run one at a time on one transport, with each
# suite's time and peak memory recorded.
#
#   moirai-interp/tests/nw16a/p6.sh <tcp|zenoh> <out-dir>
#
# tcp:   MOIRAI_TRANSPORT unset (the default), model_node as built.
# zenoh: MOIRAI_TRANSPORT=zenoh, inherited by every node (no launcher of
#        this crate clears the environment), and model_node reached through
#        nw16a/node_wrapper.sh, which records every start and has
#        nw16a_witness_probe look for the node's Zenoh token at the port it
#        reported. A suite counts as run on Zenoh only when it started at
#        least one node and every node it started was witnessed.
#
# Every cargo call runs under the shared flock (NW16A_CARGO_LOCK), nice'd,
# two jobs. Writes <out-dir>/<transport>/<suite>/{run.log,time.txt,starts,
# *.witness} and <out-dir>/<transport>/summary.txt. Exit 0 when every suite
# passed (and, on zenoh, was witnessed), 1 otherwise; the summary says which.
set -u
T=${1:?usage: p6.sh <tcp|zenoh> <out-dir>}
O=${2:?usage: p6.sh <tcp|zenoh> <out-dir>}
case "$T" in tcp|zenoh) ;; *) echo "transport must be tcp or zenoh" >&2; exit 2 ;; esac
HERE=$(cd "$(dirname "$0")" && pwd)
Z=$(cd "$HERE/../../.." && pwd)
L=${NW16A_CARGO_LOCK:-/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock}
# A scratch copy built with CARGO_TARGET_DIR elsewhere finds its node there;
# NW16A_NODE_BIN names another model_node outright (a control arm's build),
# and is then not built here.
NODE=${NW16A_NODE_BIN:-${CARGO_TARGET_DIR:-$Z/target}/debug/examples/model_node}
SUITES=(e2e_interp si01_store_identity_l3 st01_optional_conflict_race_l3
    st02_emptied_optional_object_l3 st04_emptied_optional_attribute_l3
    st05_recursive_keyed_class_l3 st05_removed_key_l3 nw07_named_model_ids_l3
    ed10_umlclass_u2_l3 ed16a_notation_diagrams_l3 ed20_join_without_metamodel_l3
    ar02_declarations_served_l3)
# NW16A_P6_SUITES narrows the run to some of the twelve (a rehearsal or an
# arm); P6 itself is the twelve.
if [ -n "${NW16A_P6_SUITES:-}" ]; then read -r -a SUITES <<< "$NW16A_P6_SUITES"; fi
cd "$Z" || exit 2
for name in ZENOH_SCOUTING ZENOH_CONFIG ZENOH_ROUTER ZENOH_MODE MOIRAI_E2E_INTERP_NODE_BIN; do
    if [ -n "${!name:-}" ]; then echo "refusing: $name is set" >&2; exit 2; fi
done

if [ -z "${NW16A_NODE_BIN:-}" ]; then
    CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo build -j 2 -p moirai-interp --features network \
        --example model_node || exit 2
fi
[ -x "$NODE" ] || { echo "no model_node at $NODE" >&2; exit 2; }
PROBE=$(CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo test -j 2 -p moirai-interp \
    --features network --test nw16a_witness_probe --no-run --message-format=json 2>/dev/null \
    | python3 -c 'import sys, json
out = [m["executable"] for m in map(json.loads, sys.stdin)
       if m.get("reason") == "compiler-artifact" and m.get("executable")]
print(out[-1] if out else "")')
[ -x "$PROBE" ] || { echo "no witness probe built" >&2; exit 2; }
for s in "${SUITES[@]}"; do
    CARGO_BUILD_JOBS=2 flock "$L" nice -n 10 cargo test -j 2 -p moirai-interp --features network \
        --test "$s" --no-run >/dev/null 2>&1 || { echo "suite $s does not build" >&2; exit 2; }
done

mkdir -p "$O/$T"
SUMMARY=$O/$T/summary.txt
: > "$SUMMARY"
status=0
for s in "${SUITES[@]}"; do
    W=$O/$T/$s
    rm -rf "$W"; mkdir -p "$W"; : > "$W/starts"
    if [ "$T" = zenoh ]; then
        env MOIRAI_TRANSPORT=zenoh MOIRAI_E2E_INTERP_NODE_BIN="$HERE/node_wrapper.sh" \
            NW16A_REAL_NODE="$NODE" NW16A_PROBE_BIN="$PROBE" NW16A_WITNESS_DIR="$W" \
            CARGO_BUILD_JOBS=2 flock "$L" /usr/bin/time -v -o "$W/time.txt" nice -n 10 \
            cargo test -j 2 -p moirai-interp --features network --no-fail-fast --test "$s" \
            > "$W/run.log" 2>&1
        rc=$?
    else
        env -u MOIRAI_TRANSPORT MOIRAI_E2E_INTERP_NODE_BIN="$NODE" \
            CARGO_BUILD_JOBS=2 flock "$L" /usr/bin/time -v -o "$W/time.txt" nice -n 10 \
            cargo test -j 2 -p moirai-interp --features network --no-fail-fast --test "$s" \
            > "$W/run.log" 2>&1
        rc=$?
    fi
    # Probes outlive their node by at most their own timeouts; wait for them.
    for _ in $(seq 150); do
        pgrep -f -- "$PROBE --ignored --exact probe" >/dev/null || break
        sleep 1
    done
    passed=$(grep -h '^test result' "$W/run.log" | awk '{p+=$4} END {print p+0}')
    failed=$(grep -h '^test result' "$W/run.log" | awk '{f+=$6} END {print f+0}')
    failing=$(grep -E '^test .* \.\.\. FAILED$' "$W/run.log" | awk '{print $2}' | sort | tr '\n' ' ')
    wall=$(grep 'Elapsed (wall clock)' "$W/time.txt" | awk '{print $NF}')
    rss=$(grep 'Maximum resident set size' "$W/time.txt" | awk '{print $NF}')
    skips=$(grep -c 'E2E-SKIP' "$W/run.log")
    line="$s rc=$rc passed=$passed failed=$failed skips=$skips wall=$wall max_rss_kb=$rss failing=[${failing% }]"
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
        if [ "$started" -eq 0 ] || [ "$witnessed" -ne "$started" ]; then status=1; fi
    fi
    [ "$rc" -eq 0 ] || status=1
    echo "$line" | tee -a "$SUMMARY"
done
echo "status=$status" | tee -a "$SUMMARY"
exit $status
