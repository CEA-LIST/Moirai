#!/bin/sh
# NW.16a, P6 (vault spec/items/NW.16a.md): stands in for model_node through
# MOIRAI_E2E_INTERP_NODE_BIN while nw16a/p6.sh runs a suite under Zenoh.
# Records the start, starts the witness probe beside the node, then becomes
# the node: exec keeps the pid, which PORTS_FILE must name.
#
# Needs NW16A_WITNESS_DIR, NW16A_REAL_NODE and NW16A_PROBE_BIN.
d="${NW16A_WITNESS_DIR:?NW16A_WITNESS_DIR is unset}"
printf 'pid=%s id=%s transport=%s ports_file=%s\n' \
    "$$" "${REPLICA_ID:-}" "${MOIRAI_TRANSPORT:-unset}" "${PORTS_FILE:-}" >> "$d/starts"
if [ -n "${PORTS_FILE:-}" ]; then
    NW16A_PROBE_PORTS_FILE="$PORTS_FILE" NW16A_PROBE_ID="${REPLICA_ID:-}" \
        NW16A_PROBE_PID="$$" NW16A_PROBE_OUT="$d/$$.witness" \
        "${NW16A_PROBE_BIN:?NW16A_PROBE_BIN is unset}" --ignored --exact probe --quiet \
        </dev/null >/dev/null 2>&1 &
fi
exec "${NW16A_REAL_NODE:?NW16A_REAL_NODE is unset}" "$@"
