#!/bin/sh
#
# Sampling loop for the Compose rig: appends one CSV row per replica per tick.
#
# It gets the replica list from the *bootnode roster*, not from a static list
# baked into the Compose file. That way `--scale node=N` needs no change here,
# and a replica that joins or leaves mid-run appears and disappears in the data
# by itself. It also means the observer exercises the directory rather than
# working around it.
#
# Runs inside the replica image, which already carries curl and coreutils, so
# the rig stays one image. Mounted read-only rather than baked in, so the
# sampling can be changed without a rebuild.
#
#
# # Why this is still here
#
# The dashboard supersedes it for *watching* a run — it shows the same counters
# live, plus per-operation attribution and resolution the CSV never had. It does
# not supersede it for *recording* one: `experiments/e4-silent-member/run.sh`
# mounts this file and its committed `baseline.csv` / `silent.csv` were produced
# by it. Deleting it would make a run with a recorded image digest
# unreproducible, which is the one thing the experiments directory exists to
# prevent. It stays for that, and new measurements should prefer the dashboard
# or a purpose-built script under `experiments/`.
# Environment:
#   BOOTNODE_URL   default http://bootnode:7000
#   SESSION_ID     default compose
#   HTTP_PORT      port the replicas' HTTP API listens on, default 8081
#   INTERVAL_SECS  seconds between samples, default 1
#   OUT            CSV path, default /out/metrics.csv
#
# Deliberately no `set -e`: a sampler that exits because one replica was
# unreachable for one tick destroys the run it was meant to record. Every
# failure is a blank field and `reachable=0` instead.

set -u

BOOTNODE_URL=${BOOTNODE_URL:-http://bootnode:7000}
SESSION_ID=${SESSION_ID:-compose}
HTTP_PORT=${HTTP_PORT:-8081}
INTERVAL_SECS=${INTERVAL_SECS:-1}
OUT=${OUT:-/out/metrics.csv}

# `"field":123` -> `123`
num() {
    printf '%s' "$1" | grep -o "\"$2\":[0-9][0-9]*" | head -n 1 | cut -d: -f2
}

# `"field":"value"` -> `value`
str() {
    printf '%s' "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -n 1 | cut -d'"' -f4
}

if [ ! -s "$OUT" ]; then
    printf '%s\n' \
        "elapsed_s,replica_id,host,reachable,roster_size,stable_prefix,delivered_ops,retained_ops,pending_ops,known_replicas,peer_count,ops_applied,state_md5" \
        >"$OUT"
fi

echo "[observer] sampling session '$SESSION_ID' every ${INTERVAL_SECS}s into $OUT"
start=$(date +%s)

while :; do
    elapsed=$(($(date +%s) - start))

    roster=$(curl -fsS --max-time 2 "$BOOTNODE_URL/session/$SESSION_ID/peers" 2>/dev/null)
    hosts=$(printf '%s' "$roster" | grep -o '"addr":"[^"]*"' | cut -d'"' -f4 | cut -d: -f1)
    roster_size=$(printf '%s\n' "$hosts" | grep -c '[^[:space:]]')

    for host in $hosts; do
        metrics=$(curl -fsS --max-time 2 "http://$host:$HTTP_PORT/api/metrics" 2>/dev/null)
        if [ -z "$metrics" ]; then
            # In the roster but not answering: severed, or mid-restart. Recorded
            # rather than skipped, so a gap in a plot is visible as a gap.
            printf '%s,,%s,0,%s,,,,,,,,\n' "$elapsed" "$host" "$roster_size" >>"$OUT"
            continue
        fi

        state=$(curl -fsS --max-time 2 "http://$host:$HTTP_PORT/api/state" 2>/dev/null)
        # A hash, not the state: convergence is "every replica agrees", which a
        # digest answers exactly, and a full JSON document per replica per
        # second would swamp the metrics it sits next to.
        state_md5=$(printf '%s' "$state" | md5sum | cut -d' ' -f1)

        printf '%s,%s,%s,1,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
            "$elapsed" \
            "$(str "$metrics" replica_id)" \
            "$host" \
            "$roster_size" \
            "$(num "$metrics" stable_prefix)" \
            "$(num "$metrics" delivered_ops)" \
            "$(num "$metrics" retained_ops)" \
            "$(num "$metrics" pending_ops)" \
            "$(num "$metrics" known_replicas)" \
            "$(num "$metrics" peer_count)" \
            "$(num "$metrics" ops_applied)" \
            "$state_md5" \
            >>"$OUT"
    done

    sleep "$INTERVAL_SECS"
done
