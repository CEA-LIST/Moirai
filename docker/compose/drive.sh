#!/bin/sh
#
# Load driver for the Compose rig: one operation per replica per tick.
#
# **Every replica writes, on purpose.** Causal stability advances only on
# inbound traffic — a replica learns a peer's clock from the messages that peer
# sends, and there is no acknowledgement or heartbeat. A run in which only some
# replicas write therefore shows a stalled stable prefix for the silent ones,
# which is indistinguishable from the effect E4 exists to measure. Driving
# every replica is what makes the baseline a baseline.
#
# The replica list comes from the bootnode roster, like the observer's. A
# replica severed from the network stops refreshing its registration and drops
# out, so the driver stops writing to it by itself — which is exactly the
# behaviour a silent member should produce.
#
#
# # Why this is still here
#
# Superseded for new work by `moirai-dashboard --random`, which shares its
# generator with the e2e suite's R1 scenario, so a run can be replayed from its
# seed. This one stays because `experiments/e4-silent-member/run.sh` brings it
# up by profile and the committed E4 curves were measured with it; removing it
# would make that run unreproducible.
# Environment:
#   BOOTNODE_URL     default http://bootnode:7000
#   SESSION_ID       default compose
#   HTTP_PORT        default 8081
#   OP_INTERVAL_SECS seconds between rounds, default 1

set -u

BOOTNODE_URL=${BOOTNODE_URL:-http://bootnode:7000}
SESSION_ID=${SESSION_ID:-compose}
HTTP_PORT=${HTTP_PORT:-8081}
OP_INTERVAL_SECS=${OP_INTERVAL_SECS:-1}

echo "[driver] one op per replica every ${OP_INTERVAL_SECS}s in session '$SESSION_ID'"

while :; do
    roster=$(curl -fsS --max-time 2 "$BOOTNODE_URL/session/$SESSION_ID/peers" 2>/dev/null)
    hosts=$(printf '%s' "$roster" | grep -o '"addr":"[^"]*"' | cut -d'"' -f4 | cut -d: -f1)

    for host in $hosts; do
        # A counter increment under the replica's own key. Increments commute,
        # so nothing here can fail to converge for semantic reasons and the
        # only thing the run measures is the replication layer.
        curl -fsS --max-time 2 -o /dev/null \
            -X POST "http://$host:$HTTP_PORT/api/op" \
            -d "{\"JsonKind\":{\"Object\":{\"Update\":[\"k_$host\",{\"Number\":{\"Inc\":1}}]}}}" \
            2>/dev/null
    done

    sleep "$OP_INTERVAL_SECS"
done
