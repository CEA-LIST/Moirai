#!/usr/bin/env bash
#
# E4 — what one silent member costs.
#
# Two runs of the same rig, identical apart from one event:
#
#   baseline  N replicas, all live, all writing, for the whole run
#   silent    the same, except one replica is severed from the network at
#             SEVER_AT seconds and never comes back
#
# What to look at afterwards: `stable_prefix` and `retained_ops` on the
# *survivors*. The severed replica is not observable after the cut — the rig
# has one network, so disconnecting it takes the control plane with it — and
# that is fine, because the claim under test is about the replicas that are
# still running.
#
# The claim, from the weekly report, is asserted there in prose: "logs grow
# unboundedly". This turns it into a curve.
#
# Usage, from this directory, on a host whose shell needs `sg docker`:
#
#     sg docker -c ./run.sh
#
# Knobs (all optional): NODES, DURATION, SEVER_AT, OP_INTERVAL_SECS,
# INTERVAL_SECS, RECONCILE_SECS, BOOTNODE_TTL_SECS.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$HERE/../../docker/compose"

NODES=${NODES:-4}
DURATION=${DURATION:-180}
SEVER_AT=${SEVER_AT:-60}
IMAGE=${MOIRAI_IMAGE:-moirai-json-crdt:test}

export MOIRAI_IMAGE="$IMAGE"
export OP_INTERVAL_SECS=${OP_INTERVAL_SECS:-1}
export INTERVAL_SECS=${INTERVAL_SECS:-1}
export RECONCILE_SECS=${RECONCILE_SECS:-5}
export BOOTNODE_TTL_SECS=${BOOTNODE_TTL_SECS:-30}

# One session id per run, so a bootnode left over from an earlier attempt
# cannot contribute stale members to this one.
run_stamp=$(date -u +%Y%m%dT%H%M%SZ)

teardown() {
    (cd "$COMPOSE_DIR" && docker compose --profile load down -v >/dev/null 2>&1) || true
}
trap teardown EXIT

# Each run leaves its CSV in this directory, named for the condition.
run_once() {
    local label="$1" sever="$2"
    local out="$HERE/$label.csv"
    rm -f "$out"

    export SESSION_ID="e4-$label-$run_stamp"
    export MOIRAI_RUN_DIR="$HERE"

    echo "=== $label: $NODES replicas, ${DURATION}s, sever=$sever ==="
    (cd "$COMPOSE_DIR" && docker compose --profile load up -d --scale "node=$NODES" >/dev/null)

    # Poll to a deadline for readiness rather than sleeping a guessed amount:
    # the rig is not ready when the containers are up, it is ready when every
    # replica has found every other one.
    local deadline=$((SECONDS + 120))
    while :; do
        roster=$(cd "$COMPOSE_DIR" && docker compose exec -T observer \
            curl -fsS --max-time 2 "http://bootnode:7000/session/$SESSION_ID/peers" 2>/dev/null || true)
        count=$(printf '%s' "$roster" | grep -o '"id":' | wc -l)
        if [ "$count" -eq "$NODES" ]; then break; fi
        if [ "$SECONDS" -ge "$deadline" ]; then
            echo "the roster never reached $NODES members (last: $count)" >&2
            exit 1
        fi
        sleep 1
    done
    echo "--- roster complete at $NODES members, running for ${DURATION}s ---"

    if [ "$sever" -gt 0 ]; then
        sleep "$sever"
        echo "--- t+${sever}s: severing moirai-node-$NODES ---"
        docker network disconnect moirai_session "moirai-node-$NODES"
        sleep $((DURATION - sever))
    else
        sleep "$DURATION"
    fi

    (cd "$COMPOSE_DIR" && docker compose --profile load down -v >/dev/null 2>&1)
    mv "$HERE/metrics.csv" "$out"
    echo "--- wrote $out ($(wc -l <"$out") rows) ---"
}

run_once baseline 0
run_once silent "$SEVER_AT"

# Recorded so the run can be reproduced rather than merely believed.
{
    echo "run          $run_stamp"
    echo "host         $(uname -srm)"
    echo "image        $IMAGE"
    echo "image digest $(docker image inspect --format '{{.Id}}' "$IMAGE")"
    echo "compose      $(docker compose version --short)"
    echo "docker       $(docker version --format '{{.Server.Version}}')"
    echo "nodes        $NODES"
    echo "duration_s   $DURATION"
    echo "sever_at_s   $SEVER_AT"
    echo "op_interval  $OP_INTERVAL_SECS"
    echo "sample_every $INTERVAL_SECS"
    echo "reconcile_s  $RECONCILE_SECS"
    echo "bootnode_ttl $BOOTNODE_TTL_SECS"
    echo "command      NODES=$NODES DURATION=$DURATION SEVER_AT=$SEVER_AT ./run.sh"
} >"$HERE/manifest.txt"

echo
cat "$HERE/manifest.txt"
echo
echo "Now: python3 -m venv .venv && .venv/bin/pip install matplotlib && .venv/bin/python plot.py"
