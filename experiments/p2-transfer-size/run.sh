#!/usr/bin/env bash
#
# Phase 2 — how large does a joiner's state transfer get?
#
# Design §5.1 leaves the size of a state transfer open: it is unbounded in
# principle, and the question is whether chunking is needed or whether "one
# message, and measure it" is enough for a session-sized model. This produces
# the number.
#
# Two replicas, both writing, so the causally stable frontier keeps advancing
# and the donor's outbox stays short — the shape a healthy session has. After
# every batch of operations the rig opens a replication connection to the
# donor, asks for a state transfer under an id no replica has heard from, and
# records the length of the reply *on the wire*.
#
# Usage, from this directory, on a host whose shell needs `sg docker`:
#
#     sg docker -c ./run.sh
#
# Knobs (all optional): STEPS, OPS_PER_STEP, MOIRAI_IMAGE.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

STEPS=${STEPS:-12}
OPS_PER_STEP=${OPS_PER_STEP:-20}
IMAGE=${MOIRAI_IMAGE:-moirai-json-crdt:test}

stamp=$(date -u +%Y%m%dT%H%M%SZ)
net="moirai-p2-size-$stamp"
a="$net-a"
b="$net-b"

teardown() {
    docker rm --force --volumes "$a" "$b" >/dev/null 2>&1 || true
    docker network rm "$net" >/dev/null 2>&1 || true
}
trap teardown EXIT

docker network create "$net" >/dev/null

# Both the HTTP API and the replication listener are published: the probe has
# to speak the replication protocol from the host, which is the only way to
# measure the payload as it actually crosses the wire rather than re-deriving
# it from the replica's own state.
start_node() {
    local name=$1 peers=$2
    docker run --detach --name "$name" --network "$net" \
        --env REPLICA_ID="${name##*-}" \
        --env LISTEN_PORT=9001 \
        --env HTTP_PORT=8081 \
        --env PEERS="$peers" \
        --publish 127.0.0.1::8081 \
        --publish 127.0.0.1::9001 \
        "$IMAGE" >/dev/null
}

start_node "$a" "b:$b:9001"
start_node "$b" "a:$a:9001"

http_of() { docker port "$1" 8081/tcp | head -1; }
sync_of() { docker port "$1" 9001/tcp | head -1; }

python3 "$HERE/measure.py" \
    --donor-http "http://$(http_of "$a")" \
    --peer-http "http://$(http_of "$b")" \
    --donor-sync "$(sync_of "$a")" \
    --steps "$STEPS" \
    --ops-per-step "$OPS_PER_STEP" \
    --out "$HERE/size.csv"

{
    printf '%-13s%s\n' run "$stamp"
    printf '%-13s%s\n' host "$(uname -sro)"
    printf '%-13s%s\n' image "$IMAGE"
    printf '%-13s%s\n' "image digest" \
        "$(docker image inspect --format '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null \
            || docker image inspect --format '{{.Id}}' "$IMAGE")"
    printf '%-13s%s\n' docker "$(docker version --format '{{.Server.Version}}')"
    printf '%-13s%s\n' nodes 2
    printf '%-13s%s\n' steps "$STEPS"
    printf '%-13s%s\n' ops_per_step "$OPS_PER_STEP"
    printf '%-13s%s\n' command "STEPS=$STEPS OPS_PER_STEP=$OPS_PER_STEP ./run.sh"
} > "$HERE/manifest.txt"

cat "$HERE/manifest.txt"
