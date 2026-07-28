#!/usr/bin/env bash
#
# Tooling track — what does dashboard reporting cost the replicas?
#
# The requirement is that it costs nothing measurable. This is the measurement
# behind that claim rather than an assertion of it.
#
# Two arms, identical in every other respect — same image, same replicas, same
# seeded workload, same duration:
#
#   off   `DASHBOARD_URL` unset. The replica starts no reporting thread, makes
#         no outbound request, and collects no delivery trace.
#   on    `DASHBOARD_URL` set at a dashboard container that is actually
#         consuming. The arm is only counted if the dashboard confirms it
#         received reports, so "no overhead because nothing was reported" is
#         not a way to pass.
#
# The oracle is `ops_applied` from `/api/metrics`, summed over the replicas.
# That counter is exact — it counts operations *originated* here — unlike
# `/api/operations`, which double-counts remote deliveries. Throughput is that
# sum over the wall-clock duration.
#
# # Why the load is concurrent
#
# `network_node`'s event loop sleeps 10 ms per iteration, so a single serial
# client is capped near 100 operations per second by the tick and would measure
# the sleep rather than the replica. Several clients per replica keep the
# adapter channel non-empty, so each iteration drains a batch and the loop's
# actual per-operation cost is what limits the rate. That is the thing under
# measurement.
#
# Usage, from this directory, on a host whose shell needs `sg docker`:
#
#     sg docker -c ./run.sh
#
# Knobs (all optional): DURATION, REPLICAS, CLIENTS_PER_REPLICA, SEED,
# MOIRAI_IMAGE.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

DURATION=${DURATION:-60}
REPLICAS=${REPLICAS:-3}
CLIENTS_PER_REPLICA=${CLIENTS_PER_REPLICA:-3}
SEED=${SEED:-20260729}
IMAGE=${MOIRAI_IMAGE:-moirai-json-crdt:test}

stamp=$(date -u +%Y%m%dT%H%M%SZ)
out="$HERE/throughput.csv"
net=""

teardown() {
    if [ -n "$net" ]; then
        docker ps -aq --filter "network=$net" | xargs -r docker rm --force --volumes >/dev/null 2>&1 || true
        docker network rm "$net" >/dev/null 2>&1 || true
    fi
}
trap teardown EXIT

# Echoes the id of every replica container in the current arm.
replica_names() {
    local i
    for i in $(seq 1 "$REPLICAS"); do echo "$net-r$i"; done
}

# `PEERS` for replica $1, naming every other replica. Static rather than
# discovered: this measures the delivery path, and a bootnode would add a
# variable that has nothing to do with the question.
peers_for() {
    local self="$1" i spec=""
    for i in $(seq 1 "$REPLICAS"); do
        [ "$net-r$i" = "$self" ] && continue
        spec="${spec:+$spec,}r$i:$net-r$i:9001"
    done
    echo "$spec"
}

# $1 = arm name ("off" | "on")
run_arm() {
    local arm="$1"
    local dashboard_env=() dash="" i name

    net="moirai-overhead-$arm-$stamp"
    docker network create "$net" >/dev/null

    if [ "$arm" = "on" ]; then
        dash="$net-dash"
        docker run --detach --name "$dash" --network "$net" \
            --network-alias dashboard "$IMAGE" \
            moirai-dashboard --port 8090 >/dev/null
        # Poll to a deadline; never sleep-then-assert.
        local deadline=$((SECONDS + 30))
        until docker run --rm --network "$net" "$IMAGE" \
              curl -fsS "http://dashboard:8090/api/health" >/dev/null 2>&1; do
            [ "$SECONDS" -lt "$deadline" ] || { echo "dashboard never became healthy" >&2; exit 1; }
        done
        dashboard_env=(--env "DASHBOARD_URL=http://dashboard:8090"
                       --env "DASHBOARD_INTERVAL_MS=${DASHBOARD_INTERVAL_MS:-500}")
    fi

    for i in $(seq 1 "$REPLICAS"); do
        name="$net-r$i"
        docker run --detach --name "$name" --network "$net" \
            --network-alias "$name" \
            --env "REPLICA_ID=r$i" \
            --env "LISTEN_PORT=9001" \
            --env "HTTP_PORT=8081" \
            --env "PEERS=$(peers_for "$name")" \
            "${dashboard_env[@]}" \
            "$IMAGE" >/dev/null
    done

    # Wait for the mesh: every replica must see every other one, or the arms
    # would be driving different topologies.
    local deadline=$((SECONDS + 90))
    local want=$((REPLICAS - 1))
    while :; do
        local ready=0
        for name in $(replica_names); do
            local n
            n=$(docker exec "$name" curl -fsS localhost:8081/api/metrics 2>/dev/null \
                | grep -o '"peer_count":[0-9]*' | cut -d: -f2 || echo 0)
            [ "${n:-0}" -ge "$want" ] && ready=$((ready + 1))
        done
        [ "$ready" -eq "$REPLICAS" ] && break
        [ "$SECONDS" -lt "$deadline" ] || { echo "$arm: mesh never formed" >&2; exit 1; }
    done

    # --- load ---
    local targets="" drivers=()
    for name in $(replica_names); do
        targets="${targets:+$targets,}http://$name:8081"
    done

    local started=$SECONDS c
    for i in $(seq 1 "$REPLICAS"); do
        for c in $(seq 1 "$CLIENTS_PER_REPLICA"); do
            docker run --detach --name "$net-drv-$i-$c" --network "$net" "$IMAGE" \
                moirai-dashboard --random \
                --seed "$((SEED + i * 100 + c))" \
                --rate 100000 --count 1000000 \
                --nodes "$targets" >/dev/null
            drivers+=("$net-drv-$i-$c")
        done
    done

    while [ $((SECONDS - started)) -lt "$DURATION" ]; do :; done
    local elapsed=$((SECONDS - started))
    docker rm --force "${drivers[@]}" >/dev/null 2>&1 || true

    # --- read the exact counter ---
    local applied=0 delivered=0 n
    for name in $(replica_names); do
        n=$(docker exec "$name" curl -fsS localhost:8081/api/metrics \
            | grep -o '"ops_applied":[0-9]*' | cut -d: -f2)
        applied=$((applied + n))
        n=$(docker exec "$name" curl -fsS localhost:8081/api/metrics \
            | grep -o '"delivered_ops":[0-9]*' | cut -d: -f2)
        delivered=$((delivered + n))
    done

    local reports=0 dropped=0
    if [ "$arm" = "on" ]; then
        reports=$(docker run --rm --network "$net" "$IMAGE" \
            curl -fsS http://dashboard:8090/api/snapshot \
            | grep -o '"reports":[0-9]*' | cut -d: -f2)
        dropped=$(docker run --rm --network "$net" "$IMAGE" \
            curl -fsS http://dashboard:8090/api/snapshot \
            | grep -o '"dropped_reports":[0-9]*' | cut -d: -f2 | sort -rn | head -1)
        if [ "${reports:-0}" -eq 0 ]; then
            echo "the 'on' arm reported nothing, so it would measure the 'off' arm twice" >&2
            exit 1
        fi
    fi

    printf '%s,%s,%s,%s,%s,%s,%s,%s\n' \
        "$arm" "$elapsed" "$REPLICAS" "$((REPLICAS * CLIENTS_PER_REPLICA))" \
        "$applied" "$((applied / elapsed))" "$delivered" "${reports:-0}" >> "$out"
    echo "$arm: $applied operations in ${elapsed}s = $((applied / elapsed)) ops/s (dashboard reports: ${reports:-0}, dropped: ${dropped:-0})"

    teardown
    net=""
}

echo "arm,duration_s,replicas,clients,ops_applied,ops_per_s,delivered_ops,dashboard_reports" > "$out"
run_arm off
run_arm on
run_arm off
run_arm on

{
    echo "run          $stamp"
    echo "host         $(uname -sr) $(uname -m)"
    echo "image        $IMAGE"
    echo "image digest $(docker image inspect --format '{{index .RepoDigests 0}}' "$IMAGE" 2>/dev/null \
                        || docker image inspect --format '{{.Id}}' "$IMAGE")"
    echo "docker       $(docker version --format '{{.Server.Version}}')"
    echo "replicas     $REPLICAS"
    echo "clients      $((REPLICAS * CLIENTS_PER_REPLICA))"
    echo "duration_s   $DURATION"
    echo "seed         $SEED"
    echo "arms         off, on, off, on (interleaved, so drift shows up as spread rather than as an effect)"
    echo "oracle       sum of /api/metrics ops_applied across replicas"
    echo "command      DURATION=$DURATION REPLICAS=$REPLICAS CLIENTS_PER_REPLICA=$CLIENTS_PER_REPLICA SEED=$SEED ./run.sh"
} > "$HERE/manifest.txt"

echo
column -s, -t < "$out"
