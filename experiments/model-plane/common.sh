#!/usr/bin/env bash
#
# Shared by the model-plane measurement harnesses (m-e1 to m-e6): the run
# stamp, the manifest fields every measurement records, the container helpers
# the wire measurements start replicas with, and the discipline the runs are
# taken under. Sourced, never executed.
#
# Every manifest states the machine, the load average at the start of the
# run, the commits of both worktrees, the image and its digest, and the exact
# command, so a CSV can be reproduced rather than believed.

set -euo pipefail

MP_COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOIRAI_ROOT="$(cd "$MP_COMMON_DIR/../.." && pwd)"
# The Arachne worktree beside this one, by the suffix rule `docker/rig.sh` uses.
_suffix="${MOIRAI_ROOT##*/moirai}"
ARACHNE_ROOT="${ARACHNE_ROOT:-$(cd "$MOIRAI_ROOT/.." && pwd)/arachne${_suffix}}"
# The image the rig builds for this worktree, the same rule.
MP_IMAGE="${MOIRAI_IMAGE:-moirai-json-crdt${_suffix}:test}"
# The default log every replica of a run hosts, pinned as the rig pins it.
MP_LOG_ID="${LOG_ID:-c0113c7ed10c0113c7ed10c0113c7ed1}"

mp_stamp() { date -u +%Y%m%dT%H%M%SZ; }

mp_say() { printf '%s: %s\n' "$(basename "$(dirname "${BASH_SOURCE[1]}")")" "$1" >&2; }

mp_die() { printf '\n%s: %s\n\n' "$(basename "$(dirname "${BASH_SOURCE[1]}")")" "$1" >&2; exit 1; }

mp_load_average() { cut -d' ' -f1-3 /proc/loadavg; }

mp_git_head() { git -C "$1" rev-parse --short=7 HEAD 2>/dev/null || echo unknown; }

mp_git_dirty() {
    if [ -n "$(git -C "$1" status --porcelain 2>/dev/null)" ]; then echo dirty; else echo clean; fi
}

# The manifest lines every measurement shares. Callers append their own.
#   $1 run stamp, $2 the exact command
mp_manifest_common() {
    local stamp="$1" command="$2"
    printf '%-14s%s\n' run "$stamp"
    printf '%-14s%s\n' host "$(uname -srm)"
    printf '%-14s%s\n' cpu "$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //') x$(nproc)"
    printf '%-14s%s\n' memory "$(awk '/MemTotal/ {printf "%.1f GiB", $2/1048576}' /proc/meminfo)"
    printf '%-14s%s\n' load_avg "$(mp_load_average) (1, 5, 15 min at the start of the run)"
    printf '%-14s%s\n' moirai "$(mp_git_head "$MOIRAI_ROOT") ($(mp_git_dirty "$MOIRAI_ROOT")) $MOIRAI_ROOT"
    printf '%-14s%s\n' arachne "$(mp_git_head "$ARACHNE_ROOT") ($(mp_git_dirty "$ARACHNE_ROOT")) $ARACHNE_ROOT"
    printf '%-14s%s\n' rustc "$(rustc --version 2>/dev/null || echo unknown)"
    printf '%-14s%s\n' command "$command"
}

# The image lines, for a measurement that runs containers.
mp_manifest_image() {
    printf '%-14s%s\n' image "$MP_IMAGE"
    printf '%-14s%s\n' "image digest" "$(docker image inspect --format '{{.Id}}' "$MP_IMAGE")"
    printf '%-14s%s\n' "image built" "$(docker image inspect --format '{{.Created}}' "$MP_IMAGE")"
    printf '%-14s%s\n' docker "$(docker version --format '{{.Server.Version}}')"
}

mp_require_docker() {
    docker info >/dev/null 2>&1 || mp_die "the Docker daemon is not reachable"
    docker image inspect "$MP_IMAGE" >/dev/null 2>&1 \
        || mp_die "the image $MP_IMAGE is missing; build it with $MOIRAI_ROOT/docker/rig.sh build"
}

# Every container of the rig or of a harness, running or not.
mp_moirai_containers() {
    docker ps -a --format '{{.Names}}' | grep -E '^moirai' || true
}

# A measurement starts from nothing and leaves nothing: any leftover replica
# would share the machine with the one being timed.
mp_assert_no_moirai_containers() {
    local left
    left="$(mp_moirai_containers)"
    [ -z "$left" ] || mp_die "moirai containers are still up: $(echo "$left" | tr '\n' ' ')"
}

# ---------------------------------------------------------------------------
# Containers with published ports, the `experiments/p2-transfer-size` shape:
# the harness speaks HTTP and the replication protocol from the host, which
# is what lets it poll at loopback latency and read frames off the wire.
# ---------------------------------------------------------------------------

# $1 network, $2 container name, $3 REPLICA_ID, $4 PEERS spec, $5.. extra
# `docker run` arguments
mp_start_node() {
    local net="$1" name="$2" id="$3" peers="$4"
    shift 4
    docker run --detach --name "$name" --network "$net" --network-alias "$name" \
        --env "REPLICA_ID=$id" \
        --env LISTEN_PORT=9001 \
        --env HTTP_PORT=8081 \
        --env "PEERS=$peers" \
        --env "LOG_ID=$MP_LOG_ID" \
        --env METAMODEL_DIR=/metamodels \
        --publish 127.0.0.1::8081 \
        --publish 127.0.0.1::9001 \
        "$@" \
        "$MP_IMAGE" >/dev/null
}

# The last lines of every container on `$1`'s network, for a run that failed.
mp_dump_logs() {
    local name
    for name in $(docker ps -a --filter "network=$1" --format '{{.Names}}' | sort); do
        printf '\n===== %s (%s) =====\n' "$name" "$(docker inspect -f '{{.State.Status}} exit={{.State.ExitCode}}' "$name")" >&2
        docker logs --tail "${2:-30}" "$name" >&2 2>&1
    done
}

mp_http_of() { docker port "$1" 8081/tcp | head -1; }
mp_sync_of() { docker port "$1" 9001/tcp | head -1; }

# Poll to a deadline for a replica's HTTP API.
mp_wait_healthy() {
    local name="$1" deadline=$((SECONDS + ${2:-60})) url
    while :; do
        url="http://$(mp_http_of "$name" 2>/dev/null || true)"
        if [ "$url" != "http://" ] && curl -fsS --max-time 2 "$url/api/health" >/dev/null 2>&1; then
            return 0
        fi
        [ "$SECONDS" -lt "$deadline" ] || mp_die "$name never answered /api/health"
        sleep 0.2
    done
}
