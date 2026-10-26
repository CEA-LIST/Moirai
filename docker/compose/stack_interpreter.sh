#!/usr/bin/env bash
#
# Lifecycle for the split rig: one infrastructure stack, N replica stacks, each
# started and stopped on its own.
#
# `rig.sh` next door starts the measurement rig, which is one Compose project
# and therefore one lifecycle: every replica goes up and comes down with the
# bootnode. That is right for a measured run and wrong for a demonstration,
# where the interesting moment is a replica leaving and rejoining a session
# that never stopped. So this starts the infrastructure as one project and each
# replica as a project of its own, and the only thing that needs arranging for
# that to work is the network — separate projects do not share one, so this
# creates the bridge itself and both YAML files declare it `external`.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOIRAI_ROOT="$(cd "$HERE/../.." && pwd)"

INFRA_FILE="$HERE/infra.yml"
REPLICA_FILE="$HERE/replica.yml"

# A worktree checkout carries its branch in the directory name
# (moirai-model-plane), and `rig.sh` builds `moirai-json-crdt-model-plane:test`
# from it. Same rule here, so both entry points reach for the same image.
_suffix="${MOIRAI_ROOT##*/moirai}"
IMAGE="${MOIRAI_IMAGE:-moirai-json-crdt${_suffix:-}:test}"

# What `build` needs, and only `build`. The image copies `moirai/` and
# `arachne/generated/json_crdt/`, so the context is the directory holding both
# checkouts rather than either one of them, and the two directory names are
# passed in as build args because a worktree carries its branch in its name.
# `rig.sh` reaches the same conclusion by the same route; this arrives there on
# its own, so that a stack can be built without the measurement rig's script
# being present or correct.
BUILD_CONTEXT="$(cd "$MOIRAI_ROOT/.." && pwd)"
if [ -z "${ARACHNE_ROOT:-}" ] && [ -d "$BUILD_CONTEXT/arachne$_suffix" ]; then
    ARACHNE_ROOT="$BUILD_CONTEXT/arachne$_suffix"
fi
ARACHNE_ROOT="${ARACHNE_ROOT:-$BUILD_CONTEXT/arachne}"
DOCKERFILE="$MOIRAI_ROOT/docker/e2e/Dockerfile"
GENERATED_CRATE="$ARACHNE_ROOT/generated/json_crdt"
ECORE="$ARACHNE_ROOT/examples/json.ecore"
FORCE_REBUILD=0

NETWORK="${MOIRAI_STACK_NETWORK:-moirai_stack}"
INFRA_PROJECT="${MOIRAI_INFRA_PROJECT:-moirai-infra}"
SESSION_ID="${SESSION_ID:-stack}"
BOOTNODE_HTTP_PORT="${BOOTNODE_HTTP_PORT:-7000}"

# The transport, resolved as the binaries resolve it (moirai-network's
# transport_config.rs): `--tcp` before the command, or MOIRAI_TRANSPORT `tcp`
# or `zenoh` in this script's environment; with neither, the binaries'
# default, which BINARY_DEFAULT_TRANSPORT mirrors. Whatever it resolves, the
# stack states it in every replica it starts, never leaving it to the image.
#   tcp    infra.yml's `tcp` profile (bootnode, relay); replicas register with
#          the bootnode
#   zenoh  infra.yml's `zenoh` profile (one router); replicas are told
#          ZENOH_ROUTER=tcp/router:7447 and no BOOTNODE_URL
BINARY_DEFAULT_TRANSPORT=zenoh
TRANSPORT_FLAG=
ROUTER_NAME="${MOIRAI_ROUTER_NAME:-router}"
ROUTER_ENDPOINT="tcp/router:7447"

die() {
    printf '\nstack: %s\n' "$1" >&2
    shift
    for line in "$@"; do printf '      %s\n' "$line" >&2; done
    printf '\n' >&2
    exit 1
}
say() { printf 'stack: %s\n' "$1"; }

usage() {
    cat <<'EOF'
stack_interpreter.sh — the Moirai session as an infrastructure stack plus
                       independently started and stopped replica stacks.

The name is the script's own binary default: a replica it starts is the
interpreted `model_node` unless `--bin network_node` says otherwise.

Usage:  ./stack_interpreter.sh [--tcp] COMMAND [options]

  infra up [options]          bootnode and relay under TCP, one router under
                              Zenoh (and optionally a dashboard)
  infra down [--keep-network] stop them; remove the bridge if nothing is on it
  build [--rebuild]           build the replica image the rest of this runs on
  up NAME --port N [options]  start one replica, published on host port N
  down NAME                   stop and remove that replica, nothing else
  restart NAME --port N       down then up, which is the leave/rejoin story
  ls                          what is up, and what the directory sees
  logs NAME|infra [-f]        logs of one replica, or of the infrastructure
  urls                        the reachable URLs
  down-all                    every replica, then the infrastructure
  config infra [options]      print, as JSON, the Compose configuration that
  config up NAME --port N ... `infra up` or `up` with the same arguments and
                              environment would use; starts, creates and
                              changes nothing

The transport: `--tcp` before the command, or MOIRAI_TRANSPORT=tcp or zenoh
in the environment (the two forms of TCP are the same); with neither, the
binaries' default. Under TCP the infrastructure is a bootnode and a relay and
every replica registers with the bootnode (BOOTNODE_URL, default
http://bootnode:7000). Under Zenoh it is one router, MOIRAI_ROUTER_NAME
(default router), on the session bridge with the alias `router` and no
published port; every replica is told ZENOH_ROUTER=tcp/router:7447 and no
BOOTNODE_URL, and links directly with the peers the router introduces. Every
replica's environment states MOIRAI_TRANSPORT. Give the same transport to
`infra up` and to each `up`: `up` refuses a replica on the other transport
than the running infrastructure's, and `ls` and `urls` report the running
infrastructure's transport.

Options for `build`:
      --rebuild          rebuild even if the image already exists
      --image NAME       tag to build (default: $MOIRAI_IMAGE, else
                         moirai-json-crdt<worktree suffix>:test)

  The image is the same one `rig.sh build` produces — same Dockerfile, same
  context, same build args, same default tag — so the measurement rig runs on
  an image built here and this stack runs on one built there. Nothing is
  delegated to `rig.sh`, so neither script needs the other to be present.

  The generated crate at arachne/generated/json_crdt/ is tracked in git and is
  normally already there. This builds from whatever is on disk, and generates
  it only when it is genuinely absent; it never overwrites a crate that exists.
  To regenerate one deliberately, that is `rig.sh generate --force`.

Options for `infra up`:
      --dashboard        also start the dashboard (host port 8090)
      --no-relay         no relay at all: the bootnode advertises none (TCP
                         only; a Zenoh infrastructure has no relay)
      --image NAME       replica image tag (default: $MOIRAI_IMAGE, else
                         moirai-json-crdt<worktree suffix>:test)

Options for `up`:
  -p, --port N           host port for this replica's HTTP API (required)
      --bin BINARY       model_node (interpreted, the default) or network_node
                         (generated from one metamodel)
      --bare             start it holding no metamodel descriptor at all, so a
                         model it joins by id can only be described by the
                         descriptor that arrives inside the log
      --log-id HEX       the replica's default log (32 hex); every replica of a
                         session must agree
      --image NAME       replica image tag

Everything takes SESSION_ID from the environment (default: stack), so a second
session is `SESSION_ID=other stack_interpreter.sh ...` throughout.

Which file to use for what:
  docker-compose.yml   the measurement rig, one project, driven by rig.sh and
                       by experiments/. Untouched by this script.
  infra.yml            bootnode and relay (profile tcp), router (profile
                       zenoh), dashboard — the part that outlives a replica.
  replica.yml          exactly one replica, instantiated once per replica.

Examples:
  ./stack_interpreter.sh infra up --dashboard
  ./stack_interpreter.sh up alice --port 8081
  ./stack_interpreter.sh up bob   --port 8082
  ./stack_interpreter.sh ls
  ./stack_interpreter.sh down bob          # alice keeps serving
  ./stack_interpreter.sh up bob --port 8082  # rejoins and converges
  ./stack_interpreter.sh down-all
  ./stack_interpreter.sh --tcp infra up    # the bootnode and the relay
  ./stack_interpreter.sh --tcp up alice --port 8081
EOF
}

# --------------------------------------------------------------------------
# Preflight and the shared bridge.
# --------------------------------------------------------------------------

preflight_docker() {
    command -v docker >/dev/null 2>&1 || die "docker is not on PATH."
    docker info >/dev/null 2>&1 || die \
        "the Docker daemon is not reachable." \
        "If you were added to the 'docker' group in this session, try: sg docker -c \"$0 ...\""
    docker compose version >/dev/null 2>&1 \
        || die "the 'docker compose' plugin is not available (v2 or newer is required)."
}

preflight() {
    preflight_docker
    [ -f "$INFRA_FILE" ] || die "infra.yml not found at $INFRA_FILE"
    [ -f "$REPLICA_FILE" ] || die "replica.yml not found at $REPLICA_FILE"
}

# TRANSPORT from `--tcp` (TRANSPORT_FLAG) or MOIRAI_TRANSPORT, else the
# binaries' default; the two forms of TCP give the same result.
resolve_transport() {
    local named="${MOIRAI_TRANSPORT:-}"
    case "$named" in
        ''|tcp|zenoh) ;;
        *) die "MOIRAI_TRANSPORT takes tcp or zenoh, got '$named'" ;;
    esac
    if [ -n "$TRANSPORT_FLAG" ] && [ -n "$named" ] && [ "$named" != "$TRANSPORT_FLAG" ]; then
        die "--tcp and MOIRAI_TRANSPORT=$named disagree; give one of them"
    fi
    TRANSPORT="${TRANSPORT_FLAG:-${named:-$BINARY_DEFAULT_TRANSPORT}}"
}

require_image() {
    image_exists || die \
        "the replica image '$IMAGE' does not exist." \
        "Build it here:" \
        "    $HERE/$(basename "$0") build" \
        "or pass --image / set MOIRAI_IMAGE to one you have."
    say_image
}

# The one thing separate projects cannot arrange between themselves. Created
# here rather than by either compose file, because whichever project created it
# would also destroy it, and then stopping the infrastructure would take every
# replica's network with it.
ensure_network() {
    if ! docker network inspect "$NETWORK" >/dev/null 2>&1; then
        say "creating the session bridge '$NETWORK'"
        docker network create --driver bridge "$NETWORK" >/dev/null || die \
            "could not create the network '$NETWORK'." \
            "If this says the address pools are exhausted, that is Docker's" \
            "default pool and not a regression: 'docker network prune' and retry."
    fi
}

network_members() {
    docker network inspect "$NETWORK" \
        --format '{{range .Containers}}{{.Name}} {{end}}' 2>/dev/null || true
}

remove_network_if_empty() {
    docker network inspect "$NETWORK" >/dev/null 2>&1 || return 0
    local members
    members="$(network_members)"
    members="$(printf '%s' "$members" | tr -s ' ' | sed 's/^ *//;s/ *$//')"
    if [ -n "$members" ]; then
        say "leaving '$NETWORK' in place: still attached — $members"
        return 0
    fi
    docker network rm "$NETWORK" >/dev/null && say "removed the session bridge '$NETWORK'"
}

# --------------------------------------------------------------------------
# The image.
#
# Cam ran a rig one commit stale without being told, so every path that starts
# a container says which image it is and how old it is, in one line.
# --------------------------------------------------------------------------

image_exists() { docker image inspect "$IMAGE" >/dev/null 2>&1; }

human_age() {
    local secs=$1
    if   [ "$secs" -lt 90 ];     then printf '%ds' "$secs"
    elif [ "$secs" -lt 5400 ];   then printf '%dmin' "$((secs / 60))"
    elif [ "$secs" -lt 172800 ]; then printf '%dh' "$((secs / 3600))"
    else                              printf '%dd' "$((secs / 86400))"
    fi
}

say_image() {
    local created epoch id
    created="$(docker image inspect -f '{{.Created}}' "$IMAGE" 2>/dev/null)" || {
        say "image $IMAGE (age unknown)"
        return
    }
    id="$(docker image inspect -f '{{.Id}}' "$IMAGE" 2>/dev/null | cut -c8-19)"
    if epoch="$(date -d "$created" +%s 2>/dev/null)" && [ -n "$epoch" ]; then
        say "image $IMAGE ($id) built $(human_age "$(( $(date +%s) - epoch ))") ago"
    else
        say "image $IMAGE ($id) built $created"
    fi
}

preflight_context() {
    [ -f "$DOCKERFILE" ] || die \
        "the replica Dockerfile was not found at $DOCKERFILE" \
        "This script builds from the moirai checkout it lives in; if that path" \
        "is wrong, the checkout is not laid out the way the image expects."

    [ -d "$ARACHNE_ROOT" ] || die \
        "the arachne checkout was not found at $ARACHNE_ROOT" \
        "The image copies from both checkouts, so they must sit side by side" \
        "under one directory, which is then the build context:" \
        "    $BUILD_CONTEXT/$(basename "$MOIRAI_ROOT")/" \
        "    $BUILD_CONTEXT/arachne$_suffix/   (or .../arachne/)" \
        "Set ARACHNE_ROOT to point somewhere else."

    [ -d "$ARACHNE_ROOT/examples" ] || die \
        "$ARACHNE_ROOT has no examples/ directory." \
        "The image copies it in as the metamodel descriptors a replica serves," \
        "so this is either not an arachne checkout or not a complete one."
}

# The generated crate is tracked in git, which makes writing over it a
# destructive act rather than a cache refresh. So this generates it only when
# it is genuinely absent, and has no flag that would overwrite one that is
# there — deliberately narrower than `rig.sh`, which has --regenerate and
# guards it with --allow-dirty. A stack has no reason to want that, and the
# safety is then structural rather than a matter of not typing the flag.
crate_is_present() {
    [ -f "$GENERATED_CRATE/Cargo.toml" ] && [ -f "$GENERATED_CRATE/src/lib.rs" ]
}

ensure_crate() {
    if crate_is_present; then
        if git -C "$ARACHNE_ROOT" rev-parse --git-dir >/dev/null 2>&1 \
            && [ -n "$(git -C "$ARACHNE_ROOT" status --porcelain -- generated/json_crdt 2>/dev/null)" ]; then
            say "generated crate at $GENERATED_CRATE has uncommitted changes; building from them as they are"
        fi
        return
    fi

    command -v cargo >/dev/null 2>&1 || die \
        "the generated CRDT crate is missing and cargo is not on PATH." \
        "Expected it at $GENERATED_CRATE — it is tracked in git, so a checkout" \
        "normally has it. Either restore it, or install cargo so it can be" \
        "generated from $ECORE."
    [ -f "$ECORE" ] || die \
        "the generated CRDT crate is missing and so is the metamodel it comes from." \
        "Looked for the crate at $GENERATED_CRATE and the source at $ECORE."

    say "generated crate is absent; generating it into $GENERATED_CRATE"
    ( cd "$ARACHNE_ROOT" \
        && cargo run -q -p arachne-cli -- generate examples/json.ecore \
            -o generated/json_crdt -p json-crdt ) \
        || die "generating the CRDT crate failed; see the cargo output above."
    crate_is_present || die \
        "the generator reported success but $GENERATED_CRATE still looks incomplete."
}

build_image() {
    say "building $IMAGE from $BUILD_CONTEXT (the context holds both checkouts)"
    docker build \
        -f "$DOCKERFILE" \
        -t "$IMAGE" \
        --build-arg MOIRAI_DIR="$(basename "$MOIRAI_ROOT")" \
        --build-arg ARACHNE_DIR="$(basename "$ARACHNE_ROOT")" \
        "$BUILD_CONTEXT" \
        || die "the image build failed; see the docker output above."
}

cmd_build() {
    while [ $# -gt 0 ]; do
        case "$1" in
            --rebuild) FORCE_REBUILD=1 ;;
            --image) IMAGE="$2"; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option for 'build': $1" ;;
        esac
        shift
    done
    preflight_docker
    preflight_context
    if [ "$FORCE_REBUILD" -eq 0 ] && image_exists; then
        say_image
        say "already built (pass --rebuild to build it again)"
        return
    fi
    ensure_crate
    build_image
    say_image
}

# --------------------------------------------------------------------------
# Compose invocations. Every variable the two files read is passed explicitly,
# so the behaviour does not depend on what happens to be exported.
# --------------------------------------------------------------------------

# The profiles are named by the caller (`--profile "$TRANSPORT"` and, with
# --dashboard, `--profile dashboard`); COMPOSE_PROFILES is emptied so that
# one exported on the host cannot add the other transport's services.
infra_compose() {
    MOIRAI_IMAGE="$IMAGE" \
    MOIRAI_INFRA_PROJECT="$INFRA_PROJECT" \
    MOIRAI_STACK_NETWORK="$NETWORK" \
    MOIRAI_ROUTER_NAME="$ROUTER_NAME" \
    BOOTNODE_HTTP_PORT="$BOOTNODE_HTTP_PORT" \
    COMPOSE_PROFILES= \
    docker compose -f "$INFRA_FILE" "$@"
}

replica_project() { printf 'moirai-replica-%s' "$1"; }

# The transport's own settings go in explicitly, whatever the host exported:
# under TCP the bootnode (BOOTNODE_URL from the host if set, as before) and no
# router; under Zenoh the router and an empty BOOTNODE_URL, which every node
# reads as unset.
replica_compose() {
    local name=$1 port=$2 bootnode router
    shift 2
    if [ "$TRANSPORT" = zenoh ]; then
        bootnode=""
        router="$ROUTER_ENDPOINT"
    else
        bootnode="${BOOTNODE_URL:-http://bootnode:7000}"
        router=""
    fi
    MOIRAI_TRANSPORT="$TRANSPORT" \
    BOOTNODE_URL="$bootnode" \
    ZENOH_ROUTER="$router" \
    REPLICA_NAME="$name" \
    REPLICA_PROJECT="$(replica_project "$name")" \
    REPLICA_HTTP_PORT="$port" \
    REPLICA_BIN="${REPLICA_BIN:-model_node}" \
    MOIRAI_IMAGE="$IMAGE" \
    MOIRAI_STACK_NETWORK="$NETWORK" \
    SESSION_ID="$SESSION_ID" \
    LOG_ID="${LOG_ID:-1a7e9be511a7e9be511a7e9be511a7e9}" \
    METAMODEL_DIR="${METAMODEL_DIR-/metamodels}" \
    docker compose -f "$REPLICA_FILE" "$@"
}

wait_healthy() {
    local container=$1 secs=${2:-120} deadline
    deadline=$(( $(date +%s) + secs ))
    while :; do
        case "$(docker inspect -f '{{.State.Health.Status}}' "$container" 2>/dev/null)" in
            healthy) return 0 ;;
            '') ;;
        esac
        [ "$(date +%s)" -lt "$deadline" ] || return 1
        sleep 1
    done
}

# --------------------------------------------------------------------------
# infra
# --------------------------------------------------------------------------

# The options of `infra up` (and `config infra`), into INFRA_PROFILES: the
# transport's profile, then the dashboard's when asked for.
INFRA_PROFILES=()
parse_infra_up() {
    INFRA_PROFILES=(--profile "$TRANSPORT")
    while [ $# -gt 0 ]; do
        case "$1" in
            --dashboard) INFRA_PROFILES+=(--profile dashboard) ;;
            --no-relay)
                [ "$TRANSPORT" = tcp ] \
                    || die "--no-relay is for the TCP infrastructure; under Zenoh there is no relay."
                export RELAY_ADDR=""
                ;;
            --image) IMAGE="$2"; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option for 'infra up': $1" ;;
        esac
        shift
    done
}

cmd_infra_up() {
    parse_infra_up "$@"
    preflight
    require_image
    ensure_network
    say "starting the infrastructure (project $INFRA_PROJECT, image $IMAGE, transport $TRANSPORT)"
    infra_compose "${INFRA_PROFILES[@]}" up -d
    if [ "$TRANSPORT" = zenoh ]; then
        wait_healthy "$ROUTER_NAME" \
            || die "the router never started listening; try: $0 logs infra"
    else
        wait_healthy "${MOIRAI_BOOTNODE_NAME:-bootnode}" \
            || die "the bootnode never became healthy; try: $0 logs infra"
        if [ "${RELAY_ADDR-relay:7100}" != "" ]; then
            wait_healthy "${MOIRAI_RELAY_NAME:-relay}" \
                || die "the relay never became healthy; try: $0 logs infra"
        fi
    fi
    cmd_urls
}

cmd_infra_down() {
    local keep=0
    while [ $# -gt 0 ]; do
        case "$1" in
            --keep-network) keep=1 ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option for 'infra down': $1" ;;
        esac
        shift
    done
    preflight
    # Every profile, so the router, the bootnode and the relay all go,
    # whichever transport started them.
    infra_compose --profile '*' down --remove-orphans
    [ "$keep" -eq 1 ] || remove_network_if_empty
}

# --------------------------------------------------------------------------
# replicas
# --------------------------------------------------------------------------

# The arguments of `up` (and `config up`) into UP_NAME and UP_PORT. Reads
# arguments only; starts, creates and changes nothing.
UP_NAME=
UP_PORT=
parse_up() {
    [ $# -gt 0 ] || die "usage: $0 up NAME --port N"
    UP_NAME=$1
    shift
    case "$UP_NAME" in
        -*) die "the replica name comes first: $0 up NAME --port N" ;;
    esac
    while [ $# -gt 0 ]; do
        case "$1" in
            -p|--port) UP_PORT="$2"; shift ;;
            --bin) REPLICA_BIN="$2"; shift ;;
            --bare) METAMODEL_DIR="" ;;
            --log-id) LOG_ID="$2"; shift ;;
            --image) IMAGE="$2"; shift ;;
            -h|--help) usage; exit 0 ;;
            *) die "unknown option for 'up': $1" ;;
        esac
        shift
    done
    [ -n "$UP_PORT" ] || die "a replica needs a published HTTP port: $0 up $UP_NAME --port 8081"
    case "${REPLICA_BIN:-model_node}" in
        model_node|network_node) ;;
        *) die "--bin takes model_node or network_node, got '${REPLICA_BIN}'" ;;
    esac
}

cmd_up() {
    parse_up "$@"
    local name=$UP_NAME port=$UP_PORT
    preflight
    require_image
    # A duplicate name is the failure the rig's `node` service had to design
    # around: two replicas with one id, upserted onto a single directory entry.
    # Here the name is chosen by hand, so it is checked by hand.
    if docker container inspect "$name" >/dev/null 2>&1; then
        die "a container named '$name' already exists." \
            "Stop it first: $0 down $name"
    fi
    docker network inspect "$NETWORK" >/dev/null 2>&1 || die \
        "the session bridge '$NETWORK' does not exist; start the infrastructure first:" \
        "    $0 infra up"

    # A replica on the other transport than the infrastructure's finds no
    # peer: under TCP it would ask a bootnode that is not there, under Zenoh a
    # router that is not there. Refused here rather than left to time out.
    local running
    running="$(infra_transport)"
    if [ "$running" = zenoh ] && [ "$TRANSPORT" = tcp ]; then
        die "the infrastructure ($INFRA_PROJECT) is the Zenoh router, and '$name' would be told TCP." \
            "Start it on Zenoh: drop --tcp and MOIRAI_TRANSPORT=tcp."
    fi
    if [ "$running" = tcp ] && [ "$TRANSPORT" = zenoh ]; then
        die "the infrastructure ($INFRA_PROJECT) is the TCP bootnode and relay, and '$name' would be told Zenoh." \
            "Start it on TCP: $0 --tcp up $name --port $port"
    fi

    say "starting replica '$name' (${REPLICA_BIN:-model_node}) on host port $port, transport $TRANSPORT"
    replica_compose "$name" "$port" up -d
    wait_healthy "$name" || die "'$name' never became healthy; try: $0 logs $name"
    say "$name is up: http://localhost:$port/api/health"
}

cmd_down() {
    [ $# -ge 1 ] || die "usage: $0 down NAME"
    local name=$1
    preflight
    # Ask it to leave before killing it. Without this the directory keeps
    # advertising a replica that is gone until BOOTNODE_TTL_SECS expires, and
    # every surviving replica keeps dialling it — harmless, but it makes `ls`
    # lie for half a minute, which in a demonstration is the wrong half minute.
    # A replica on Zenoh has no directory to leave and may not answer.
    if docker container inspect "$name" >/dev/null 2>&1; then
        docker exec "$name" curl -fsS -X POST http://localhost:8081/api/leave \
            >/dev/null 2>&1 || say "'$name' did not answer /api/leave; stopping it anyway"
    fi
    replica_compose "$name" 0 down --remove-orphans
    say "$name is down"
}

cmd_restart() {
    [ $# -ge 1 ] || die "usage: $0 restart NAME --port N"
    local name=$1
    shift
    cmd_down "$name"
    cmd_up "$name" "$@"
}

cmd_down_all() {
    preflight
    local name
    for name in $(replica_names); do
        cmd_down "$name"
    done
    cmd_infra_down
}

# Replica containers are found by the label Compose stamps on them, so this
# sees every replica project whether or not this shell started it.
replica_names() {
    docker ps -a --filter 'label=com.docker.compose.service=replica' \
        --format '{{.Names}}' 2>/dev/null || true
}

# --------------------------------------------------------------------------
# reporting
# --------------------------------------------------------------------------

# The transport of the infrastructure already in the project, read from its
# containers in any state (a stopped router still names Zenoh): zenoh when it
# holds the router, tcp when it holds the bootnode or the relay, both when it
# holds the two, nothing when it holds neither.
infra_transport() {
    local services z=0 t=0
    services="$(docker ps -a --filter "label=com.docker.compose.project=$INFRA_PROJECT" \
        --format '{{.Label "com.docker.compose.service"}}' 2>/dev/null)" || true
    grep -qx router <<< "$services" && z=1
    grep -qxE 'bootnode|relay' <<< "$services" && t=1
    case "$z$t" in
        10) printf 'zenoh' ;;
        01) printf 'tcp' ;;
        11) printf 'both' ;;
    esac
}

# What `ls` and `urls` report: the transport of the infrastructure that is up,
# whatever this call resolved; the resolved one only when none is up.
shown_transport() {
    local running
    running="$(infra_transport)"
    printf '%s' "${running:-$TRANSPORT}"
}

cmd_ls() {
    preflight
    printf '\nnetwork: %s' "$NETWORK"
    if docker network inspect "$NETWORK" >/dev/null 2>&1; then printf ' (up)\n'; else printf ' (absent)\n'; fi

    printf '\ninfrastructure (%s):\n' "$INFRA_PROJECT"
    infra_compose --profile '*' ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' 2>/dev/null \
        | sed 's/^/  /'

    printf '\nreplicas:\n'
    local found=0 name
    for name in $(replica_names); do
        found=1
        printf '  %-16s %-28s %s\n' \
            "$name" \
            "$(docker inspect -f '{{.State.Status}} ({{if .State.Health}}{{.State.Health.Status}}{{else}}-{{end}})' "$name")" \
            "$(docker inspect -f '{{range $p, $c := .NetworkSettings.Ports}}{{range $c}}{{.HostPort}} {{end}}{{end}}' "$name")"
    done
    [ "$found" -eq 1 ] || printf '  (none)\n'

    local shown roster
    shown="$(shown_transport)"
    printf '\ndirectory (session %s, transport %s):\n' "$SESSION_ID" "$shown"
    if [ "$shown" = zenoh ] || [ "$shown" = both ]; then
        printf '  (none under Zenoh: the router %s introduces the replicas)\n' "$ROUTER_NAME"
    fi
    if [ "$shown" = tcp ] || [ "$shown" = both ]; then
        if roster="$(curl -fsS --max-time 3 "http://localhost:$BOOTNODE_HTTP_PORT/session/$SESSION_ID/peers" 2>/dev/null)"; then
            printf '%s' "$roster" | sed 's/},{/}\n{/g' | sed 's/^/  /'
            printf '\n'
        else
            printf '  (the bootnode did not answer on port %s)\n' "$BOOTNODE_HTTP_PORT"
        fi
    fi
    printf '\n'
}

cmd_urls() {
    local shown
    shown="$(shown_transport)"
    printf '\n'
    if [ "$shown" = zenoh ] || [ "$shown" = both ]; then
        printf '  router     %s on %s (%s, no published port)\n' "$ROUTER_ENDPOINT" "$NETWORK" "$ROUTER_NAME"
    fi
    if [ "$shown" = tcp ] || [ "$shown" = both ]; then
        printf '  bootnode   http://localhost:%s/session/%s/peers\n' "$BOOTNODE_HTTP_PORT" "$SESSION_ID"
    fi
    if docker container inspect "${MOIRAI_DASHBOARD_NAME:-dashboard}" >/dev/null 2>&1; then
        printf '  dashboard  http://localhost:%s/\n' "${DASHBOARD_PORT:-8090}"
    fi
    local name
    for name in $(replica_names); do
        printf '  %-10s http://localhost:%s/api/models\n' "$name" \
            "$(docker inspect -f '{{range $p, $c := .NetworkSettings.Ports}}{{range $c}}{{.HostPort}}{{end}}{{end}}' "$name")"
    done
    printf '\n'
}

cmd_logs() {
    [ $# -ge 1 ] || die "usage: $0 logs NAME|infra [-f]"
    local target=$1
    shift
    preflight
    if [ "$target" = "infra" ]; then
        # Every profile: Compose leaves the services of a profile not named
        # out of `logs`, and the router, the bootnode and the relay each sit
        # behind one.
        infra_compose --profile '*' logs "$@"
    else
        docker logs "$@" "$target"
    fi
}

# --------------------------------------------------------------------------
# config: the dry run
#
# What `infra up` or `up` with the same arguments and environment would hand
# Compose, printed as `docker compose config --format json` prints it: the
# same file, profiles and variables, through the same functions. Only the JSON
# goes to stdout. It starts, creates and changes nothing, and needs neither the
# image nor the session bridge to exist.
# --------------------------------------------------------------------------

cmd_config() {
    [ $# -gt 0 ] || die "usage: $0 config infra [options] | config up NAME --port N [options]"
    local what=$1
    shift
    case "$what" in
        infra)
            parse_infra_up "$@"
            preflight
            infra_compose "${INFRA_PROFILES[@]}" config --format json
            ;;
        up)
            parse_up "$@"
            preflight
            replica_compose "$UP_NAME" "$UP_PORT" config --format json
            ;;
        *) die "unknown 'config' subcommand: $what (infra or up)" ;;
    esac
}

# --------------------------------------------------------------------------

[ $# -gt 0 ] || { usage; exit 0; }
if [ "$1" = "--tcp" ]; then
    TRANSPORT_FLAG=tcp
    shift
    [ $# -gt 0 ] || die "usage: $0 --tcp COMMAND [options]"
fi
resolve_transport
command=$1
shift
case "$command" in
    infra)
        [ $# -gt 0 ] || die "usage: $0 infra up|down"
        sub=$1; shift
        case "$sub" in
            up) cmd_infra_up "$@" ;;
            down) cmd_infra_down "$@" ;;
            *) die "unknown 'infra' subcommand: $sub" ;;
        esac
        ;;
    build) cmd_build "$@" ;;
    up) cmd_up "$@" ;;
    down) cmd_down "$@" ;;
    restart) cmd_restart "$@" ;;
    down-all) cmd_down_all "$@" ;;
    ls|status) cmd_ls "$@" ;;
    urls) cmd_urls "$@" ;;
    logs) cmd_logs "$@" ;;
    config) cmd_config "$@" ;;
    -h|--help|help) usage ;;
    *) die "unknown command: $command" "Try: $0 --help" ;;
esac
