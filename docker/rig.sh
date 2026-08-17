#!/usr/bin/env bash
#
# Single entry point for the Compose rig.
#
# The rig has three moving parts that have to agree with each other: the replica
# image, the Arachne-generated `json_crdt` crate the image builds from, and the
# Compose stack itself. Getting them to agree by hand means knowing that the
# image is built from a context two levels up, because it copies both checkouts.
# That is the kind of thing that lives in someone's shell history rather than in
# the repository, so it lives here instead.

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MOIRAI_ROOT="$(cd "$HERE/.." && pwd)"
# The image copies `moirai/` and `arachne/generated/json_crdt/`, so the build
# context is the directory holding both checkouts, not either checkout.
BUILD_CONTEXT="$(cd "$MOIRAI_ROOT/.." && pwd)"
ARACHNE_ROOT="${ARACHNE_ROOT:-$BUILD_CONTEXT/arachne}"

COMPOSE_FILE="$HERE/compose/docker-compose.yml"
DOCKERFILE="$HERE/e2e/Dockerfile"

IMAGE="${MOIRAI_IMAGE:-moirai-json-crdt:test}"
GENERATED_CRATE="$ARACHNE_ROOT/generated/json_crdt"
ECORE="$ARACHNE_ROOT/examples/json.ecore"

NODES=3
# The rig is nearly always wanted whole: replicas, a dashboard to watch them
# converge, and a driver producing the edits that make the dashboard worth
# watching. So that is what a bare `rig.sh` gives you, and the flags take parts
# away rather than adding them back.
WITH_DASHBOARD=1
WITH_LOAD=1
FORCE_REBUILD=0
FORCE_REGENERATE=0
ALLOW_DIRTY_REGEN=0
NO_RELAY=0
NO_REPORTING=0
WAIT_SECS=120

die() {
    printf '\nrig: %s\n' "$1" >&2
    shift
    for line in "$@"; do printf '     %s\n' "$line" >&2; done
    printf '\n' >&2
    exit 1
}

say() { printf 'rig: %s\n' "$1"; }

usage() {
    cat <<'EOF'
rig.sh — bring the Moirai Compose rig up and down as one operation.

  rig.sh            start everything: replicas, relay, dashboard, driver
  rig.sh down       stop it and remove its volumes and networks

Usage:
  rig.sh [up] [options]      preflight, build/generate as needed, start the stack
  rig.sh down [options]      stop the stack and remove its volumes and networks
  rig.sh status              what is running, and whether it is healthy
  rig.sh urls                print the reachable URLs
  rig.sh logs [service]      follow logs (default: all services)
  rig.sh build [--rebuild]   build the replica image only
  rig.sh generate [--force]  generate the CRDT crate only

Options for `up`:
  -n, --nodes N          number of `node` replicas (default: 3)
      --no-dashboard     do not start the dashboard
      --no-load          do not start the load driver (a quiet, idle rig)
      --rebuild          rebuild the replica image even if it already exists
      --regenerate       regenerate the CRDT crate even if it is already present
      --allow-dirty      permit --regenerate to overwrite locally modified files
      --no-relay         run with no relay at all (the islander stays unreachable)
      --no-reporting     replicas start no reporting thread (zero overhead rig)
      --image NAME       replica image tag (default: $MOIRAI_IMAGE or
                         moirai-json-crdt:test)
      --wait SECS        how long to wait for health before giving up (default: 120)

Options for `down`:
      --keep-volumes     leave the observer's CSV volume in place

Notes:
  The generated crate at arachne/generated/json_crdt/ is tracked in git. It is
  generated only when genuinely absent, or when you pass --regenerate; a
  --regenerate over locally modified files refuses unless you add --allow-dirty.

Examples:
  rig.sh                     the whole rig, dashboard and driver included
  rig.sh --nodes 5           the same, with five replicas
  rig.sh --no-load           up and idle, so you can drive it by hand
  rig.sh down
EOF
}

# --------------------------------------------------------------------------
# Preflight. Everything that can be checked without side effects is checked
# before anything starts, so a missing dependency is one message rather than a
# half-built stack.
# --------------------------------------------------------------------------

preflight_docker() {
    command -v docker >/dev/null 2>&1 \
        || die "docker is not on PATH." "Install Docker Engine, or put the client on PATH."

    docker info >/dev/null 2>&1 || die \
        "the Docker daemon is not reachable." \
        "Either it is not running, or this shell's group credentials are stale." \
        "If you were added to the 'docker' group in this session, try:" \
        "    sg docker -c \"$0 $*\""

    docker compose version >/dev/null 2>&1 \
        || die "the 'docker compose' plugin is not available (v2 or newer is required)."

    [ -f "$COMPOSE_FILE" ] || die "compose file not found at $COMPOSE_FILE"
}

preflight_checkouts() {
    [ -f "$DOCKERFILE" ] || die "Dockerfile not found at $DOCKERFILE"

    [ -d "$ARACHNE_ROOT" ] || die \
        "the arachne checkout was not found at $ARACHNE_ROOT" \
        "The replica image copies both checkouts, so they must sit side by side:" \
        "    $BUILD_CONTEXT/moirai/" \
        "    $BUILD_CONTEXT/arachne/" \
        "Set ARACHNE_ROOT to override where arachne is looked for."
}

# --------------------------------------------------------------------------
# The generated crate.
#
# This is tracked in git, which makes regeneration a destructive operation
# rather than a cache refresh. So: generate when it is genuinely missing, and
# otherwise only on an explicit request that is refused if it would discard
# uncommitted work.
# --------------------------------------------------------------------------

crate_is_present() {
    [ -f "$GENERATED_CRATE/Cargo.toml" ] && [ -f "$GENERATED_CRATE/src/lib.rs" ]
}

crate_is_dirty() {
    git -C "$ARACHNE_ROOT" rev-parse --git-dir >/dev/null 2>&1 || return 1
    [ -n "$(git -C "$ARACHNE_ROOT" status --porcelain -- generated/json_crdt 2>/dev/null)" ]
}

generate_crate() {
    command -v cargo >/dev/null 2>&1 \
        || die "cargo is not on PATH, and the CRDT crate needs generating."

    [ -f "$ECORE" ] || die "the source metamodel is missing at $ECORE"

    say "generating the CRDT crate into $GENERATED_CRATE"
    ( cd "$ARACHNE_ROOT" \
        && cargo run -q -p arachne-cli -- generate examples/json.ecore \
            -o generated/json_crdt -p json-crdt ) \
        || die "generating the CRDT crate failed; see the cargo output above."

    crate_is_present || die \
        "the generator reported success but $GENERATED_CRATE still looks incomplete."
}

ensure_crate() {
    if [ "$FORCE_REGENERATE" -eq 1 ]; then
        if crate_is_dirty && [ "$ALLOW_DIRTY_REGEN" -eq 0 ]; then
            die \
                "refusing to regenerate over local modifications." \
                "generated/json_crdt/ is tracked and currently has uncommitted changes:" \
                "$(git -C "$ARACHNE_ROOT" status --porcelain -- generated/json_crdt | sed 's/^/         /')" \
                "Commit or discard them first, or pass --allow-dirty to overwrite."
        fi
        generate_crate
        return
    fi

    if crate_is_present; then
        say "CRDT crate present at $GENERATED_CRATE (pass --regenerate to rebuild it)"
    else
        say "CRDT crate is missing"
        generate_crate
    fi
}

# --------------------------------------------------------------------------
# The image.
# --------------------------------------------------------------------------

image_exists() { docker image inspect "$IMAGE" >/dev/null 2>&1; }

build_image() {
    say "building $IMAGE from $BUILD_CONTEXT (context holds both checkouts)"
    docker build \
        -f "$DOCKERFILE" \
        -t "$IMAGE" \
        "$BUILD_CONTEXT" \
        || die "the image build failed; see the docker output above."
}

ensure_image() {
    if [ "$FORCE_REBUILD" -eq 1 ]; then
        build_image
    elif image_exists; then
        say "image $IMAGE present (pass --rebuild to rebuild it)"
    else
        say "image $IMAGE is missing"
        build_image
    fi
}

# --------------------------------------------------------------------------
# The stack.
# --------------------------------------------------------------------------

# Profile flags must be repeated on `down`, or Compose leaves profile services
# running and the next `up` inherits them.
compose() {
    ( cd "$HERE/compose" \
        && MOIRAI_IMAGE="$IMAGE" docker compose --profile load --profile dashboard "$@" )
}

# `up` deliberately does not enable profiles wholesale — the services are named
# explicitly so that --dashboard and --load are additive rather than all-or-nothing.
compose_up() {
    local services=(bootnode relay node islander observer)
    [ "$WITH_LOAD" -eq 1 ] && services+=(driver)
    [ "$WITH_DASHBOARD" -eq 1 ] && services+=(dashboard)

    ( cd "$HERE/compose" \
        && MOIRAI_IMAGE="$IMAGE" \
           docker compose --profile load --profile dashboard \
           up -d --scale "node=$NODES" "${services[@]}" )
}

health_of() {
    local cid="$1"
    docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "$cid" 2>/dev/null || echo "gone"
}

wait_for_health() {
    local service="$1" deadline=$((SECONDS + WAIT_SECS)) cid status
    while [ "$SECONDS" -lt "$deadline" ]; do
        cid="$(compose ps -q "$service" 2>/dev/null | head -n 1)"
        if [ -n "$cid" ]; then
            status="$(health_of "$cid")"
            case "$status" in
                healthy|running) return 0 ;;
                exited|dead) return 1 ;;
            esac
        fi
        sleep 1
    done
    return 1
}

print_urls() {
    local port="${DASHBOARD_PORT:-8090}"
    printf '\n'
    if [ "$WITH_DASHBOARD" -eq 1 ] || compose ps --services --filter status=running 2>/dev/null | grep -qx dashboard; then
        printf '  dashboard   http://localhost:%s\n' "$port"
    else
        printf '  dashboard   not running (started by default; %s without --no-dashboard)\n' "$(basename "$0")"
    fi
    # Only the dashboard publishes a host port; the replicas are reached through
    # Compose, which keeps the rig's port usage to exactly one.
    printf '  replicas    docker compose -f %s exec -T node curl -s localhost:8081/api/metrics\n' \
        "$COMPOSE_FILE"
    printf '  observer    %s\n' "$HERE/compose/runs/metrics.csv"
    printf '\n'
}

cmd_up() {
    preflight_docker "$@"
    preflight_checkouts
    ensure_crate
    ensure_image

    [ "$NO_RELAY" -eq 1 ] && export RELAY_ADDR=
    [ "$NO_REPORTING" -eq 1 ] && export DASHBOARD_URL=

    say "starting the stack with $NODES replica(s)"
    if ! compose_up; then
        say "start failed — tearing down so the next attempt begins from nothing"
        compose down -v --remove-orphans >/dev/null 2>&1 || true
        die "the stack failed to start; see the compose output above."
    fi

    for svc in bootnode relay; do
        wait_for_health "$svc" || {
            say "$svc did not become healthy within ${WAIT_SECS}s"
            compose logs --tail 40 "$svc" || true
            compose down -v --remove-orphans >/dev/null 2>&1 || true
            die "the stack did not come up cleanly and has been torn down."
        }
    done

    if [ "$WITH_DASHBOARD" -eq 1 ]; then
        wait_for_health dashboard || {
            say "the dashboard did not become healthy within ${WAIT_SECS}s"
            compose logs --tail 40 dashboard || true
            die "the stack is up but the dashboard is not; leaving it running to inspect."
        }
    fi

    say "up"
    print_urls
}

cmd_down() {
    preflight_docker "$@"
    local args=(down --remove-orphans)
    [ "${KEEP_VOLUMES:-0}" -eq 1 ] || args+=(-v)
    say "tearing down"
    compose "${args[@]}" || die "teardown reported an error; see the output above."
    say "down"
}

cmd_status() {
    preflight_docker "$@"
    compose ps
}

cmd_logs() {
    preflight_docker "$@"
    if [ "$#" -gt 0 ]; then compose logs -f "$@"; else compose logs -f; fi
}

# --------------------------------------------------------------------------

main() {
    local command
    case "${1:-}" in
        -h|--help|help) usage; exit 0 ;;
        # A bare `rig.sh`, or one that leads with an option, means `up`. That is
        # what is wanted nearly every time, so it should not have to be said.
        # Nothing is shifted here: the option still has to reach the parser.
        ""|-*)          command=up ;;
        *)              command="$1"; shift ;;
    esac

    local rest=()
    while [ "$#" -gt 0 ]; do
        case "$1" in
            -n|--nodes)
                [ "$#" -ge 2 ] || die "--nodes needs a value"
                NODES="$2"
                case "$NODES" in
                    ''|*[!0-9]*) die "--nodes takes a positive integer, got '$NODES'" ;;
                esac
                [ "$NODES" -ge 1 ] || die "--nodes must be at least 1"
                shift 2 ;;
            --no-dashboard) WITH_DASHBOARD=0; shift ;;
            --no-load)      WITH_LOAD=0; shift ;;
            # Both are the default now; accepted so that older invocations and
            # anything written down elsewhere keep working.
            --dashboard)    WITH_DASHBOARD=1; shift ;;
            --load)         WITH_LOAD=1; shift ;;
            --rebuild)      FORCE_REBUILD=1; shift ;;
            --regenerate|--force) FORCE_REGENERATE=1; shift ;;
            --allow-dirty)  ALLOW_DIRTY_REGEN=1; shift ;;
            --no-relay)     NO_RELAY=1; shift ;;
            --no-reporting) NO_REPORTING=1; shift ;;
            --keep-volumes) KEEP_VOLUMES=1; shift ;;
            --image)
                [ "$#" -ge 2 ] || die "--image needs a value"
                IMAGE="$2"; shift 2 ;;
            --wait)
                [ "$#" -ge 2 ] || die "--wait needs a value"
                WAIT_SECS="$2"; shift 2 ;;
            -h|--help)      usage; exit 0 ;;
            --)             shift; rest+=("$@"); break ;;
            -*)             die "unknown option '$1'" "Run '$(basename "$0") --help' for usage." ;;
            *)              rest+=("$1"); shift ;;
        esac
    done

    case "$command" in
        up)       cmd_up ;;
        down)     cmd_down ;;
        status)   cmd_status ;;
        urls)     print_urls ;;
        logs)     cmd_logs "${rest[@]+"${rest[@]}"}" ;;
        build)    preflight_docker; preflight_checkouts; ensure_image ;;
        generate) preflight_checkouts; ensure_crate ;;
        *)        die "unknown command '$command'" "Run '$(basename "$0") --help' for usage." ;;
    esac
}

main "$@"
