#!/bin/sh
#
# Registers N models on the rig, so that `rig.sh --models N` gives every
# replica the same N logs beside its pinned default one.
#
# # Where it runs, and why it is not on the host
#
# Only the two editor replicas publish a host port (8081 and 8082). The `node`
# replicas are reachable by name on the `session` bridge and nowhere else, so a
# script that registered from the host could reach the editors and none of the
# nodes. This therefore runs *inside* the rig: `rig.sh` pipes it into
# `docker compose exec -T bootnode sh -s`, which puts it on both bridges with
# the image's own `curl`, and needs no new service, no mount and no image
# change. Its stdout is the model list, one `model_id,package,digest` per line,
# which `rig.sh` tees to `runs/models.csv`; everything a human reads goes to
# stderr.
#
# # How the same N models reach every replica
#
# One replica creates, the rest join by id. `POST /api/models {metamodel_id}`
# on `editor-a` mints the `ModelId` and writes the `__model` header, and
# `POST /api/models {model_id, metamodel_id}` on every other replica hosts that
# same id with no history, which the joiner then fills by state transfer or
# delta sync. The id is carried between them in this script's shell variable
# and nowhere else: the node mints it, this reads it out of the 201, and every
# join names it back. There is no catalogue and no gossip of hosted ids — the
# model plane's design says so in §12 — so this script *is* the out-of-band
# channel that design leaves to a person pasting an id into the editor.
#
# The metamodels are whatever `GET /api/metamodels` lists on the creator, taken
# in the order the node lists them and used round-robin, which on the shipped
# image (`/metamodels`, holding `bt.metamodel.json` and `uml.metamodel.json`)
# means the models alternate behaviour tree, SimpleUML, behaviour tree, ...
# A registration names a metamodel by its digest, so nothing here holds a copy
# of a descriptor or of the digest rule.
#
# # Environment
#
#   BOOTNODE_URL   default http://bootnode:7000
#   SESSION_ID     default compose
#   HTTP_PORT      replica HTTP port inside the rig, default 8081
#   MODELS         how many models to register, required, > 0
#   CREATOR        replica that creates every model, default editor-a
#   SKIP_JOIN      space-separated replica ids never joined, default islander
#   EXPECT_PEERS   roster size to wait for before starting, default 0
#   WAIT_SECS      how long to wait for the roster and the creator, default 120

set -u

BOOTNODE_URL=${BOOTNODE_URL:-http://bootnode:7000}
SESSION_ID=${SESSION_ID:-compose}
HTTP_PORT=${HTTP_PORT:-8081}
MODELS=${MODELS:-0}
CREATOR=${CREATOR:-editor-a}
SKIP_JOIN=${SKIP_JOIN:-islander}
EXPECT_PEERS=${EXPECT_PEERS:-0}
WAIT_SECS=${WAIT_SECS:-120}

say() { printf 'register: %s\n' "$1" >&2; }
die() { printf '\nregister: %s\n\n' "$1" >&2; exit 1; }

case "$MODELS" in
    ''|*[!0-9]*) die "MODELS must be a non-negative integer, got '$MODELS'" ;;
esac
[ "$MODELS" -gt 0 ] || exit 0

now() { date +%s; }

# One JSON string field out of a compact object, by name.
field() {
    printf '%s' "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -n 1 | cut -d'"' -f4
}

# ---------------------------------------------------------------------------
# HTTP, with a retry.
#
# A replica answers `POST /api/models` from the same event loop that drains its
# transport, so a busy node can miss the API's five-second reply window and
# answer 504. That is worth another attempt rather than an abandoned run:
# repeating a *join* of an id already hosted answers 409, which the caller
# below reads as "already there", so the retry cannot register anything twice.
#
# Sets HTTP_CODE and HTTP_BODY; returns non-zero only when no attempt got an
# answer at all.
# ---------------------------------------------------------------------------
post_json() {
    _url=$1
    _body=$2
    _attempt=1
    HTTP_CODE=000
    HTTP_BODY=
    while [ "$_attempt" -le 3 ]; do
        _out=$(curl -sS --max-time 30 -w '\n%{http_code}' \
            -H 'Content-Type: application/json' \
            -X POST "$_url" -d "$_body" 2>/dev/null)
        if [ -n "$_out" ]; then
            HTTP_CODE=$(printf '%s' "$_out" | tail -n 1)
            HTTP_BODY=$(printf '%s' "$_out" | sed '$d')
            case "$HTTP_CODE" in
                5*|000) ;;
                *) return 0 ;;
            esac
        fi
        _attempt=$((_attempt + 1))
        sleep 1
    done
    return 1
}

# ---------------------------------------------------------------------------
# Readiness. The roster is the same signal `drive.sh` and `observe.sh` use: a
# replica that has registered itself is one that is up, and its `addr` is how
# every other replica reaches it, which is also how this reaches its HTTP API.
# ---------------------------------------------------------------------------

deadline=$(( $(now) + WAIT_SECS ))
while :; do
    roster=$(curl -fsS --max-time 5 "$BOOTNODE_URL/session/$SESSION_ID/peers" 2>/dev/null)
    count=$(printf '%s' "$roster" | grep -o '"addr":"' | wc -l | tr -d ' ')
    [ "$count" -ge "$EXPECT_PEERS" ] && break
    if [ "$(now)" -ge "$deadline" ]; then
        say "the directory lists $count replica(s) after ${WAIT_SECS}s, expected \
$EXPECT_PEERS; registering on the ones that are there"
        break
    fi
    sleep 1
done

# `id:host` per replica, in the order the directory lists them. The two fields
# are pulled by name and not by position, because the roster serialises `addr`
# before `id`.
peers=$(printf '%s' "$roster" \
    | sed 's/},{/}\n{/g' \
    | grep -o '{[^{}]*}' \
    | while read -r entry; do
        _id=$(field "$entry" id)
        _addr=$(field "$entry" addr)
        [ -n "$_id" ] && printf '%s:%s\n' "$_id" "${_addr%%:*}"
    done)

[ -n "$peers" ] || die "the directory lists no replica for session '$SESSION_ID'"

creator_host=$CREATOR
for peer in $peers; do
    [ "${peer%%:*}" = "$CREATOR" ] && creator_host=${peer#*:}
done
creator_url="http://$creator_host:$HTTP_PORT"

deadline=$(( $(now) + WAIT_SECS ))
while :; do
    curl -fsS --max-time 5 "$creator_url/api/health" >/dev/null 2>&1 && break
    [ "$(now)" -lt "$deadline" ] || die "$CREATOR never answered /api/health at $creator_url"
    sleep 1
done

# ---------------------------------------------------------------------------
# The metamodels, and the replicas that will hold every model.
# ---------------------------------------------------------------------------

listing=$(curl -fsS --max-time 10 "$creator_url/api/metamodels" 2>/dev/null)
descriptors=$(printf '%s' "$listing" \
    | sed 's/},{/}\n{/g' \
    | grep -o '{[^{}]*}' \
    | while read -r entry; do
        _digest=$(field "$entry" digest)
        _package=$(field "$entry" package)
        _ns_uri=$(field "$entry" nsURI)
        [ -n "$_digest" ] && printf '%s\t%s\t%s\n' "${_package:-unnamed}" "$_digest" "$_ns_uri"
    done)

[ -n "$descriptors" ] || die \
    "$CREATOR lists no metamodel at $creator_url/api/metamodels; is METAMODEL_DIR set? ($listing)"

kinds=$(printf '%s\n' "$descriptors" | wc -l | tr -d ' ')

joiners=
joiner_names=
for peer in $peers; do
    _id=${peer%%:*}
    [ "$_id" = "$CREATOR" ] && continue
    _skip=0
    for excluded in $SKIP_JOIN; do
        [ "$_id" = "$excluded" ] && _skip=1
    done
    [ "$_skip" -eq 1 ] && continue
    joiners="$joiners $peer"
    joiner_names="$joiner_names $_id"
done

[ -n "$joiners" ] || say "no replica to join: every model will be hosted on $CREATOR alone"

say "registering $MODELS model(s) on $CREATOR over $kinds metamodel(s), joined on:$joiner_names"

# ---------------------------------------------------------------------------
# Create on the creator, join by id everywhere else.
# ---------------------------------------------------------------------------

started=$(now)
created=0
joins=0
i=0
while [ "$i" -lt "$MODELS" ]; do
    i=$((i + 1))
    descriptor=$(printf '%s\n' "$descriptors" | sed -n "$(( (i - 1) % kinds + 1 ))p")
    package=$(printf '%s' "$descriptor" | cut -f1)
    digest=$(printf '%s' "$descriptor" | cut -f2)
    ns_uri=$(printf '%s' "$descriptor" | cut -f3)
    metamodel="{\"nsURI\":\"$ns_uri\",\"digest\":\"$digest\"}"

    post_json "$creator_url/api/models" "{\"metamodel_id\":$metamodel}" \
        || die "model $i of $MODELS: $CREATOR did not answer POST /api/models"
    [ "$HTTP_CODE" = "201" ] \
        || die "model $i of $MODELS: $CREATOR answered $HTTP_CODE to a create: $HTTP_BODY"
    model_id=$(field "$HTTP_BODY" model_id)
    [ -n "$model_id" ] || die "model $i of $MODELS: no model_id in $HTTP_BODY"
    created=$((created + 1))

    for peer in $joiners; do
        peer_id=${peer%%:*}
        peer_url="http://${peer#*:}:$HTTP_PORT"
        post_json "$peer_url/api/models" \
            "{\"model_id\":\"$model_id\",\"metamodel_id\":$metamodel}" \
            || die "model $model_id: $peer_id did not answer POST /api/models"
        case "$HTTP_CODE" in
            200|409) joins=$((joins + 1)) ;;
            *) die "model $model_id: $peer_id answered $HTTP_CODE to a join: $HTTP_BODY" ;;
        esac
    done

    printf '%s,%s,%s\n' "$model_id" "$package" "$digest"
    if [ $((i % 8)) -eq 0 ] || [ "$i" -eq "$MODELS" ]; then
        say "$i/$MODELS registered ($(( $(now) - started ))s)"
    fi
done

say "$created created on $CREATOR, $joins join(s) across$joiner_names in $(( $(now) - started ))s"
