# Replica image for the end-to-end convergence tests.
#
# The binary under test is `network_node`, the example shipped by the
# Arachne-generated `json_crdt` crate. That crate depends on this workspace
# through relative path dependencies (`../../../moirai/moirai-crdt`, ...), so
# the build context must reproduce the sibling-repository layout:
#
#     <context>/moirai/                        <- this workspace
#     <context>/arachne/generated/json_crdt/   <- the generated crate
#
# In other words, the context is the directory that holds *both* checkouts:
#
#     docker build -f moirai/docker/e2e/Dockerfile -t moirai-json-crdt:test .
#
# run from the parent of the two clones. `Dockerfile.dockerignore` next to this
# file keeps `target/` and `.git/` out of the context — without it the context
# is several gigabytes of build artefacts.
#
# The e2e harness (`moirai-network/tests/e2e_convergence.rs`) builds this image
# itself when `MOIRAI_E2E_IMAGE` is unset, so this file is normally invoked
# indirectly.

FROM rust:1.97-slim AS builder

WORKDIR /build
COPY moirai/ ./moirai/
COPY arachne/generated/json_crdt/ ./arachne/generated/json_crdt/

# The bootnode, the relay and the dashboard ship in the *same* image as the
# replica rather than one of their own. They are small binaries, and one image
# means the Compose rig, the e2e harness and whatever is watching cannot drift
# onto different builds of the protocol — which, for a rig whose whole job is to
# measure that protocol, is worth more than a few megabytes.
#
# One image, still two services: the bootnode and the relay are separate
# binaries and separate processes (C-D3), because the bootnode's blob-blindness
# is a property worth being able to state and merging a data path into it would
# end that. Co-deploying them in one image is packaging; merging them would be
# design.
WORKDIR /build/moirai
RUN cargo build -p moirai-bootnode -p moirai-relay -p moirai-dashboard

WORKDIR /build/arachne/generated/json_crdt
# Debug profile: the tests exercise replication, not throughput, and a release
# build of the whole workspace costs several minutes per image rebuild.
RUN cargo build --example network_node

FROM debian:bookworm-slim

# `curl` is here for HEALTHCHECK, which is what lets the harness wait on
# readiness instead of guessing with a sleep.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder \
    /build/arachne/generated/json_crdt/target/debug/examples/network_node \
    /usr/local/bin/network_node
COPY --from=builder \
    /build/moirai/target/debug/moirai-bootnode \
    /usr/local/bin/moirai-bootnode
COPY --from=builder \
    /build/moirai/target/debug/moirai-relay \
    /usr/local/bin/moirai-relay
# Same reasoning as the bootnode: the dashboard ships in the replica image so
# that what watches the protocol cannot drift onto a different build of it.
COPY --from=builder \
    /build/moirai/target/debug/moirai-dashboard \
    /usr/local/bin/moirai-dashboard

ENV REPLICA_ID=a \
    LISTEN_PORT=9001 \
    HTTP_PORT=8081 \
    PEERS=

EXPOSE 9001 8081 7000 7100 7101 8090

HEALTHCHECK --interval=1s --timeout=2s --start-period=1s --retries=60 \
    CMD curl -fsS "http://localhost:${HTTP_PORT}/api/health" || exit 1

CMD ["network_node"]
