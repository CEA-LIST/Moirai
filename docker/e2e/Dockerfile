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

ENV REPLICA_ID=a \
    LISTEN_PORT=9001 \
    HTTP_PORT=8081 \
    PEERS=

EXPOSE 9001 8081

HEALTHCHECK --interval=1s --timeout=2s --start-period=1s --retries=60 \
    CMD curl -fsS "http://localhost:${HTTP_PORT}/api/health" || exit 1

CMD ["network_node"]
