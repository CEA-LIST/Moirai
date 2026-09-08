# Replica image for the end-to-end convergence tests, and for the rig.
#
# It carries **two** replica binaries, which is the phase 5 claim in one line
# of packaging: `network_node`, generated from one metamodel and rebuilt for
# the next, and `model_node`, built once from `moirai-interp` and hosting a
# model of any descriptor it holds. The compose rig's `interp` profile runs
# the second; everything else runs the first.
#
# The binary under test in `e2e_convergence.rs` is `network_node`, the example
# shipped by the Arachne-generated `json_crdt` crate. That crate depends on this workspace
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

# The checkout directories are not always named `moirai` and `arachne`: a git
# worktree keeps the branch name in the directory (`moirai-modelsward`), and the
# generated crate's relative path dependencies must keep resolving inside the
# image exactly as they do on disk. The build therefore copies each checkout
# under its real name.
ARG MOIRAI_DIR=moirai
ARG ARACHNE_DIR=arachne

WORKDIR /build
COPY ${MOIRAI_DIR}/ ./${MOIRAI_DIR}/
COPY ${ARACHNE_DIR}/generated/json_crdt/ ./${ARACHNE_DIR}/generated/json_crdt/
# Metamodel descriptors ship in the image so a rig replica can serve
# /api/metamodel without any host mount.
COPY ${ARACHNE_DIR}/examples/ /build/metamodels/

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
WORKDIR /build/${MOIRAI_DIR}
RUN cargo build -p moirai-bootnode -p moirai-relay -p moirai-dashboard

# The interpreted replica. One build, and it never needs another: a metamodel
# it has never seen arrives at run time through `POST /api/metamodels`. The
# `network` feature is what brings `moirai-network` into `moirai-interp`, and
# with it `QueryableLog for ModelLog`, which is what `GET /api/state` answers
# through.
RUN cargo build -p moirai-interp --features network --example model_node

WORKDIR /build/${ARACHNE_DIR}/generated/json_crdt
# Debug profile: the tests exercise replication, not throughput, and a release
# build of the whole workspace costs several minutes per image rebuild.
RUN cargo build --example network_node

FROM debian:bookworm-slim

ARG MOIRAI_DIR=moirai
ARG ARACHNE_DIR=arachne

# `curl` is here for HEALTHCHECK, which is what lets the harness wait on
# readiness instead of guessing with a sleep.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder \
    /build/${ARACHNE_DIR}/generated/json_crdt/target/debug/examples/network_node \
    /usr/local/bin/network_node
COPY --from=builder \
    /build/${MOIRAI_DIR}/target/debug/examples/model_node \
    /usr/local/bin/model_node
COPY --from=builder \
    /build/${MOIRAI_DIR}/target/debug/moirai-bootnode \
    /usr/local/bin/moirai-bootnode
COPY --from=builder \
    /build/${MOIRAI_DIR}/target/debug/moirai-relay \
    /usr/local/bin/moirai-relay
# Same reasoning as the bootnode: the dashboard ships in the replica image so
# that what watches the protocol cannot drift onto a different build of it.
COPY --from=builder \
    /build/${MOIRAI_DIR}/target/debug/moirai-dashboard \
    /usr/local/bin/moirai-dashboard
COPY --from=builder /build/metamodels /metamodels

ENV REPLICA_ID=a \
    LISTEN_PORT=9001 \
    HTTP_PORT=8081 \
    PEERS=

EXPOSE 9001 8081 7000 7100 7101 8090

HEALTHCHECK --interval=1s --timeout=2s --start-period=1s --retries=60 \
    CMD curl -fsS "http://localhost:${HTTP_PORT}/api/health" || exit 1

CMD ["network_node"]
