# Build with the root of the complete Moirai workspace as the context:
#   docker build -f kubernetes-launcher/Dockerfile -t moirai-replica:dev .
FROM docker.io/library/rust:bookworm AS build

WORKDIR /workspace
COPY . .

ARG REPLICA_EXAMPLE=commit_set
RUN cargo build --release --locked --manifest-path moirai-network-node/Cargo.toml --target-dir target --example "${REPLICA_EXAMPLE}" \
    && install -D "target/release/examples/${REPLICA_EXAMPLE}" /out/moirai-replica

FROM docker.io/library/debian:bookworm-slim
RUN groupadd --gid 10001 moirai \
    && useradd --uid 10001 --gid 10001 \
       --no-create-home --shell /usr/sbin/nologin moirai
COPY --from=build /out/moirai-replica /usr/local/bin/moirai-replica
USER 10001:10001
EXPOSE 19001/tcp 8081/tcp
ENTRYPOINT ["/usr/local/bin/moirai-replica"]
CMD ["/etc/moirai/replica.json"]
