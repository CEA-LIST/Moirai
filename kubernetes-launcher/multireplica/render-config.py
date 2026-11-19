#!/usr/bin/env python3
"""Create one replica's config and wait until all peer DNS names resolve."""

import json
import os
import socket
import sys
import time


def main() -> None:
    template_path, output_path = sys.argv[1:3]
    with open(template_path, encoding="utf-8") as source:
        template = json.load(source)

    replica_count = int(os.environ["REPLICA_COUNT"])
    pod_name = os.environ["POD_NAME"]
    namespace = os.environ["POD_NAMESPACE"]
    try:
        ordinal = int(pod_name.rsplit("-", 1)[1])
    except (IndexError, ValueError) as error:
        raise SystemExit(f"Cannot parse StatefulSet ordinal from POD_NAME={pod_name!r}") from error

    if replica_count < 1:
        raise SystemExit("REPLICA_COUNT must be at least 1 for this launcher")
    if ordinal >= replica_count:
        raise SystemExit(f"Pod ordinal {ordinal} is outside REPLICA_COUNT={replica_count}")

    service = "moirai-multi-replicas"
    port = int(template["listen_port"])
    peers = []
    for peer_ordinal in range(replica_count):
        if peer_ordinal == ordinal:
            continue
        peer_name = f'{template["replica_name_prefix"]}{peer_ordinal}'
        hostname = f"{pod_name.rsplit('-', 1)[0]}-{peer_ordinal}.{service}.{namespace}.svc.cluster.local"
        peers.append(
            {
                "replica_id": int(template["replica_id_base"]) + peer_ordinal,
                "replica_name": peer_name,
                "address": f"{hostname}:{port}",
            }
        )

    # The replica transport resolves peer names during startup. Wait for all
    # pod DNS records so simultaneous initial startup does not hit a DNS race.
    deadline = time.monotonic() + 180
    unresolved = []
    while True:
        unresolved = []
        for peer in peers:
            host, peer_port = peer["address"].rsplit(":", 1)
            try:
                socket.getaddrinfo(host, int(peer_port), type=socket.SOCK_STREAM)
            except socket.gaierror:
                unresolved.append(host)
        if not unresolved:
            break
        if time.monotonic() >= deadline:
            raise SystemExit(
                "Timed out waiting for peer DNS records: " + ", ".join(unresolved)
            )
        time.sleep(2)

    config = {
        "replica_id": int(template["replica_id_base"]) + ordinal,
        "replica_name": f'{template["replica_name_prefix"]}{ordinal}',
        "listen_port": port,
        "http_port": int(template["http_port"]),
        "peers": peers,
        "pull_interval_ms": int(template["pull_interval_ms"]),
        "pull_peer_count": int(template["pull_peer_count"]),
        "event_timeout_ms": int(template.get("event_timeout_ms", template.get("heartbeat_interval_ms", 10000))),
    }
    for key in ("noop_interval_ms", "request_timeout_ms", "http_optimistic_limit", "http_consistent_limit", "memory_usage_enabled",
                "memory_usage_interval_ms"):
        if key in template:
            config[key] = template[key]
    with open(output_path, "w", encoding="utf-8") as output:
        json.dump(config, output, indent=2)
        output.write("\n")
    print(f"Wrote config for {config['replica_name']} with {len(peers)} peers")


if __name__ == "__main__":
    main()
