"""Validate experiment settings and render the Kubernetes replica manifest."""

import json
import re


SERVICE = "benchmark-replicas"
REPLICA_PORT = 19001
CREEK_PORT = REPLICA_PORT + 1
PROTOCOLS = ("timestamp", "commit", "creek")
ADMIN_PORT = 8081


def canonical_protocol(name):
    if name == "omega":
        return "commit"
    if name == "lamport":
        return "timestamp"
    return name


def integer(data, key, low, high):
    value = data[key]
    if type(value) is not int or not low <= value <= high:
        raise ValueError(f"{key} must be an integer in [{low}, {high}]")
    return value


def keys(data, expected):
    if not isinstance(data, dict):
        raise ValueError("expected a JSON object")
    missing = expected - data.keys()
    extra = data.keys() - expected
    if missing or extra:
        raise ValueError(f"missing fields {sorted(missing)}, unknown fields {sorted(extra)}")


def validate(config):
    keys(config, {
        "simulation_name", "n_replicas", "protocol", "replica_image", "replica_settings",
        "workload", "sample_interval_ms", "network_interface", "grace_s",
        "startup_timeout_s", "drain_timeout_s", "fault_after_s",
    })
    name = config["simulation_name"]
    if not isinstance(name, str) or not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,119}", name):
        raise ValueError("simulation_name must be a portable directory name")
    replicas = integer(config, "n_replicas", 1, 65_536)
    protocol = canonical_protocol(config["protocol"])
    if protocol not in PROTOCOLS:
        raise ValueError(f"protocol must be one of {', '.join(PROTOCOLS)}")
    if not isinstance(config["replica_image"], str) or not config["replica_image"].strip():
        raise ValueError("replica_image must be a nonempty image reference")
    interface = config["network_interface"]
    if not isinstance(interface, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,15}", interface):
        raise ValueError("network_interface must name a Linux network interface")
    integer(config, "sample_interval_ms", 1, 60_000)
    integer(config, "grace_s", 0, 86_400)
    integer(config, "startup_timeout_s", 1, 86_400)
    integer(config, "drain_timeout_s", 1, 86_400)
    settings = config["replica_settings"]
    keys(settings, {"pull_interval_ms", "pull_peer_count", "noop_interval_ms", "event_timeout_ms"})
    for key in ("pull_interval_ms", "noop_interval_ms", "event_timeout_ms"):
        integer(settings, key, 1, 86_400_000)
    integer(settings, "pull_peer_count", 1, 65_535)
    workload = config["workload"]
    keys(workload, {
        "rate_per_replica", "duration_s", "query_percent", "add_percent",
        "key_space", "queue_capacity", "seed",
    })
    integer(workload, "rate_per_replica", 1, 1_000_000)
    duration = integer(workload, "duration_s", 1, 86_400)
    integer(workload, "query_percent", 0, 100)
    integer(workload, "add_percent", 0, 100)
    integer(workload, "key_space", 1, 2**32 - 1)
    integer(workload, "queue_capacity", 1, 1_000_000)
    integer(workload, "seed", 0, 2**64 - 1)
    if config["fault_after_s"] is not None:
        if replicas < 2:
            raise ValueError("fault injection requires at least two replicas")
        integer(config, "fault_after_s", 1, duration - 1)
    return {**config, "protocol": protocol}


def pod_name(replica_id):
    return f"benchmark-replica-{replica_id}"


def replica_config(config, index):
    replica = {
        "replica_id": index,
        "replica_name": pod_name(index),
        "listen_port": REPLICA_PORT,
        "http_port": ADMIN_PORT,
        "peers": [
            {"replica_id": peer, "address": f"{pod_name(peer)}.{SERVICE}:{REPLICA_PORT}"}
            for peer in range(config["n_replicas"]) if peer != index
        ],
        **config["replica_settings"],
    }
    return {
        "replica": replica,
        "peer_dns_timeout_s": config["startup_timeout_s"],
        **{key: config[key] for key in ("protocol", "workload", "sample_interval_ms", "network_interface")},
    }


def manifest(config):
    labels = {"app": "moirai-commit-benchmark"}
    replication_ports = [("replication", REPLICA_PORT)]
    if config["protocol"] == "creek":
        replication_ports.append(("creek", CREEK_PORT))
    objects = [{
        "apiVersion": "v1", "kind": "Service", "metadata": {"name": SERVICE},
        "spec": {
            "clusterIP": "None", "publishNotReadyAddresses": True, "selector": labels,
            "ports": [{"name": name, "port": port} for name, port in replication_ports],
        },
    }]
    for index in range(config["n_replicas"]):
        name = pod_name(index)
        objects.extend([
            {
                "apiVersion": "v1", "kind": "ConfigMap", "metadata": {"name": name},
                "data": {"replica.json": json.dumps(replica_config(config, index))},
            },
            {
                "apiVersion": "v1", "kind": "Pod", "metadata": {"name": name, "labels": labels},
                "spec": {
                    "hostname": name, "subdomain": SERVICE,
                    "restartPolicy": "Never", "terminationGracePeriodSeconds": 1,
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [{
                        "name": "replica", "image": config["replica_image"],
                        "imagePullPolicy": "IfNotPresent",
                        "args": ["/etc/moirai/replica.json"],
                        "ports": [
                            {"name": name, "containerPort": port}
                            for name, port in [*replication_ports, ("admin", ADMIN_PORT)]
                        ],
                        "readinessProbe": {
                            "httpGet": {"path": "/api/health", "port": "admin"},
                            "periodSeconds": 2,
                        },
                        "securityContext": {
                            "allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                            "capabilities": {"drop": ["ALL"]},
                        },
                        "volumeMounts": [{"name": "config", "mountPath": "/etc/moirai", "readOnly": True}],
                    }],
                    "volumes": [{"name": "config", "configMap": {"name": name}}],
                },
            },
        ])
    return {"apiVersion": "v1", "kind": "List", "items": objects}
