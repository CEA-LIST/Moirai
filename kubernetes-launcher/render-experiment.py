#!/usr/bin/env python3
"""Validate an experiment JSON file and render run-specific Kustomize overlays."""

import json
import re
import shutil
import sys
from pathlib import Path


REQUIRED_KEYS = {
    "simulation_name",
    "n_replicas",
    "n_clients",
    "n_operations",
    "frequency",
    "use_optimistic_requests",
    "use_consistent_requests",
    "with_fault",
    "failure_time",
    "replica_image",
    "client_image",
    "output_path",
}


def fail(message: str) -> None:
    raise SystemExit(f"Experiment configuration error: {message}")


def require_positive_integer(config: dict, key: str, minimum: int) -> int:
    value = config[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        fail(f"{key} must be an integer >= {minimum}")
    return value


def require_nonempty_string(config: dict, key: str) -> str:
    value = config[key]
    if not isinstance(value, str) or not value.strip():
        fail(f"{key} must be a nonempty string")
    return value.strip()


def validate_config(path: Path) -> dict:
    try:
        with path.open(encoding="utf-8") as source:
            config = json.load(source)
    except (OSError, json.JSONDecodeError) as error:
        fail(f"cannot read valid JSON from {path}: {error}")

    if not isinstance(config, dict):
        fail("the top-level JSON value must be an object")

    missing = REQUIRED_KEYS - config.keys()
    unknown = config.keys() - REQUIRED_KEYS
    if missing:
        fail("missing required field(s): " + ", ".join(sorted(missing)))
    if unknown:
        fail("unknown field(s): " + ", ".join(sorted(unknown)))

    name = require_nonempty_string(config, "simulation_name")
    if len(name) > 63 or not re.fullmatch(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?", name):
        fail("simulation_name must be a lowercase Kubernetes label value (1-63 characters)")

    require_positive_integer(config, "n_replicas", 1)
    require_positive_integer(config, "n_clients", 1)
    require_positive_integer(config, "n_operations", 0)
    require_positive_integer(config, "frequency", 1)
    require_positive_integer(config, "failure_time", 0)

    for key in (
        "use_optimistic_requests",
        "use_consistent_requests",
        "with_fault",
    ):
        if not isinstance(config[key], bool):
            fail(f"{key} must be a JSON boolean (true or false)")
    if not config["use_optimistic_requests"] and not config["use_consistent_requests"]:
        fail("at least one request mode must be enabled")

    require_nonempty_string(config, "replica_image")
    require_nonempty_string(config, "client_image")
    output_path = require_nonempty_string(config, "output_path")
    if "\n" in output_path or "\r" in output_path:
        fail("output_path must not contain a newline")

    output_dir = Path(output_path).expanduser()
    if not output_dir.is_absolute():
        output_dir = path.parent / output_dir
    config["results_file"] = str(
        output_dir.resolve() / f"{name}-measurements.jsonl"
    )
    return config


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def write_kustomization(path: Path, resource: str, patch: str, group: str,
                        kind: str, name: str, generated_configmap: bool = False) -> None:
    lines = [
        "apiVersion: kustomize.config.k8s.io/v1beta1",
        "kind: Kustomization",
    ]
    if generated_configmap:
        lines += [
            "",
            "configMapGenerator:",
            "  - name: moirai-multi-bootstrap",
            "    files:",
            "      - replica-template.json",
            "      - render-config.py",
        ]
    lines += [
        "",
        "resources:",
        f"  - {resource}",
        "",
        "patches:",
        f"  - path: {patch}",
        "    target:",
    ]
    if group:
        lines.append(f"      group: {group}")
    lines += [
        "      version: v1",
        f"      kind: {kind}",
        f"      name: {name}",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def render(config: dict, launcher_dir: Path, output_dir: Path) -> None:
    write_json(output_dir / "runtime-config.json", {
        "with_fault": config["with_fault"],
        "failure_time": config["failure_time"],
    })

    replicas = output_dir / "replicas"
    clients = output_dir / "clients"
    replicas.mkdir(parents=True, exist_ok=True)
    clients.mkdir(parents=True, exist_ok=True)

    source_replicas = launcher_dir / "multireplica"
    for filename in ("k8s.yaml", "replica-template.json", "render-config.py"):
        shutil.copy2(source_replicas / filename, replicas / filename)
    shutil.copy2(launcher_dir / "clients" / "job.yaml", clients / "job.yaml")

    replica_patch = [
        {"op": "replace", "path": "/spec/replicas", "value": config["n_replicas"]},
        {
            "op": "replace",
            "path": "/spec/template/spec/initContainers/0/env/0/value",
            "value": str(config["n_replicas"]),
        },
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/image",
            "value": config["replica_image"],
        },
    ]
    write_json(replicas / "replica-patch.json", replica_patch)
    write_kustomization(
        replicas / "kustomization.yaml", "k8s.yaml", "replica-patch.json",
        "apps", "StatefulSet", "moirai-multi-replica", generated_configmap=True,
    )

    client_patch = [
        {"op": "replace", "path": "/spec/completions", "value": config["n_clients"]},
        {"op": "replace", "path": "/spec/parallelism", "value": config["n_clients"]},
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/image",
            "value": config["client_image"],
        },
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/env/1/value",
            "value": str(config["n_operations"]),
        },
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/env/2/value",
            "value": str(config["frequency"]),
        },
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/env/3/value",
            "value": str(config["use_optimistic_requests"]).lower(),
        },
        {
            "op": "replace",
            "path": "/spec/template/spec/containers/0/env/4/value",
            "value": str(config["use_consistent_requests"]).lower(),
        },
    ]
    write_json(clients / "client-patch.json", client_patch)
    write_kustomization(
        clients / "kustomization.yaml", "job.yaml", "client-patch.json",
        "batch", "Job", "moirai-client-load",
    )


def main() -> None:
    if len(sys.argv) != 4:
        raise SystemExit(
            "Usage: render-experiment.py CONFIG.json LAUNCHER_DIR OUTPUT_DIR"
        )
    config_path = Path(sys.argv[1]).resolve()
    launcher_dir = Path(sys.argv[2]).resolve()
    output_dir = Path(sys.argv[3]).resolve()
    config = validate_config(config_path)
    render(config, launcher_dir, output_dir)
    print(f"Experiment: {config['simulation_name']}", file=sys.stderr)
    print(f"Replicas: {config['n_replicas']}; clients: {config['n_clients']}", file=sys.stderr)
    print(config["results_file"])


if __name__ == "__main__":
    main()
