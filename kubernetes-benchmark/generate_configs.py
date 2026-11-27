"""Generate independent benchmark run configurations from a JSON sweep specification."""

import argparse
from itertools import product
import json
from pathlib import Path
import sys

LAUNCHER = Path(__file__).resolve().parents[1] / "kubernetes-launcher" / "benchmark-replica"
sys.path.insert(0, str(LAUNCHER))
from benchmark_config import PROTOCOLS, canonical_protocol, integer, validate  # noqa: E402


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def values(spec, key, default, parse):
    items = spec.get(key, default)
    if not isinstance(items, list) or not items:
        raise ValueError(f"{key} must be a nonempty array")
    return sorted({parse(item) for item in items})


def positive(value):
    return integer({"value": value}, "value", 1, 2**63 - 1)


def protocol(value):
    value = canonical_protocol(value)
    if value not in PROTOCOLS:
        raise ValueError(f"unknown protocol {value}")
    return value


def generate(path):
    spec = read_json(path)
    allowed = {"output_folder", "base", "replicas", "total_rates", "rates_per_replica",
               "protocols", "repetitions", "duration_s", "with_fault", "failure_time_s"}
    if not isinstance(spec, dict) or "output_folder" not in spec:
        raise ValueError("output_folder is required in the sweep JSON")
    if extra := spec.keys() - allowed:
        raise ValueError(f"unknown sweep fields {sorted(extra)}")
    if "total_rates" in spec and "rates_per_replica" in spec:
        raise ValueError("choose either total_rates or rates_per_replica")
    base_path = path.parent / spec["base"] if "base" in spec else LAUNCHER / "experiment_sample.json"
    base = validate(read_json(base_path))
    replicas = values(spec, "replicas", [base["n_replicas"]], positive)
    total = "total_rates" in spec
    rate_key = "total_rates" if total else "rates_per_replica"
    offered = values(spec, rate_key, [base["workload"]["rate_per_replica"]], positive)
    protocols = values(spec, "protocols", [base["protocol"]], protocol)
    repetitions = positive(spec.get("repetitions", 1))
    duration = spec.get("duration_s", base["workload"]["duration_s"])
    fault_at = spec.get("failure_time_s", base["fault_after_s"])
    with_fault = spec.get("with_fault", fault_at is not None)
    if type(with_fault) is not bool:
        raise ValueError("with_fault must be true or false")
    if with_fault and fault_at is None:
        raise ValueError("failure_time_s is required when with_fault is true")
    if total and any(rate % n for rate, n in product(offered, replicas)):
        raise ValueError("each total rate must be divisible by every replica count")
    configs = []
    for n, rate, name, rep in product(replicas, offered, protocols, range(repetitions)):
        per_replica = rate // n if total else rate
        config = {
            **base, "simulation_name": f"{name}-{n}r-{per_replica * n}ops-r{rep + 1:03}",
            "n_replicas": n, "protocol": name,
            "fault_after_s": fault_at if with_fault else None,
            "workload": {**base["workload"], "rate_per_replica": per_replica, "duration_s": duration,
                         "seed": base["workload"]["seed"] + rep},
        }
        configs.append(validate(config))
    folder = path.parent / spec["output_folder"]
    folder.mkdir(parents=True, exist_ok=False)
    for config in configs:
        output = folder / f"{config['simulation_name']}.json"
        output.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    return folder, len(configs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sweep", type=Path, help="Path to a JSON sweep specification")
    args = parser.parse_args()
    try:
        folder, count = generate(args.sweep)
    except (TypeError, ValueError, OSError) as error:
        parser.error(str(error))
    print(f"Wrote {count} configurations to {folder}")


if __name__ == "__main__":
    main()
