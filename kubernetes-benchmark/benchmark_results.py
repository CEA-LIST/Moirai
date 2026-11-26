"""Read replica logs and compute the statistics used by both viewers."""

from dataclasses import dataclass, field
import json
from pathlib import Path
from statistics import mean, quantiles


NS = 1_000_000_000
MIB = 1024 * 1024


@dataclass
class Run:
    name: str
    config: dict
    samples: dict
    workload_done: dict
    version: int = 1
    uncommitted: dict = field(default_factory=dict)

    @property
    def replicas(self):
        return self.config["n_replicas"]

    def load(self, basis):
        per_replica = self.config["workload"]["rate_per_replica"]
        return per_replica if basis == "per_replica" else per_replica * self.replicas

    @property
    def implementation(self):
        return self.config["protocol"], self.config["replica_image"]

    @property
    def conditions(self):
        settings = self.config
        workload = settings["workload"]
        common = {key: settings[key] for key in (
            "replica_settings", "sample_interval_ms", "network_interface", "grace_s", "fault_after_s",
        )}
        common["benchmark_version"] = self.version
        common.update({key: workload[key] for key in (
            "duration_s", "query_percent", "add_percent", "key_space", "queue_capacity",
        )})
        return json.dumps(common, sort_keys=True)

    @property
    def drain_samples(self):
        return {replica: sample for replica, samples in self.samples.items()
                for sample in samples if sample["drained"]}

    def latencies_ms(self, include_uncommitted=False):
        delays = [latency / 1_000_000 for samples in self.samples.values() for sample in samples
                  for _, latency, _ in sample["commit_latency_samples"]]
        if include_uncommitted:
            for seal, updates in self.uncommitted.values():
                delays.extend((seal - dequeue) / 1_000_000 for dequeue, _ in updates)
        return delays


def read_run(root, directory):
    config = json.loads((directory / "experiment.json").read_text(encoding="utf-8"))
    run_status = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    if not run_status["complete"]:
        raise ValueError(run_status.get("error") or "; ".join(run_status.get("warnings", [])) or "incomplete")
    samples, done, pending = {}, {}, {}
    version = run_status.get("benchmark_version", 1)
    fault = run_status["faulted_replica"]
    for replica in range(config["n_replicas"]):
        records = []
        uncommitted = []
        seal = None
        with (directory / "replicas" / f"{replica}.log").open(encoding="utf-8") as stream:
            for line in stream:
                if not line.startswith("{"):
                    continue
                event = json.loads(line)
                if event["type"] == "benchmark_sample":
                    records.append(event)
                elif event["type"] == "benchmark_workload_done":
                    done[replica] = event
                elif event["type"] == "benchmark_uncommitted":
                    timestamp = event["seal_elapsed_ns"]
                    if seal is not None and seal != timestamp:
                        raise ValueError(f"replica {replica} has inconsistent seal timestamps")
                    seal = timestamp
                    uncommitted.extend(event["updates"])
        if replica != fault and (not records or not records[-1]["final"] or replica not in done
                                 or sum(row["drained"] for row in records) != 1):
            raise ValueError(f"replica {replica} is missing completed results")
        if replica in done:
            completion = done[replica]
            if (completion["scheduled"] != completion["enqueued"] + completion["queue_full"] + completion["missed_slots"]
                    or completion["enqueued"] != completion["processed_updates"] + completion["processed_queries"]):
                raise ValueError(f"replica {replica} has inconsistent workload counts")
        if any(right["elapsed_ns"] <= left["elapsed_ns"] or
               right["committed_updates"] < left["committed_updates"] or
               right["committed_local_updates"] < left["committed_local_updates"]
               for left, right in zip(records, records[1:])):
            raise ValueError(f"replica {replica} has inconsistent samples")
        if sum(len(row["commit_latency_samples"]) for row in records) != (
            records[-1]["committed_local_updates"] if records else 0
        ):
            raise ValueError(f"replica {replica} has missing latency samples")
        if version >= 3 and replica != fault:
            final = records[-1]
            expected = final["processed_updates"] - final["committed_local_updates"]
            limit = final["elapsed_ns"]
            if (len(uncommitted) != expected or (seal is not None and seal != limit)
                    or any(not 0 <= dequeue <= limit for dequeue, _ in uncommitted)):
                raise ValueError(f"replica {replica} has inconsistent uncommitted update records")
            pending[replica] = (limit, uncommitted)
        samples[replica] = records
    config["protocol"] = {"omega": "commit", "lamport": "timestamp"}.get(
        config["protocol"], config["protocol"],
    )
    return Run(str(directory.relative_to(root)), config, samples, done, version, pending)


def load_runs(root):
    runs, notes = [], []
    for status in sorted(root.rglob("run.json")):
        try:
            runs.append(read_run(root, status.parent))
        except (OSError, TypeError, ValueError, KeyError) as error:
            notes.append(f"{status.parent.relative_to(root)} was skipped ({error})")
    return runs, notes


def summary(run):
    drains = run.drain_samples
    # Each observer counts every origin. One surviving observer yields a unique
    # cluster throughput without multiplying by the degree of replication.
    reference = drains[min(drains)]
    throughput = reference["committed_updates"] / (reference["elapsed_ns"] / NS)
    return {
        "network": mean((row["resources"]["rx_bytes"] + row["resources"]["tx_bytes"]) / MIB
                        for row in drains.values()),
        "cpu": mean(row["resources"]["cpu_time_us"] / 1_000_000 for row in drains.values()),
        "throughput": throughput,
    }


def percentiles(values):
    if not values:
        return {}
    cuts = quantiles(values, n=100, method="inclusive") if len(values) > 1 else values * 99
    return {p: cuts[p - 1] for p in (50, 95, 99)}
