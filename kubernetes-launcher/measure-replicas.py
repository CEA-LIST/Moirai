#!/usr/bin/env python3
"""Take and compare per-replica network and cgroup CPU snapshots via kubectl."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys


STATEFULSET = "moirai-multi-replica"
CONTAINER = "replica"
METRIC_COMMAND = (
    "set -eu; "
    "cat /sys/class/net/eth0/statistics/rx_bytes; "
    "cat /sys/class/net/eth0/statistics/tx_bytes; "
    "cat /sys/fs/cgroup/cpu.stat"
)


def kubectl(*args: str, timeout: int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl", *args],
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def get_replica_ordinals() -> range:
    result = kubectl("get", "statefulset", STATEFULSET, "-o", "json")
    if result.returncode:
        raise RuntimeError(result.stderr.strip() or "could not read StatefulSet")
    try:
        spec = json.loads(result.stdout)["spec"]
        count = spec["replicas"]
        start = spec.get("ordinals", {}).get("start", 0)
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise RuntimeError("StatefulSet has no valid replica range") from error
    if not isinstance(count, int) or count < 0:
        raise RuntimeError(f"invalid StatefulSet replica count: {count!r}")
    if not isinstance(start, int) or start < 0:
        raise RuntimeError(f"invalid StatefulSet start ordinal: {start!r}")
    return range(start, start + count)


def snapshot_replica(ordinal: int) -> dict:
    pod_name = f"{STATEFULSET}-{ordinal}"
    entry = {
        "ordinal": ordinal,
        "pod_name": pod_name,
        "pod_uid": None,
        "container_id": None,
        "restart_count": None,
        "rx_bytes": None,
        "tx_bytes": None,
        "cpu_time_us": None,
        "error": None,
    }

    pod_result = kubectl("get", "pod", pod_name, "-o", "json")
    if pod_result.returncode:
        entry["error"] = pod_result.stderr.strip() or "could not read pod"
        return entry

    try:
        pod = json.loads(pod_result.stdout)
        entry["pod_uid"] = pod["metadata"]["uid"]
        status = next(
            item for item in pod["status"]["containerStatuses"]
            if item["name"] == CONTAINER
        )
        entry["container_id"] = status.get("containerID")
        entry["restart_count"] = status.get("restartCount", 0)
        if status.get("ready") is not True:
            entry["error"] = "replica container is not ready"
            return entry
    except (KeyError, StopIteration, TypeError, json.JSONDecodeError) as error:
        entry["error"] = f"could not read replica container status: {error}"
        return entry

    result = kubectl(
        "exec", pod_name, "-c", CONTAINER, "--", "sh", "-c", METRIC_COMMAND,
        timeout=60,
    )
    if result.returncode:
        entry["error"] = result.stderr.strip() or "could not read replica counters"
        return entry

    try:
        lines = result.stdout.splitlines()
        rx_bytes = int(lines[0])
        tx_bytes = int(lines[1])
        cpu_stat = dict(line.split() for line in lines[2:] if len(line.split()) == 2)
        cpu_time_us = int(cpu_stat["usage_usec"])
    except (IndexError, KeyError, TypeError, ValueError) as error:
        entry["error"] = f"invalid counter output: {error}"
        return entry

    entry["rx_bytes"] = rx_bytes
    entry["tx_bytes"] = tx_bytes
    entry["cpu_time_us"] = cpu_time_us
    return entry


def take_snapshot(path: Path, allow_incomplete: bool) -> int:
    ordinals = get_replica_ordinals()
    replicas = [snapshot_replica(ordinal) for ordinal in ordinals]
    snapshot = {
        "sampled_at_utc": datetime.now(timezone.utc).isoformat(),
        "replicas": replicas,
    }
    path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")

    errors = [replica for replica in replicas if replica["error"]]
    for replica in errors:
        print(
            f"Could not sample {replica['pod_name']}: {replica['error']}",
            file=sys.stderr,
        )
    return 0 if allow_incomplete or not errors else 1


def metric_delta(start: dict, end: dict, metric: str, comparable: bool):
    first = start.get(metric)
    last = end.get(metric)
    delta = None
    if comparable and isinstance(first, int) and isinstance(last, int) and last >= first:
        delta = last - first
    return {"start": first, "end": last, "delta": delta}


def compare_snapshots(start_path: Path, end_path: Path, output_path: Path,
                      failure_snapshot_path: Path | None = None) -> None:
    start = json.loads(start_path.read_text(encoding="utf-8"))
    end = json.loads(end_path.read_text(encoding="utf-8"))
    failure_snapshot = (
        json.loads(failure_snapshot_path.read_text(encoding="utf-8"))
        if failure_snapshot_path is not None else None
    )
    start_by_ordinal = {item["ordinal"]: item for item in start["replicas"]}
    end_by_ordinal = {item["ordinal"]: item for item in end["replicas"]}
    failure_by_ordinal = {
        item["ordinal"]: item for item in failure_snapshot["replicas"]
    } if failure_snapshot is not None else {}

    replicas = []
    for ordinal in sorted(
        set(start_by_ordinal) | set(end_by_ordinal) | set(failure_by_ordinal)
    ):
        before = start_by_ordinal.get(ordinal, {})
        end_sample = "experiment_end"
        after = end_by_ordinal.get(ordinal)
        if after is None and ordinal in failure_by_ordinal:
            after = failure_by_ordinal[ordinal]
            end_sample = "pre_failure"
        if after is None:
            after = {}
        same_container = bool(
            before.get("error") is None
            and after.get("error") is None
            and before.get("pod_uid")
            and before.get("pod_uid") == after.get("pod_uid")
            and before.get("container_id")
            and before.get("container_id") == after.get("container_id")
            and before.get("restart_count") == after.get("restart_count")
        )
        reason = None
        if not same_container:
            if ordinal not in start_by_ordinal:
                reason = "replica had no initial sample"
            elif ordinal not in end_by_ordinal:
                reason = "replica is outside the final StatefulSet ordinal range"
            else:
                reason = (
                    before.get("error")
                    or after.get("error")
                    or "pod or replica container restarted between snapshots"
                )

        replicas.append({
            "ordinal": ordinal,
            "pod_name": before.get("pod_name", after.get("pod_name")),
            "pod_uid_start": before.get("pod_uid"),
            "pod_uid_end": after.get("pod_uid"),
            "restart_count_start": before.get("restart_count"),
            "restart_count_end": after.get("restart_count"),
            "end_sample": end_sample,
            "comparable": same_container,
            "incomparable_reason": reason,
            "rx_bytes": metric_delta(before, after, "rx_bytes", same_container),
            "tx_bytes": metric_delta(before, after, "tx_bytes", same_container),
            "cpu_time_us": metric_delta(before, after, "cpu_time_us", same_container),
        })

    record = {
        "type": "replica_resource_usage",
        "start_sampled_at_utc": start.get("sampled_at_utc"),
        "end_sampled_at_utc": end.get("sampled_at_utc"),
        "failure_sampled_at_utc": (
            failure_snapshot.get("sampled_at_utc")
            if failure_snapshot is not None else None
        ),
        "cpu_metric": "cgroup v2 cpu.stat usage_usec",
        "cpu_unit": "microseconds of CPU time (not hardware CPU cycles)",
        "replicas": replicas,
    }
    output_path.write_text(json.dumps(record, separators=(",", ":")) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    snapshot_parser = subparsers.add_parser("snapshot")
    snapshot_parser.add_argument("output", type=Path)
    snapshot_parser.add_argument("--allow-incomplete", action="store_true")

    compare_parser = subparsers.add_parser("compare")
    compare_parser.add_argument("start", type=Path)
    compare_parser.add_argument("end", type=Path)
    compare_parser.add_argument("output", type=Path)
    compare_parser.add_argument("--failure-snapshot", type=Path)

    args = parser.parse_args()
    if args.command == "snapshot":
        try:
            status = take_snapshot(args.output, args.allow_incomplete)
        except (OSError, RuntimeError, subprocess.TimeoutExpired) as error:
            raise SystemExit(f"Replica measurement failed: {error}") from error
        raise SystemExit(status)
    try:
        compare_snapshots(
            args.start, args.end, args.output, args.failure_snapshot
        )
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise SystemExit(f"Could not compare replica measurements: {error}") from error


if __name__ == "__main__":
    main()
