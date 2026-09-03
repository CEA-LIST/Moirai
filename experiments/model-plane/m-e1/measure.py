#!/usr/bin/env python3
"""M-E1: the `Event` frames of a seeded workload, read off the wire.

One probe connection per replica. A replica broadcasts every event it
originates to every peer it holds, the probe included, so across the three
connections every operation of the workload is seen exactly once as the
frame its origin put on the wire. The driver is `moirai-dashboard --random`,
run by `run.sh` inside the containers' network.

Per frame: its length, and the length of the `,"log_id":"<id>"` member it
carries, cut out of the raw bytes and not derived, so the 44 is re-read from
every frame rather than assumed from the test.
"""

import argparse
import csv
import re
import statistics
import subprocess
import sys
import threading

sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
from wire import Probe, wait_healthy, wait_mesh  # noqa: E402

LOG_ID_MEMBER = re.compile(rb',"log_id":"[0-9a-f]{32}"')


def collect(probe, sink, expected, done):
    """Read Event frames into `sink` until `expected` have been seen or the wire goes quiet."""
    for raw, message in probe.frames(deadline_s=15):
        if message.get("type") != "Event":
            continue
        members = LOG_ID_MEMBER.findall(raw)
        sink.append((len(raw), len(members), len(members[0]) if members else 0))
        if len(sink) >= expected and done.is_set():
            break


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", required=True, help="comma-separated http://host:port per replica")
    parser.add_argument("--sync", required=True, help="comma-separated host:port listeners per replica")
    parser.add_argument("--driver", required=True, help="the shell command that applies the workload")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--operations", type=int, required=True)
    parser.add_argument("--test-frame-bytes", type=int, required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    bases = args.http.split(",")
    for base in bases:
        wait_healthy(base)
    wait_mesh(bases, expect=len(bases) - 1)

    frames = []
    done = threading.Event()
    probes = [Probe(addr, f"m-e1-probe-{i}") for i, addr in enumerate(args.sync.split(","))]
    readers = [threading.Thread(target=collect, args=(p, frames, args.operations, done), daemon=True)
               for p in probes]
    for reader in readers:
        reader.start()

    driver = subprocess.run(args.driver, shell=True, capture_output=True, text=True)
    done.set()
    print(driver.stdout.strip().splitlines()[-1] if driver.stdout.strip() else "", file=sys.stderr)
    if driver.returncode != 0:
        raise SystemExit(f"the driver failed: {driver.stderr}")
    for reader in readers:
        reader.join(timeout=30)
    for probe in probes:
        probe.close()

    if not frames:
        raise SystemExit("no Event frame reached the probe")
    lengths = sorted(length for length, _, _ in frames)
    member_counts = {count for _, count, _ in frames}
    member_lengths = {size for _, _, size in frames}
    if member_counts != {1} or member_lengths != {44}:
        raise SystemExit(f"a frame did not carry log_id exactly once at 44 bytes: counts {member_counts}, lengths {member_lengths}")

    median = statistics.median(lengths)
    row = {
        "run": 1,
        "seed": args.seed,
        "frames": len(lengths),
        "median_event_bytes": median,
        "log_id_bytes": 44,
        "log_id_pct": round(100.0 * 44 / median, 2),
        "min_event_bytes": lengths[0],
        "max_event_bytes": lengths[-1],
        "mean_event_bytes": round(statistics.mean(lengths), 1),
        "test_frame_bytes": args.test_frame_bytes,
        "test_frame_log_id_pct": round(100.0 * 44 / args.test_frame_bytes, 2),
    }
    with open(args.out, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)
    print(f"{len(lengths)} Event frames of {args.operations} operations: median {median} B, "
          f"min {lengths[0]} B, max {lengths[-1]} B; log_id 44 B = {row['log_id_pct']}% of the median",
          file=sys.stderr)
    if len(lengths) != args.operations:
        print(f"warning: {len(lengths)} frames seen for {args.operations} operations", file=sys.stderr)


if __name__ == "__main__":
    main()
