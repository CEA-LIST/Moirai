#!/usr/bin/env python3
"""M-E4: per-log state transfer, on the fresh-joiner path.

A donor hosts N models, every one at the same number of seeded operations;
model A is the SimpleUML one. Each run starts a fresh joiner container, a
replica the donor has never heard from, holding no history in any log; the
joiner registers model A alone and the donor's snapshot of A is adopted.

What is read, per run:

  response_bytes   the `StateResponse` for A as the probe reads it off the
                   wire (the p2 pattern: `Hello` as a stranger, `StateRequest`
                   for A, the length of the answer line)
  probe_ms         the probe's own request-to-answer time, host clock
  served_bytes     the serialised log the donor says it served the joiner,
                   from its `serving a state transfer ... : N bytes` line
  transfer_ms      from the donor's `serving` line to the joiner's `adopted`
                   line, both stamped by the Docker daemon (`docker logs -t`)
  join_to_state_ms from the joiner's `POST /api/models` answering to its
                   `GET /api/model/A/state` equalling the donor's, polled
                   every 2 ms from the host: what a user waits

The joiner's path is read from its log and asserted `adopted`: a joiner
turned away to a delta sync would be measuring something else.
"""

import argparse
import csv
import random
import re
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
from wire import (Probe, apply_to_model, compact, create_model, join_model, model_state,  # noqa: E402
                  poll_until, seeded_ops, served_metamodels, wait_healthy, wait_mesh)

SERVING = re.compile(r"^(\S+) .*serving a state transfer to (\S+) for log ([0-9a-f]{32}): (\d+) bytes")
ADOPTED = re.compile(r"^(\S+) .*adopted state from (\S+) for log ([0-9a-f]{32}): (\d+) members")
REFUSED = re.compile(r"^(\S+) .*cannot adopt (\S+)'s state for log ([0-9a-f]{32})")


def docker_logs(name):
    return subprocess.run(["docker", "logs", "-t", name], capture_output=True, text=True).stderr.splitlines() + \
        subprocess.run(["docker", "logs", "-t", name], capture_output=True, text=True).stdout.splitlines()


def stamp_seconds(text):
    """A `docker logs -t` timestamp as seconds since the epoch."""
    text = text.rstrip("Z")
    whole, _, fraction = text.partition(".")
    base = datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
    return base + (float("0." + fraction) if fraction else 0.0)


def sh(command, **kwargs):
    return subprocess.run(command, shell=True, check=True, capture_output=True, text=True, **kwargs).stdout.strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--donor-http", required=True)
    parser.add_argument("--donor-sync", required=True)
    parser.add_argument("--donor-name", required=True, help="the donor's container name")
    parser.add_argument("--donor-id", required=True, help="the donor's REPLICA_ID")
    parser.add_argument("--n-logs", type=int, required=True)
    parser.add_argument("--ops", type=int, default=500)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--start-joiner", required=True, help="shell command with {name} and {id}; prints host:port of the joiner's HTTP API")
    parser.add_argument("--stop-joiner", required=True, help="shell command with {name}")
    parser.add_argument("--joiner-prefix", required=True, help="container name prefix; the run number is appended")
    parser.add_argument("--out", required=True)
    parser.add_argument("--append", action="store_true")
    args = parser.parse_args()

    donor = args.donor_http
    wait_healthy(donor)
    kinds = served_metamodels(donor)
    if "simpleuml" not in kinds or "behaviortree" not in kinds:
        raise SystemExit(f"the donor lists {sorted(kinds)}; expected behaviortree and simpleuml")

    # Model A first, SimpleUML; the rest alternate as the rig registers them.
    order = ["simpleuml"] + [("behaviortree", "simpleuml")[i % 2] for i in range(args.n_logs - 1)]
    models = [(package, create_model(donor, kinds[package])) for package in order]
    model_a = models[0][1]
    rng = random.Random(args.seed)
    per_model = {model_id: seeded_ops(rng, args.ops, package) for package, model_id in models}

    def fill(model_id):
        for op in per_model[model_id]:
            apply_to_model(donor, model_id, op)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=min(16, len(models))) as pool:
        list(pool.map(fill, [model_id for _, model_id in models]))
    print(f"N={args.n_logs}: {len(models)} models x {args.ops} operations applied on the donor in "
          f"{time.perf_counter() - started:.1f}s", file=sys.stderr)
    donor_state_a = compact(model_state(donor, model_a))

    rows = []
    for run in range(1, args.runs + 1):
        # The wire: a stranger asks for A and measures the answer.
        probe = Probe(args.donor_sync, f"m-e4-probe-{args.n_logs}-{run}")
        response_bytes, message, probe_s = probe.ask_for_state(model_a)
        probe.close()
        assert message["log_id"] == model_a, message["log_id"]

        # The joiner: fresh, registers A alone, adopts.
        name = f"{args.joiner_prefix}{run}"
        joiner_id = f"j{args.n_logs}x{run}"
        joiner = "http://" + sh(args.start_joiner.format(name=name, id=joiner_id))
        wait_healthy(joiner)
        wait_mesh([joiner], expect=1)
        t_join = time.perf_counter()
        join_model(joiner, model_a, kinds["simpleuml"])
        t_joined = time.perf_counter()
        poll_until(lambda: True if compact(model_state(joiner, model_a)) == donor_state_a else None,
                   f"the joiner to hold A", timeout_s=120)
        t_state = time.perf_counter()

        donor_lines = docker_logs(args.donor_name)
        joiner_lines = docker_logs(name)
        serving = [m for m in (SERVING.match(l) for l in donor_lines) if m and m.group(2) == joiner_id and m.group(3) == model_a]
        adopted = [m for m in (ADOPTED.match(l) for l in joiner_lines) if m and m.group(3) == model_a]
        refused = [m for m in (REFUSED.match(l) for l in joiner_lines) if m and m.group(3) == model_a]
        probe_serving = [m for m in (SERVING.match(l) for l in donor_lines)
                         if m and m.group(2) == f"m-e4-probe-{args.n_logs}-{run}" and m.group(3) == model_a]
        if not serving or not adopted:
            raise SystemExit(f"run {run}: no serving/adopted pair for {model_a}; donor tail:\n"
                             + "\n".join(donor_lines[-10:]) + "\njoiner tail:\n" + "\n".join(joiner_lines[-20:]))
        path = "adopted" if adopted and not refused else "delta-sync"
        transfer_s = stamp_seconds(adopted[0].group(1)) - stamp_seconds(serving[0].group(1))
        sh(args.stop_joiner.format(name=name))

        row = {
            "run": run,
            "n_logs": args.n_logs,
            "log_id": model_a,
            "log_ops": args.ops,
            "response_bytes": response_bytes,
            "transfer_ms": round(transfer_s * 1000, 2),
            "probe_ms": round(probe_s * 1000, 2),
            "served_bytes_joiner": int(serving[0].group(4)),
            "served_bytes_probe": int(probe_serving[0].group(4)) if probe_serving else "",
            "join_to_state_ms": round((t_state - t_join) * 1000, 1),
            "join_answer_ms": round((t_joined - t_join) * 1000, 1),
            "joiner_path": path,
            "snapshot_members": int(adopted[0].group(4)),
            "donor_hosted_logs": len(models) + 1,
        }
        rows.append(row)
        print(f"N={args.n_logs} run {run}: wire {response_bytes} B in {row['probe_ms']} ms (probe); "
              f"joiner served {row['served_bytes_joiner']} B, serving->adopted {row['transfer_ms']} ms, "
              f"join->state {row['join_to_state_ms']} ms, path {path}", file=sys.stderr)
        if path != "adopted":
            raise SystemExit(f"run {run}: the joiner was not on the fresh-joiner path ({path})")

    mode = "a" if args.append else "w"
    with open(args.out, mode, newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        if not args.append:
            writer.writeheader()
        writer.writerows(rows)
    print(f"N={args.n_logs}: median wire {statistics.median(r['response_bytes'] for r in rows)} B, "
          f"median transfer {statistics.median(r['transfer_ms'] for r in rows)} ms, "
          f"median probe {statistics.median(r['probe_ms'] for r in rows)} ms", file=sys.stderr)


if __name__ == "__main__":
    main()
