#!/usr/bin/env python3
"""M-E3: no head-of-line blocking between models sharing a session.

Sixteen models in the session, every replica hosting all of them: model A is
the SimpleUML one and model B the behaviour-tree one. The question is
whether editing A gets slower because B is under load.

Arms, each RUNS runs from the same seed:

  alone             nothing else happens in the session
  alone-again       the same, run again right after the plan's arm, so a
                    session that drifted during it (B's log grows, nothing
                    compacts on a multi-model rig) shows as a change of the
                    baseline and not of the load
  loaded            B is under the driver's load as every replica receives
                    it: the rig's driver (docker/compose/drive.sh) has each
                    of the five replicas apply one operation per
                    OP_INTERVAL seconds and broadcast it, so each replica
                    takes five of B's frames per interval through the one
                    event loop A's frames go through. Here those frames come
                    from a member `load` the replicas have never met, over
                    the replication socket (`wire.py`), five per interval per
                    replica, into B. The plan's arm.
  loaded-http       (supplementary) the same rate through the adapter,
                    drive.sh's own mechanism: one `POST /api/model/B/op` per
                    replica per interval from a background thread. The
                    adapter answers requests one at a time on one thread
                    (http_api.rs), so a state poll queued behind a load POST
                    waits a loop tick that no frame waited; this arm
                    therefore carries the adapter's queueing on top of the
                    loop's, which is why it is not the arm the threshold
                    reads.
  loaded-wire-heavy (supplementary) HEAVY_RATE of B's frames per second per
                    replica on the wire, no pause.
  loaded-http-heavy (supplementary) HEAVY_CLIENTS threads per replica
                    posting into B with no pause, the adapter confound
                    included.

One arm per invocation, in a session of its own: run.sh starts fresh
replicas and registers the sixteen models again for every arm, because a
session ages. Nothing compacts on a multi-model rig (the frontier is pinned;
see the deferral in the design), so A's unstable log grows by OPS_PER_RUN
operations per run and every later delivery walks a longer log; two arms
taken one after the other in one session would compare two ages, not two
loads. The first run of this harness did exactly that and measured a
no-load re-run of `alone` at 1.65 x the first, which is the drift and not
the load. Each arm's row also records A's delivered and retained counts on
the writer at the end of the arm, the size of what every delivery walked.

A run applies OPS_PER_RUN seeded operations to A on the first replica (the
editor), takes the time the last one was answered, and polls every replica's
`GET /api/model/A/state` (one thread each, every 2 ms) until all agree on
the same state, which is the writer's own state after its last write since
nothing else writes A. Per replica: when its state first equalled that;
per run: the last of those, the session's convergence. After every wire
arm the frames it sent are checked delivered: B's state on every replica
carries the load member's counter at the number of frames sent to it.

Threshold, fixed in the plan: median(loaded) <= 1.10 x median(alone).
"""

import argparse
import csv
import json
import random
import statistics
import sys
import threading
import time

sys.path.insert(0, __import__("os").path.join(__import__("os").path.dirname(__file__), ".."))
from wire import (Probe, apply_to_model, compact, counter_inc, create_model, event_frame,  # noqa: E402
                  http_json, join_model, model_state, poll_until, seeded_ops, served_metamodels,
                  wait_healthy, wait_mesh)


class Load:
    """Operations into `model_id` on every replica until stopped."""

    def __init__(self, bases, model_id, interval_s, clients_per_replica=1):
        self.bases, self.model_id, self.interval_s = bases, model_id, interval_s
        self.clients = clients_per_replica
        self.stop = threading.Event()
        self.applied = 0
        self.lock = threading.Lock()
        self.threads = []

    def _tick_loop(self):
        # drive.sh: one round over every replica, then sleep.
        while not self.stop.is_set():
            for base in self.bases:
                apply_to_model(base, self.model_id, counter_inc(f"k_{base.rsplit(':', 1)[-1]}"))
                with self.lock:
                    self.applied += 1
            self.stop.wait(self.interval_s)

    def _hammer(self, base):
        while not self.stop.is_set():
            apply_to_model(base, self.model_id, counter_inc(f"k_{base.rsplit(':', 1)[-1]}"))
            with self.lock:
                self.applied += 1

    def start(self):
        if self.interval_s > 0:
            self.threads = [threading.Thread(target=self._tick_loop, daemon=True)]
        else:
            self.threads = [threading.Thread(target=self._hammer, args=(base,), daemon=True)
                            for base in self.bases for _ in range(self.clients)]
        for thread in self.threads:
            thread.start()

    def end(self):
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=10)
        return self.applied


class WireLoad:
    """B's frames from a member `load` the replicas have never met, over one
    replication connection per replica, `per_replica` frames per `interval_s`
    (a burst per interval, as one round of the driver lands on a replica),
    or a steady `rate` per second when `interval_s` is 0."""

    LOAD_ID = "load"
    KEY = "k_load"

    def __init__(self, sync_addrs, model_id, interval_s, per_replica=5, rate=500.0):
        self.probes = [Probe(addr, self.LOAD_ID) for addr in sync_addrs]
        self.model_id, self.interval_s, self.per_replica, self.rate = model_id, interval_s, per_replica, rate
        self.stop = threading.Event()
        self.seq = 0
        self.thread = None
        # A probe is a peer, so every replica broadcasts its own frames to it
        # as well; they are read and dropped, or the socket's buffer fills
        # and the replica's sends stall, which would be measured as A's
        # convergence.
        self.drains = [threading.Thread(target=self._drain, args=(probe,), daemon=True) for probe in self.probes]

    @staticmethod
    def _drain(probe):
        try:
            for _ in probe.frames(deadline_s=3600):
                pass
        except (OSError, ValueError):
            return

    def _send_one(self):
        self.seq += 1
        frame = event_frame(self.LOAD_ID, self.seq, self.model_id, counter_inc(self.KEY))
        for probe in self.probes:
            probe.send(frame)

    def _run(self):
        if self.interval_s > 0:
            while not self.stop.is_set():
                for _ in range(self.per_replica):
                    self._send_one()
                self.stop.wait(self.interval_s)
        else:
            gap = 1.0 / self.rate
            while not self.stop.is_set():
                self._send_one()
                self.stop.wait(gap)

    def start(self):
        for drain in self.drains:
            drain.start()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def end(self):
        self.stop.set()
        self.thread.join(timeout=10)
        return self.seq * len(self.probes)

    def assert_delivered(self, bases):
        """Every replica delivered every frame: B carries the load counter at `seq` everywhere."""
        want = float(self.seq)
        poll_until(lambda: True if all(counter_of(model_state(b, self.model_id), self.KEY) == want for b in bases) else None,
                   f"every replica to deliver the {self.seq} load frames", timeout_s=60, poll_s=0.05)
        for probe in self.probes:
            probe.close()


def counter_of(wire_state, key):
    """The number under `key` of the root object in a `GET /api/model/{id}/state` body, or None."""
    try:
        return wire_state["json"]["Value"]["Object"][key]["Value"]["Number"]
    except (KeyError, TypeError):
        return None


def watch(base, model_id, target, result, stop):
    """Poll `base` until its state for `model_id` is `target`; record the time."""
    while not stop.is_set():
        try:
            if compact(model_state(base, model_id)) == target:
                result.append(time.perf_counter())
                return
        except Exception:  # noqa: BLE001 - a transient HTTP failure is a retry
            pass
        time.sleep(0.002)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", required=True, help="comma-separated http://host:port; the first is the writer")
    parser.add_argument("--sync", required=True, help="comma-separated host:port listeners, same order")
    parser.add_argument("--ids", required=True, help="comma-separated replica ids, same order")
    parser.add_argument("--n-models", type=int, default=16)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--ops-per-run", type=int, default=20)
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--op-interval", type=float, default=1.0)
    parser.add_argument("--heavy-clients", type=int, default=4)
    parser.add_argument("--heavy-rate", type=float, default=500.0, help="frames per second per replica, loaded-wire-heavy")
    parser.add_argument("--arm", required=True, help="one of alone, alone-again, loaded, loaded-http, loaded-wire-heavy, loaded-http-heavy")
    parser.add_argument("--settle", type=float, default=5.0, help="seconds of load before the first timed run")
    parser.add_argument("--out", required=True)
    parser.add_argument("--summary", required=True, help="one JSON line for the arm")
    args = parser.parse_args()

    bases = args.http.split(",")
    syncs = args.sync.split(",")
    ids = args.ids.split(",")
    for base in bases:
        wait_healthy(base)
    wait_mesh(bases, expect=len(bases) - 1)
    writer = bases[0]

    kinds = served_metamodels(writer)
    order = [("behaviortree", "simpleuml")[i % 2] for i in range(args.n_models)]
    models = []
    for package in order:
        model_id = create_model(writer, kinds[package])
        for base in bases[1:]:
            join_model(base, model_id, kinds[package])
        models.append((package, model_id))
    model_b, model_a = models[0][1], models[1][1]
    for _, model_id in models:
        head = compact(model_state(writer, model_id))
        poll_until(lambda: True if all(compact(model_state(b, model_id)) == head for b in bases) else None,
                   f"every replica to hold model {model_id[:8]}", timeout_s=120, poll_s=0.05)
    print(f"{len(models)} models hosted on {len(bases)} replicas; A={model_a} (simpleuml), B={model_b} (behaviortree)",
          file=sys.stderr)

    origin = time.perf_counter()
    ms = lambda t: round((t - origin) * 1000, 1)  # noqa: E731
    rows = []
    summary = {}
    for arm in [args.arm]:
        load = None
        if arm == "loaded":
            load = WireLoad(syncs, model_b, args.op_interval, per_replica=len(bases))
        elif arm == "loaded-http":
            load = Load(bases, model_b, args.op_interval)
        elif arm == "loaded-wire-heavy":
            load = WireLoad(syncs, model_b, 0.0, rate=args.heavy_rate)
        elif arm == "loaded-http-heavy":
            load = Load(bases, model_b, 0.0, args.heavy_clients)
        elif not arm.startswith("alone"):
            raise SystemExit(f"unknown arm {arm}")
        if load:
            load.start()
            time.sleep(args.settle)
        rng = random.Random(args.seed)
        per_run = []
        for run in range(1, args.runs + 1):
            ops = seeded_ops(rng, args.ops_per_run, f"a{run}")
            for op in ops:
                apply_to_model(writer, model_a, op)
            t_last = time.perf_counter()
            target = compact(model_state(writer, model_a))
            results = {base: [] for base in bases}
            stop = threading.Event()
            watchers = [threading.Thread(target=watch, args=(base, model_a, target, results[base], stop), daemon=True)
                        for base in bases]
            for w in watchers:
                w.start()
            deadline = time.monotonic() + 60
            while any(not results[b] for b in bases):
                if time.monotonic() > deadline:
                    stop.set()
                    raise SystemExit(f"{arm} run {run}: A did not converge within 60s")
                time.sleep(0.001)
            stop.set()
            converged = max(results[b][0] for b in bases)
            for base, rid in zip(bases, ids):
                rows.append({
                    "run": run, "arm": arm, "seed": args.seed, "replica_id": rid, "log_id": model_a,
                    "last_write_ms": ms(t_last), "converged_ms": ms(results[base][0]),
                    "convergence_ms": round((results[base][0] - t_last) * 1000, 1),
                })
            per_run.append((converged - t_last) * 1000)
            time.sleep(0.2)
        applied = load.end() if load else 0
        if isinstance(load, WireLoad):
            load.assert_delivered(bases)
        med10 = statistics.median(per_run[:10])
        metrics = http_json(f"{writer}/api/model/{model_a}/metrics")
        summary[arm] = {"median_ms": statistics.median(per_run), "median_first10_ms": med10,
                        "mean_ms": statistics.mean(per_run),
                        "p90_ms": sorted(per_run)[int(0.9 * (len(per_run) - 1))], "runs": len(per_run),
                        "load_ops": applied, "a_delivered_end": metrics["delivered_ops"],
                        "a_retained_end": metrics["retained_ops"], "a_stable_prefix_end": metrics["stable_prefix"]}
        print(f"{arm}: session convergence of A over {len(per_run)} runs: median {summary[arm]['median_ms']:.1f} ms "
              f"(first ten {med10:.1f}), mean {summary[arm]['mean_ms']:.1f} ms, "
              f"p90 {summary[arm]['p90_ms']:.1f} ms; load put {applied} operations into B"
              + (" (delivered everywhere)" if isinstance(load, WireLoad) else ""), file=sys.stderr)
        time.sleep(1.0)

    with open(args.out, "w", newline="") as handle:
        w = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
    with open(args.summary, "w") as handle:
        for arm, s in summary.items():
            handle.write(json.dumps({"arm": arm, **{k: (round(v, 2) if isinstance(v, float) else v) for k, v in s.items()}}) + "\n")


if __name__ == "__main__":
    main()
