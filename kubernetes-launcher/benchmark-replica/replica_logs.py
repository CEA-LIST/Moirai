"""Capture live replica output and recover complete logs before Pod deletion."""

from contextlib import contextmanager
import json
import subprocess
import time


def records(path, allow_partial_tail=False):
    lines = path.read_text(encoding="utf-8").splitlines()
    result = []
    for index, line in enumerate(lines):
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            if allow_partial_tail and index == len(lines) - 1:
                break
            raise ValueError(f"malformed JSON in {path.name}, line {index + 1}")
        result.append(event)
    return result


def validate(events, replica_id, faulted):
    if any(event.get("replica_id") != replica_id for event in events):
        raise ValueError("unexpected replica ID")
    starts = [event for event in events if event.get("type") == "benchmark_start"]
    if len(starts) != 1 or starts[0].get("benchmark_version") != 3:
        raise ValueError("expected one benchmark v3 start record")
    samples = [event for event in events if event.get("type") == "benchmark_sample"]
    if any(b["elapsed_ns"] <= a["elapsed_ns"] for a, b in zip(samples, samples[1:])):
        raise ValueError("sample timestamps are duplicated or out of order")
    observed = sum(len(event["commit_latency_samples"]) for event in samples)
    expected = samples[-1]["committed_local_updates"] if samples else 0
    if observed != expected:
        raise ValueError("missing commit latency samples")
    if faulted:
        return
    if (sum(event.get("final") is True for event in samples) != 1
            or sum(event.get("drained") is True for event in samples) != 1
            or not samples[-1]["final"]):
        raise ValueError("missing final or queue-drain sample")
    done = [event for event in events if event.get("type") == "benchmark_workload_done"]
    if len(done) != 1:
        raise ValueError("missing workload completion record")
    if (done[0]["scheduled"] != done[0]["enqueued"] + done[0]["queue_full"] + done[0]["missed_slots"]
            or done[0]["enqueued"] != samples[-1]["processed_updates"] + samples[-1]["processed_queries"]):
        raise ValueError("inconsistent workload counters")
    pending = [event for event in events if event.get("type") == "benchmark_uncommitted"]
    if sum(len(event["updates"]) for event in pending) != (
        samples[-1]["processed_updates"] - samples[-1]["committed_local_updates"]
    ):
        raise ValueError("missing uncommitted update records")
    if any(event["seal_elapsed_ns"] != samples[-1]["elapsed_ns"] for event in pending):
        raise ValueError("inconsistent pending-update seal timestamps")
    if not events or events[-1].get("type") != "benchmark_sealed":
        raise ValueError("missing final seal record")


def join_records(live, snapshot):
    """Join an older live prefix to a retained snapshot with verified overlap."""
    if not live:
        return snapshot
    if not snapshot:
        return live
    key = lambda event: json.dumps(event, sort_keys=True, separators=(",", ":"))
    live_keys = [key(event) for event in live]
    snapshot_keys = [key(event) for event in snapshot]
    # A snapshot beginning at the benchmark start contains the entire available run.
    if snapshot[0].get("type") == "benchmark_start":
        if snapshot_keys[:min(len(live_keys), len(snapshot_keys))] != live_keys[:min(len(live_keys), len(snapshot_keys))]:
            raise ValueError("live and snapshot logs disagree")
        return snapshot if len(snapshot) >= len(live) else live
    try:
        start = live_keys.index(snapshot_keys[0])
    except ValueError as error:
        raise ValueError("no overlap between live output and retained container log") from error
    overlap = min(len(live) - start, len(snapshot))
    if live_keys[start:start + overlap] != snapshot_keys[:overlap]:
        raise ValueError("gap or disagreement between live output and snapshot")
    return live + snapshot[overlap:]


class LogCapture:
    def __init__(self, namespace, replica_id, folder):
        self.namespace = namespace
        self.replica_id = replica_id
        self.path = folder / f"{replica_id}.log"
        self.live_path = folder / f"{replica_id}.stream.log"
        self.stderr_path = folder / f"{replica_id}.stderr.log"
        self.output = self.live_path.open("w", encoding="utf-8")
        self.errors = self.stderr_path.open("w", encoding="utf-8")
        self.process = None

    def start(self):
        self.process = subprocess.Popen(
            ["kubectl", "-n", self.namespace, "logs", "--follow", "--tail=-1",
             f"benchmark-replica-{self.replica_id}", "-c", "replica"],
            stdout=self.output, stderr=self.errors,
        )

    def stop(self):
        code = self.process.poll() if self.process is not None else None
        if self.process is not None and code is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.output.close()
        self.errors.close()
        return code

    def recover(self, faulted):
        exit_code = self.stop()
        report = {"follower_exit_code": exit_code, "recovered": False, "attempts": []}
        try:
            live = records(self.live_path, allow_partial_tail=True)
        except (OSError, ValueError) as error:
            report["live_error"] = str(error)
            live = []
        try:
            validate(live, self.replica_id, faulted)
            # A crashed Pod has no seal marker. Fetch its final retained output.
            if not faulted:
                self.write(live)
                report["source"] = "live"
                return report
        except (ValueError, KeyError) as error:
            report["live_error"] = str(error)

        for attempt in range(3):
            candidate = self.path.with_suffix(".snapshot.log")
            try:
                with candidate.open("w", encoding="utf-8") as output, self.stderr_path.open("a") as errors:
                    result = subprocess.run(
                        ["kubectl", "-n", self.namespace, "--request-timeout=15s",
                         "logs", "--tail=-1", f"benchmark-replica-{self.replica_id}", "-c", "replica"],
                        stdout=output, stderr=errors, timeout=20,
                    )
                if result.returncode:
                    raise ValueError(f"snapshot kubectl exited with {result.returncode}")
                merged = join_records(live, records(candidate))
                validate(merged, self.replica_id, faulted)
                self.write(merged)
                report.update(recovered=True, source="snapshot")
                return report
            except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
                report["attempts"].append(str(error))
                if attempt < 2:
                    time.sleep(0.5)
        report["error"] = report["attempts"][-1]
        return report

    def write(self, events):
        temporary = self.path.with_suffix(".validated")
        with temporary.open("w", encoding="utf-8") as output:
            for event in events:
                output.write(json.dumps(event, separators=(",", ":")) + "\n")
        temporary.replace(self.path)


@contextmanager
def capture_logs(namespace, count, folder):
    captures = []
    try:
        for index in range(count):
            capture = LogCapture(namespace, index, folder)
            captures.append(capture)
            capture.start()
        yield captures
    finally:
        for capture in captures:
            capture.stop()


def recover_logs(captures, faulted_replica):
    reports = {}
    for capture in captures:
        try:
            report = capture.recover(capture.replica_id == faulted_replica)
        except (OSError, ValueError, KeyError) as error:
            report = {"error": str(error)}
        reports[str(capture.replica_id)] = report
        if "error" in report:
            print(f"Replica {capture.replica_id} log recovery failed: {report['error']}", flush=True)
        else:
            print(f"Replica {capture.replica_id} log validated ({report['source']})", flush=True)
    return reports
