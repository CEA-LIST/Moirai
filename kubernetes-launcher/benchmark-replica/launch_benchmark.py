"""Coordinate one Kubernetes run and retain raw replica logs."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
import http.client
import json
from pathlib import Path
import re
import selectors
import subprocess
import sys
from threading import Thread
import time
from uuid import uuid4

from benchmark_config import ADMIN_PORT, manifest, pod_name, validate
from replica_logs import capture_logs, recover_logs


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def kube(*args, namespace=None, input_text=None, timeout=60):
    command = ["kubectl"]
    if namespace is not None:
        command += ["--namespace", namespace]
    result = subprocess.run(
        [*command, *args], input=input_text, capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode:
        raise RuntimeError(f"kubectl {' '.join(args)}: {result.stderr.strip()}")
    return result.stdout


def discard_output(stream):
    for _ in stream:
        pass


@contextmanager
def forwarding(namespace, replica_id, remaining):
    with subprocess.Popen(
        ["kubectl", "-n", namespace, "port-forward", "--address", "127.0.0.1",
         f"pod/{pod_name(replica_id)}", f":{ADMIN_PORT}"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    ) as process:
        try:
            with selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                if not selector.select(max(0, remaining)):
                    raise TimeoutError(f"port-forward timed out for replica {replica_id}")
                line = process.stdout.readline().strip()
            match = re.fullmatch(r"Forwarding from 127\.0\.0\.1:(\d+) -> \d+", line)
            if not match:
                raise RuntimeError(f"port-forward failed for replica {replica_id}: {line}")
            # kubectl also prints a line for each forwarded HTTP request.
            Thread(target=discard_output, args=(process.stdout,), daemon=True).start()
            yield int(match[1])
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()


def admin(port, action="status", method="GET"):
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
    try:
        conn.request(method, f"/api/benchmark/{action}")
        response = conn.getresponse()
        body = response.read()
        if response.status not in (200, 202):
            raise RuntimeError(f"{action} returned HTTP {response.status}: {body.decode()}")
        return json.loads(body)
    finally:
        conn.close()


def until(check, deadline, label):
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.25)
    raise TimeoutError(label)


@contextmanager
def timed(timings, name):
    start = time.monotonic()
    try:
        yield
    finally:
        elapsed = time.monotonic() - start
        timings[name] = round(elapsed, 3)
        print(f"{name}: {elapsed:.3f} s", flush=True)


def cluster_run(namespace, config, replica_logs, timings):
    count = config["n_replicas"]
    healthy = set(range(count))
    fault_at = config["fault_after_s"]
    deadline = time.monotonic() + config["startup_timeout_s"]
    # Wait for the admin server to bind before opening a tunnel to each pod.
    kube(
        "wait", "--for=condition=Ready", "pod", "-l", "app=moirai-commit-benchmark",
        f"--timeout={config['startup_timeout_s']}s", namespace=namespace,
        timeout=config["startup_timeout_s"] + 5,
    )

    with ExitStack() as stack, ThreadPoolExecutor(max_workers=min(count, 32)) as pool:
        captures = stack.enter_context(capture_logs(namespace, count, replica_logs))
        ports = {
            index: stack.enter_context(forwarding(namespace, index, deadline - time.monotonic()))
            for index in sorted(healthy)
        }

        def fanout(action="status", method="GET"):
            ids = sorted(healthy)
            replies = list(pool.map(lambda index: admin(ports[index], action, method), ids))
            if any(reply["replica_id"] != index for index, reply in zip(ids, replies)):
                raise RuntimeError("an admin response came from the wrong replica")
            return replies

        until(lambda: all(result["peers_connected"] and result["phase"] == "ready"
                          for result in fanout()), deadline, "replication connections did not become ready")
        fanout("start", "POST")
        started = time.monotonic()
        print(f"Started local workloads on {count} replicas", flush=True)

        def workloads_done():
            if fault_at is not None and 0 in healthy and time.monotonic() - started >= fault_at:
                admin(ports[0], "crash", "POST")
                healthy.remove(0)
                print("Injected a crash at replica 0", flush=True)
            if fault_at is not None and 0 in healthy:
                return False
            return all(result["phase"] == "drained" for result in fanout())

        until(workloads_done, started + config["workload"]["duration_s"] + config["drain_timeout_s"],
              "local workload queues did not drain")
        print(f"Workload queues drained. Allowing {config['grace_s']} s for commitment", flush=True)
        with timed(timings, "commit_grace"):
            time.sleep(config["grace_s"])
        with timed(timings, "seal_acknowledgments"):
            fanout("seal", "POST")
            until(lambda: all(result["phase"] == "sealed" for result in fanout()),
                  time.monotonic() + 30, "replicas did not seal their logs")
        if fault_at is not None:
            def crashed():
                pod = json.loads(kube("get", "pod", pod_name(0), "-o", "json",
                                      namespace=namespace, timeout=5))
                return any(status.get("state", {}).get("terminated", {}).get("exitCode") == 99
                           for status in pod["status"].get("containerStatuses", []))
            with timed(timings, "crash_verification"):
                until(crashed, time.monotonic() + 15, "replica 0 did not confirm the injected crash")

        with timed(timings, "final_log_delivery"):
            reports = recover_logs(captures, 0 if fault_at is not None else None)
            write_json(replica_logs.parent / "log_collection.json", reports)
            failed = [index for index, report in reports.items() if "error" in report]
            if failed:
                raise RuntimeError(f"could not recover complete replica logs: {', '.join(failed)}")

        # Every replica's saved file has been validated before deletion.
        with timed(timings, "pod_delete_request"):
            kube("delete", "pod", "-l", "app=moirai-commit-benchmark",
                 "--wait=false", namespace=namespace, timeout=15)
        with timed(timings, "tunnel_and_log_stream_cleanup"):
            stack.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--output", type=Path, help="New directory for the run")
    args = parser.parse_args()
    config = validate(json.loads(args.config.read_text(encoding="utf-8")))
    namespace = f"moirai-bench-{uuid4().hex[:12]}"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    default_root = Path(__file__).resolve().parents[2] / "kubernetes-benchmark" / "results"
    output = (args.output or default_root / f"{config['simulation_name']}-{stamp}-{namespace[-6:]}").resolve()
    output.mkdir(parents=True, exist_ok=False)
    write_json(output / "experiment.json", config)
    resources = manifest(config)
    write_json(output / "manifest.json", resources)
    print(f"Run directory: {output}", flush=True)
    replica_logs = output / "replicas"
    replica_logs.mkdir()
    created = False
    error = None
    warnings = []
    timings = {}
    try:
        kube("create", "namespace", namespace)
        created = True
        kube("create", "-f", "-", namespace=namespace, input_text=json.dumps(resources))
        cluster_run(namespace, config, replica_logs, timings)
    except (Exception, KeyboardInterrupt) as failure:
        error = str(failure) or type(failure).__name__
    finally:
        if created:
            try:
                if error:
                    try:
                        (output / "cluster-status.txt").write_text(
                            kube("get", "pods", "-o", "wide", namespace=namespace), encoding="utf-8",
                        )
                    except (RuntimeError, OSError, subprocess.SubprocessError) as failure:
                        warnings.append(str(failure))
            finally:
                try:
                    with timed(timings, "namespace_delete_request"):
                        kube(
                            "delete", "namespace", namespace,
                            "--ignore-not-found=true",
                            "--wait=true",
                            "--timeout=120s",
                            timeout=130,
                        )
                except (RuntimeError, OSError, subprocess.SubprocessError) as failure:
                    warnings.append(f"cleanup of {namespace}: {failure}")
        write_json(output / "run.json", {
            "benchmark_version": 3,
            "namespace": namespace,
            "shutdown_timings_s": timings,
            "complete": error is None and not warnings,
            "error": error,
            "warnings": warnings,
            "faulted_replica": 0 if config["fault_after_s"] is not None else None,
        })
    if error or warnings:
        print("\n".join(filter(None, [error, *warnings])), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (RuntimeError, ValueError, OSError, subprocess.SubprocessError) as failure:
        sys.exit(str(failure))
