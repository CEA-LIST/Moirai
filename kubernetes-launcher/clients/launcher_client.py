#!/usr/bin/env python3
"""Generate Add/Remove requests through the shared Kubernetes HTTP Service."""

import argparse
import json
from pathlib import Path
import subprocess
import threading
import time


def parse_bool(value):
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    raise argparse.ArgumentTypeError("expected true or false")


def time_derived_index(size):
    return time.time_ns() % size


def emit_result_record(record):
    # `kubectl logs` remains usable after the container has exited, unlike
    # `kubectl cp`, which requires exec into a running container.
    print("MOIRAI_RESULT " + json.dumps(record, separators=(",", ":")), flush=True)


def make_post_request(item, target_url, update="Add", wait_commit=False):
    url = f"{target_url.rstrip('/')}/api/op"
    if wait_commit:
        url += "?wait_for_commit=true"
    return [
        "curl", "--fail", "--silent", "--show-error", "--max-time", "30",
        "-X", "POST", url,
        "-H", "Content-Type: application/json",
        "-d", json.dumps({update: item}),
    ]


def execute_request(target_url, items, wait_commit, lock, latencies, failures):
    item = items[time_derived_index(len(items))]
    update = ("Add", "Remove")[time_derived_index(2)]
    command = make_post_request(item, target_url, update, wait_commit)

    start_ns = time.time_ns()
    result = subprocess.run(command, text=True, capture_output=True, check=False)
    end_ns = time.time_ns()

    with lock:
        latencies.append([start_ns, end_ns])
        if result.returncode != 0:
            failures.append({
                "start_ns": start_ns,
                "end_ns": end_ns,
                "returncode": result.returncode,
                "stderr": result.stderr.strip(),
            })


def client_subroutine(client_id, target_url, items, n_operations, frequency, wait_commit,
                      output_dir, stop_signal=None):
    lock = threading.Lock()
    latencies = []
    failures = []
    threads = []

    for _ in range(n_operations):
        if stop_signal is not None and stop_signal.is_set():
            break
        thread = threading.Thread(
            target=execute_request,
            args=(target_url, items, wait_commit, lock, latencies, failures),
        )
        thread.start()
        threads.append(thread)

        # Preserve the Podman launcher's pacing rule; this is an approximate
        # randomized delay, not a precise requests-per-second setting.
        period = (1 / frequency) * (time.time_ns() % 200) / 100
        time.sleep(period)

    for thread in threads:
        thread.join()

    output = {
        "client_id": client_id,
        "mode": "committed" if wait_commit else "optimistic",
        "target_url": target_url,
        "latencies": latencies,
        "failed_requests": failures,
    }
    filename = "com_latencies.json" if wait_commit else "op_latencies.json"
    with (output_dir / filename).open("w", encoding="utf-8") as file:
        json.dump(output, file)
    emit_result_record(output)


def run(client_id=1, n_operations=100, frequency=10, optimistic=True,
        consistent=True, target_url="http://moirai-multi-http:8081"):
    for name, value, minimum in (
        ("client_id", client_id, 1),
        ("n_operations", n_operations, 0),
        ("frequency", frequency, 1),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ValueError(f"{name} must be an integer >= {minimum}")
    if not optimistic and not consistent:
        raise ValueError("at least one of optimistic or consistent must be true")
    if not target_url.startswith(("http://", "https://")):
        raise ValueError("target_url must start with http:// or https://")

    input_dir = Path(__file__).resolve().parent
    with (input_dir / "items.json").open(encoding="utf-8") as file:
        items = json.load(file)["random_items"]
    if not items:
        raise ValueError("items.json must contain a nonempty random_items list")

    output_dir = Path("/app/Outputs")
    output_dir.mkdir(parents=True, exist_ok=True)

    if optimistic:
        optimistic_thread = threading.Thread(
            target=client_subroutine,
            args=(client_id, target_url, items, n_operations, frequency,
                  False, output_dir),
        )
        optimistic_thread.start()

    if consistent:
        stop_signal = threading.Event()
        consistent_thread = threading.Thread(
            target=client_subroutine,
            args=(client_id, target_url, items, n_operations, frequency,
                  True, output_dir,
                  stop_signal if optimistic else None),
        )
        consistent_thread.start()

    if optimistic:
        optimistic_thread.join()
    if optimistic and consistent:
        stop_signal.set()
    if consistent:
        consistent_thread.join()

    print(f"Client {client_id} finished; results are in {output_dir}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--client-id", type=int, default=1)
    parser.add_argument("--n-operations", type=int, default=100)
    parser.add_argument("--frequency", type=int, default=10)
    parser.add_argument("--optimistic", type=parse_bool, default=True)
    parser.add_argument("--consistent", type=parse_bool, default=True)
    parser.add_argument("--target-url", default="http://moirai-multi-http:8081")
    args = parser.parse_args()
    run(**vars(args))


if __name__ == "__main__":
    main()
