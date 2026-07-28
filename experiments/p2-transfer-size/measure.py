#!/usr/bin/env python3
"""How large does a joiner's state transfer get?

Design §5.1 leaves this open: the transfer is unbounded in principle, because a
long-lived model has an arbitrarily large compacted state. This answers it with
a number for a session-sized model instead of an assumption.

The payload is measured **on the wire**, not estimated. The script speaks the
replication protocol directly — newline-delimited `TransportMessage` JSON — and
records the length of the `StateResponse` line a donor actually sends. It
introduces itself under an id no replica has ever heard from, which is what
makes the donor serve rather than refuse it (scenario T6).

Two replicas, both writing, so the causally stable frontier keeps advancing and
the donor's outbox stays short. That is the realistic shape: a single replica
never stabilises anything, so its outbox holds the whole history and the
measurement would be dominated by a suffix no healthy session ever carries.

The reply is split into its two halves, because they grow for different
reasons:

  log       the compacted CRDT state — grows with the *model*
  snapshot  members, matrix clock, stable frontier, and the events above it —
            grows with the number of members and with how far behind stability
            is running, not with the model

Usage: driven by `run.sh`; see there for the environment it expects.
"""

import argparse
import csv
import json
import socket
import sys
import time
import urllib.error
import urllib.request

# A replica id no member of the session has ever originated an operation from.
# A known one would be refused, correctly — that is the returning-member guard.
STRANGER = "size-probe"

# Every poll in here runs to a deadline. A fixed sleep would either be too
# short, and measure a half-converged session, or too long, and waste minutes.
TIMEOUT_S = 60.0
POLL_S = 0.1


def http_json(url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    request = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.loads(response.read().decode())


def poll_until(predicate, what):
    deadline = time.monotonic() + TIMEOUT_S
    last = None
    while True:
        try:
            value = predicate()
            if value is not None:
                return value
        except (urllib.error.URLError, OSError, ValueError) as exc:
            last = exc
        if time.monotonic() >= deadline:
            raise SystemExit(f"timed out waiting for {what} ({last})")
        time.sleep(POLL_S)


def string_insert(key, pos):
    return {
        "JsonKind": {
            "Object": {
                "Update": [key, {"String": {"Insert": {"content": "x", "pos": pos}}}]
            }
        }
    }


def counter_inc(key):
    return {"JsonKind": {"Object": {"Update": [key, {"Number": {"Inc": 1.0}}]}}}


def ask_for_state(addr, port):
    """Return the donor's `StateResponse`, as raw bytes and as a parsed object."""
    with socket.create_connection((addr, port), timeout=30) as sock:
        sock.settimeout(30)
        hello = {"type": "Hello", "id": STRANGER, "metadata": None}
        request = {"type": "StateRequest", "id": STRANGER}
        sock.sendall((json.dumps(hello) + "\n" + json.dumps(request) + "\n").encode())

        stream = sock.makefile("rb")
        for line in stream:
            message = json.loads(line)
            kind = message.get("type")
            if kind == "StateResponse":
                return len(line.rstrip(b"\n")), message
            if kind == "StateUnavailable":
                raise SystemExit(f"the donor refused the probe: {message['reason']}")
            # A donor answers a `Hello` with a sync request of its own, and may
            # push a batch. Neither is the answer being waited for.
    raise SystemExit("the donor closed the connection without answering")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--donor-http", required=True, help="http://host:port")
    parser.add_argument("--peer-http", required=True, help="http://host:port")
    parser.add_argument("--donor-sync", required=True, help="host:port of the donor's listener")
    parser.add_argument("--steps", type=int, default=12)
    parser.add_argument("--ops-per-step", type=int, default=20)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    donor_host, donor_port = args.donor_sync.rsplit(":", 1)

    poll_until(
        lambda: True if http_json(f"{args.donor_http}/api/health")["status"] == "ok" else None,
        "the donor to answer",
    )
    poll_until(
        lambda: True if http_json(f"{args.peer_http}/api/health")["status"] == "ok" else None,
        "the peer to answer",
    )
    poll_until(
        lambda: True
        if all(
            p["status"] == "Connected"
            for p in http_json(f"{args.donor_http}/api/peers")["peers"]
        )
        else None,
        "the pair to form a mesh",
    )

    rows = []
    applied = 0
    for step in range(args.steps):
        # Both replicas write. A stable frontier is a column-wise minimum, so a
        # member that never contributes pins it at zero and nothing is ever
        # compacted — which would turn this into a measurement of the outbox.
        for i in range(args.ops_per_step // 2):
            pos = step * (args.ops_per_step // 2) + i
            http_json(f"{args.donor_http}/api/op", string_insert("title", pos))
            http_json(f"{args.peer_http}/api/op", counter_inc("revision"))
            applied += 2

        want = applied
        poll_until(
            lambda: True
            if http_json(f"{args.donor_http}/api/metrics")["delivered_ops"] >= want
            else None,
            f"the donor to deliver {want} operations",
        )
        # Stability lags delivery by a message in each direction; give it the
        # chance to catch up, but do not require any particular value — how far
        # behind it runs is part of what is being measured.
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if http_json(f"{args.donor_http}/api/metrics")["retained_ops"] <= 4:
                break
            time.sleep(POLL_S)

        metrics = http_json(f"{args.donor_http}/api/metrics")
        total_bytes, message = ask_for_state(donor_host, int(donor_port))
        log_bytes = len(json.dumps(message["log"], separators=(",", ":")))
        snapshot_bytes = len(json.dumps(message["snapshot"], separators=(",", ":")))
        state_bytes = len(
            json.dumps(http_json(f"{args.donor_http}/api/state"), separators=(",", ":"))
        )

        rows.append(
            {
                "ops": metrics["delivered_ops"],
                "stable_prefix": metrics["stable_prefix"],
                "retained_ops": metrics["retained_ops"],
                "response_bytes": total_bytes,
                "log_bytes": log_bytes,
                "snapshot_bytes": snapshot_bytes,
                "rendered_state_bytes": state_bytes,
            }
        )
        print(
            f"{rows[-1]['ops']:>5} ops  "
            f"stable {rows[-1]['stable_prefix']:>5}  "
            f"retained {rows[-1]['retained_ops']:>3}  "
            f"transfer {total_bytes:>9} B  "
            f"(log {log_bytes} + snapshot {snapshot_bytes})",
            flush=True,
        )

    with open(args.out, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
