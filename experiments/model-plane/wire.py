#!/usr/bin/env python3
"""The replication protocol from the host, for the model-plane measurements.

The pattern of `experiments/p2-transfer-size/measure.py`: a probe speaks the
newline-delimited `TransportMessage` JSON a replica speaks, introduces itself
with a `Hello` under an id no replica has heard from, and reads what the
replica puts on the wire. What crosses the wire is what is measured; nothing
here re-derives a frame from a replica's state.

Shared by m-e1 (frames of the seeded workload), m-e3 (B's frames from a
member the replicas have never met, and the HTTP helpers) and m-e4 (a
`StateResponse` for one log). Python 3 standard library only.
"""

import json
import socket
import time
import urllib.error
import urllib.request

POLL_S = 0.002


def http_json(url, payload=None, timeout=10):
    """GET, or POST `payload` as JSON, and parse the JSON answer."""
    data = json.dumps(payload).encode() if payload is not None else None
    headers = {"Content-Type": "application/json"} if data else {}
    request = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode())


def http_status(url, payload):
    """POST and return (status, parsed body), a 4xx answer included."""
    data = json.dumps(payload).encode()
    request = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status, json.loads(response.read().decode())
    except urllib.error.HTTPError as err:
        body = err.read().decode()
        try:
            return err.code, json.loads(body)
        except ValueError:
            return err.code, {"error": body}


def poll_until(predicate, what, timeout_s=60.0, poll_s=POLL_S):
    """Run `predicate` until it answers a non-None value or the deadline passes."""
    deadline = time.monotonic() + timeout_s
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
        time.sleep(poll_s)


def wait_healthy(base):
    poll_until(lambda: True if http_json(f"{base}/api/health")["status"] == "ok" else None,
               f"{base} to answer /api/health", poll_s=0.1)


def wait_mesh(bases, expect):
    """Every replica reports `expect` connected peers."""
    def connected():
        for base in bases:
            peers = http_json(f"{base}/api/peers")["peers"]
            if sum(1 for p in peers if p["status"] == "Connected") < expect:
                return None
        return True
    poll_until(connected, f"the mesh of {len(bases)} replicas", timeout_s=120, poll_s=0.1)


def compact(value):
    return json.dumps(value, separators=(",", ":"))


class Probe:
    """One connection to a replica's listener, introduced as `probe_id`.

    A `Hello` makes the replica treat the socket as a peer: it broadcasts
    every event it originates to it and answers a `StateRequest` from it.
    The probe never answers the replica's own `SyncRequest`, which is what
    keeps its view read-only.
    """

    def __init__(self, addr, probe_id, timeout_s=30):
        host, port = addr.rsplit(":", 1)
        self.sock = socket.create_connection((host, int(port)), timeout=timeout_s)
        self.sock.settimeout(timeout_s)
        self.stream = self.sock.makefile("rb")
        self.probe_id = probe_id
        self.send({"type": "Hello", "id": probe_id, "metadata": None})

    def send(self, message):
        self.sock.sendall((compact(message) + "\n").encode())

    def frames(self, deadline_s):
        """Yield (raw line without its newline, parsed message) until quiet for `deadline_s`."""
        self.sock.settimeout(deadline_s)
        try:
            for line in self.stream:
                yield line.rstrip(b"\n"), json.loads(line)
        except socket.timeout:
            return

    def ask_for_state(self, log_id):
        """Send a `StateRequest` for `log_id`; answer (bytes on the wire, message, seconds)."""
        started = time.perf_counter()
        self.send({"type": "StateRequest", "id": self.probe_id, "log_id": log_id})
        for raw, message in self.frames(60):
            kind = message.get("type")
            if kind == "StateResponse":
                return len(raw), message, time.perf_counter() - started
            if kind == "StateUnavailable":
                raise SystemExit(f"the donor refused the probe: {message['reason']}")
        raise SystemExit("the donor closed the connection without answering")

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


# ---- the models -------------------------------------------------------------

def served_metamodels(base):
    """`{package: {nsURI, digest}}` as `GET /api/metamodels` lists them."""
    listing = http_json(f"{base}/api/metamodels")["metamodels"]
    return {m["package"]: {"nsURI": m["nsURI"], "digest": m["digest"]} for m in listing}


def create_model(base, metamodel):
    status, body = http_status(f"{base}/api/models", {"metamodel_id": metamodel})
    if status != 201:
        raise SystemExit(f"{base} did not create a model: {status} {body}")
    return body["model_id"]


def join_model(base, model_id, metamodel):
    status, body = http_status(f"{base}/api/models", {"model_id": model_id, "metamodel_id": metamodel})
    if status != 200:
        raise SystemExit(f"{base} did not join {model_id}: {status} {body}")


def model_state(base, model_id):
    return http_json(f"{base}/api/model/{model_id}/state", timeout=10)


def apply_to_model(base, model_id, op):
    reply = http_json(f"{base}/api/model/{model_id}/op", op)
    if reply.get("success") is not True:
        raise SystemExit(f"{base} refused {compact(op)} on {model_id}: {reply}")


def event_frame(origin, seq, log_id, op):
    """The `Event` frame a replica `origin`, alone in its own view, puts on the
    wire for its `seq`th operation on `log_id`: the shape M-E1 reads off the
    socket, with a one-column version vector, so a receiver delivers it as
    soon as it holds the origin's previous one."""
    event = {
        "id": {"idx": 0, "seq": seq, "resolver": [origin]},
        "lamport": seq,
        "op": op,
        "version": {"entries": [seq], "origin_idx": 0, "resolver": [origin]},
    }
    return {"type": "Event",
            "event": {"payload": {"Event": event}, "resolver": [origin], "log_id": log_id}}


# ---- the slot every write lands in ------------------------------------------

# The path, from the root, of a string attribute each package's own descriptor
# declares. The node checks an operation submitted through the adapter against
# the model's descriptor at the intake and refuses a root key the root class
# does not declare, or a string attribute written as a number (mp30 in the
# generated crate's tests), so every write a harness makes into a registered
# model lands here, as `docker/compose/drive.sh`'s `model_op_for` and the e2e
# suite's `slot_of` do: the behaviour tree's `Root.main` is a `BehaviorTree`,
# whose `ID` is a string; every root class of SimpleUML is a `ModelElement`,
# whose `name` is a string. A string is written one character per
# `String.Insert` on this wire.
SLOTS = {"behaviortree": ("main", "ID"), "simpleuml": ("name",)}


def slot_of(package):
    try:
        return SLOTS[package]
    except KeyError:
        raise SystemExit(f"no slot for the `{package}` package; known: {sorted(SLOTS)}") from None


def slot_write(package, inner):
    """`inner`, a string operation, wrapped for the package's slot: an
    `Object.Update` per key of the path, posted at the root."""
    op = inner
    for key in reversed(slot_of(package)):
        op = {"Object": {"Update": [key, op]}}
    return {"JsonKind": op}


def slot_insert(package, ch, pos=0):
    return slot_write(package, {"String": {"Insert": {"content": ch, "pos": pos}}})


def slot_string(wire_state, package):
    """The string in the package's slot of a `GET /api/model/{id}/state` body, or None."""
    try:
        node = wire_state["json"]
        for key in slot_of(package):
            node = node["Value"]["Object"][key]
        return "".join(node["Value"]["String"])
    except (KeyError, TypeError):
        return None


# ---- the seeded operations --------------------------------------------------

def seeded_ops(rng, count, package):
    """`count` operations into the slot of a model of `package`: one seeded
    letter each, inserted at position 0.

    The shape of the workload generator in `moirai-network/src/workload.rs`
    restricted to what the descriptor admits at the slot: inserts at
    position 0 of a text, so concurrent inserts collide. No counter, since
    neither descriptor declares a numeric attribute at its root and a string
    attribute written as a number is refused at the intake.
    """
    return [slot_insert(package, chr(ord("a") + rng.randrange(26))) for _ in range(count)]
