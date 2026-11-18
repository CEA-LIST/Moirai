#!/usr/bin/env python3
"""EC.03c2: EC.03's schedules driven through model_node processes over HTTP.

Spec: cea-cdrt-knowledge/spec/items/EC.03c2.md. One run of one model_node
binary over the schedules EC.03's recorder wrote (ec03/record.rs, checked
against ec03/schedules.sha256 by run.sh); run.sh runs it on 748a185's
model_node and on the candidate's and compares the two traces line by line.

Per schedule, four nodes a, b, c, d on loopback, meshed by NW.02's contract
(LISTEN_PORT=0, HTTP_PORT=0, PORTS_FILE, PEERS_FILE), every bind made on
loopback by the preloaded shim. `a` creates the model under the schedule's
descriptor (POST /api/models) and `b`, `c` join it. The schedule's sends are
then posted on their senders in rounds: every link paused (POST
/api/pause/<peer>, both ends), each send posted on its sender (an operation on
/api/model/<id>/op, the rebase on /api/model/<id>/migrate, strategy rebase,
the schedule's drops) and the sender read; then the links resume within each
group and the harness waits until every node hosting the model has delivered
every operation its group accepted, nothing pending (GET .../metrics), and
reads them all. A round ends before a send whose sender the schedule had
delivered something to since the round began, so nodes write concurrently
about as often as the recorder's replicas did, and every node writes on a
state fixed by the schedule alone: a trace does not depend on timing. The
share of writes where EC.03's recorder kept `c` silent (25 % to 60 %) keeps
`c` cut off from the others; at its end `d` joins the model by state transfer
from a, b (under the rebase, in the rebase schedules that sent it in that
share, which c has not delivered), then `c` comes back and all four settle.
Each read is the model's state, identified state and migration status, the
model's id replaced by `MODEL`.

One trace line per answer and per read:
    <schedule> <step> <node> <what> <sha256 of the text, 16 hex>
Exit 0 when every schedule ran; 2 when the instrument failed (a node did not
start or mesh, a wait timed out); the trace says where.

    nodes.py --bin <model_node> --schedules <dir> --out <trace> --shim <so>
             --scratch <dir> [--only name,name] [--jobs 4]
The transport is the environment's: MOIRAI_TRANSPORT unset is Zenoh.
"""
import argparse
import concurrent.futures
import hashlib
import io
import json
import os
import pathlib
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

IDS = ["a", "b", "c", "d"]
WAIT = 120.0


class Broken(Exception):
    """The instrument failed: nothing after it in the schedule is compared."""


def http(port, method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


def until(what, check):
    deadline = time.monotonic() + WAIT
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.005)
    raise Broken(f"timeout: {what}")


class Cluster:
    def __init__(self, binary, shim, scratch, descriptors):
        self.dir = scratch
        shutil.rmtree(scratch, ignore_errors=True)
        (scratch / "metamodels").mkdir(parents=True)
        for n, text in enumerate(descriptors):
            (scratch / "metamodels" / f"d{n}.json").write_text(text)
        self.procs, self.http, self.listen = {}, {}, {}
        for node in IDS:
            env = {k: v for k, v in os.environ.items()
                   if k not in ("PEERS", "BOOTNODE_URL", "DASHBOARD_URL", "METAMODEL_PATH",
                                "ADVERTISE_ADDR", "ZENOH_ROUTER", "ZENOH_SCOUTING")}
            env.update(REPLICA_ID=node, LISTEN_PORT="0", HTTP_PORT="0", HTTP_BIND_ADDR="127.0.0.1",
                       PORTS_FILE=str(scratch / f"{node}.ports"),
                       PEERS_FILE=str(scratch / f"{node}.peers"),
                       METAMODEL_DIR=str(scratch / "metamodels"), LD_PRELOAD=shim)
            log = open(scratch / f"{node}.log", "w")
            self.procs[node] = subprocess.Popen([binary], env=env, stdout=log,
                                                stderr=subprocess.STDOUT, cwd=scratch)
        for node in IDS:
            ports = scratch / f"{node}.ports"
            until(f"{node} reports its ports", lambda: ports.exists() and self.dead() is None)
            reported = json.loads(ports.read_text())
            self.http[node], self.listen[node] = reported["http_port"], reported["listen_port"]
        for node in IDS:
            peers = ",".join(f"{p}:127.0.0.1:{self.listen[p]}" for p in IDS if p != node)
            staging = scratch / f"{node}.peers.staging"
            staging.write_text(peers)
            staging.rename(scratch / f"{node}.peers")
        for node in IDS:
            until(f"{node} sees its three peers", lambda: self.peers(node) >= 3)

    def dead(self):
        return next((n for n, p in self.procs.items() if p.poll() is not None), None)

    def peers(self, node):
        if self.dead() is not None:
            raise Broken(f"node {self.dead()} exited")
        status, text = http(self.http[node], "GET", "/api/peers")
        return len(json.loads(text)["peers"]) if status == 200 else 0

    def stop(self):
        for proc in self.procs.values():
            proc.kill()
            proc.wait()


class Run:
    def __init__(self, name, cluster, trace):
        self.name, self.c, self.trace = name, cluster, trace
        self.model = None
        self.hosts = set()
        self.cut = False  # c cut off from a, b, d
        self.known = {n: set() for n in IDS}  # operations each node must have delivered
        self.ops = 0

    def line(self, step, node, what, text):
        if self.model:
            text = text.replace(self.model, "MODEL")
        digest = hashlib.sha256(text.encode()).hexdigest()[:16]
        self.trace.write(f"{self.name} {step} {node} {what} {digest}\n")

    def post(self, step, node, path, body):
        status, text = http(self.c.http[node], "POST", path, body)
        self.line(step, node, f"answer-{status}", text)
        return status, text

    def read(self, step, node):
        for what, path in (("state", "state"), ("identified", "state/identified"),
                           ("migration", "migration")):
            status, text = http(self.c.http[node], "GET", f"/api/model/{self.model}/{path}")
            self.line(step, node, f"{what}-{status}", text)

    def settled(self, node):
        status, text = http(self.c.http[node], "GET", f"/api/model/{self.model}/metrics")
        m = json.loads(text) if status == 200 else {}
        return m.get("delivered_ops") == len(self.known[node]) and m.get("pending_ops") == 0

    def groups(self):
        return [{"a", "b", "d"}, {"c"}] if self.cut else [set(IDS)]

    def links(self, pause):
        """Pause every link, or resume the links of each group; both ends."""
        for group in ([set(IDS)] if pause else self.groups()):
            for node in sorted(group):
                for peer in sorted(group - {node}):
                    verb = "pause" if pause else "resume"
                    status, _ = http(self.c.http[node], "POST", f"/api/{verb}/{peer}")
                    if status != 200:
                        raise Broken(f"{verb} {node}->{peer}: {status}")

    def settle(self, step):
        """Resume the groups' links, wait until each hosting node has delivered
        every operation its group accepted, and read every hosting node."""
        self.links(pause=False)
        for group in self.groups():
            union = set().union(*(self.known[n] for n in group & self.hosts))
            for node in group & self.hosts:
                self.known[node] = set(union)
        for node in sorted(self.hosts):
            until(f"{self.name} step {step}: {node} delivers {len(self.known[node])}",
                  lambda: self.settled(node))
        for node in sorted(self.hosts):
            self.read(step, node)

    def write(self, step, who, op):
        if "Rebase" in op:
            rebase = op["Rebase"]
            status, _ = self.post(step, who, f"/api/model/{self.model}/migrate",
                                  {"metamodel_id": rebase["metamodel_id"], "strategy": "rebase",
                                   "drops": rebase.get("drops", [])})
            ok = status == 200
        else:
            status, text = self.post(step, who, f"/api/model/{self.model}/op", op)
            ok = status == 200 and json.loads(text).get("success") is True
        if ok:
            self.ops += 1
            self.known[who].add(self.ops)
        self.read(step, who)


def replay(name, lines, cluster, trace):
    """Rounds: every link paused, the senders write on their own state, then
    the links of each group resume and the group settles. A round ends before
    a send whose sender the schedule delivered something to since the round
    began, and where the silent share begins or ends."""
    install = next(line for line in lines if "send" in line)["op"]["Install"]
    writes = 1000 if name.startswith("ip12") else 600
    run = Run(name, cluster, trace)
    status, text = http(cluster.http["a"], "POST", "/api/models",
                        {"metamodel_id": install["metamodel_id"]})
    if status != 201:
        raise Broken(f"create: {status} {text}")
    run.model = json.loads(text)["model_id"]
    run.line("create", "a", f"answer-{status}", text)
    run.hosts.add("a")
    run.known["a"].add(0)
    for node in ("b", "c"):
        run.post("join", node, "/api/models", {"model_id": run.model})
        run.hosts.add(node)
    run.settle("join")
    learned, k = set(), -1
    run.links(pause=True)
    for line in lines:
        if "deliver" in line:
            learned.add(line["deliver"])
        if "send" not in line or "Install" in line["op"]:
            continue
        k += 1
        who, silent = line["send"], k < writes and 25 <= k * 100 // writes < 60
        if who in learned or silent != run.cut:
            run.settle(f"{k}:round")
            learned = set()
            if silent and not run.cut:
                run.cut = True
            elif run.cut and not silent:
                # d joins from a, b (a rebase sent in the silent share is not
                # yet stable there), then c comes back.
                run.post(f"{k}:join", "d", "/api/models", {"model_id": run.model})
                run.hosts.add("d")
                run.known["d"] = set(run.known["a"])
                run.settle(f"{k}:join")
                run.cut = False
                run.settle(f"{k}:heal")
            run.links(pause=True)
        run.write(f"{k}", who, line["op"])
    run.settle("end")


def one(path, args):
    """One schedule on its own cluster: its trace text, and whether it broke."""
    name = path.stem
    lines = [json.loads(line) for line in path.read_text().splitlines()]
    texts = [s["op"][k]["descriptor"] for s in lines if "send" in s
             for k in ("Install", "Rebase") if k in s["op"]]
    trace = io.StringIO()
    cluster, broke = None, False
    started = time.monotonic()
    try:
        cluster = Cluster(args.bin, args.shim, pathlib.Path(args.scratch) / name, texts)
        replay(name, lines, cluster, trace)
    except Broken as err:
        broke = True
        trace.write(f"{name} broken {err}\n")
        print(f"BROKEN {name}: {err}", file=sys.stderr)
    finally:
        if cluster:
            cluster.stop()
    print(f"{name}: {time.monotonic() - started:.0f} s", file=sys.stderr, flush=True)
    return trace.getvalue(), broke


def main():
    ap = argparse.ArgumentParser()
    for flag in ("--bin", "--schedules", "--out", "--shim", "--scratch"):
        ap.add_argument(flag, required=True)
    ap.add_argument("--only", default="")
    ap.add_argument("--jobs", type=int, default=4)
    args = ap.parse_args()
    only = set(filter(None, args.only.split(",")))
    files = [p for p in sorted(pathlib.Path(args.schedules).glob("*.jsonl"))
             if not only or p.stem in only]
    # Schedules run side by side, each on its own four nodes; a trace does not
    # depend on timing (every step waits for its group), only the wall clock does.
    with concurrent.futures.ThreadPoolExecutor(args.jobs) as pool:
        results = list(pool.map(lambda p: one(p, args), files))
    with open(args.out, "w") as out:
        for text, _ in results:
            out.write(text)
    sys.exit(2 if any(broke for _, broke in results) else 0)


if __name__ == "__main__":
    main()
