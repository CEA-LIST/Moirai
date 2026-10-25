#!/usr/bin/env python3
"""NW.23: NW.22's start-stall harness (vault results/NW.22/harness.txt, stall_harness.py as
run), ported unchanged in what it starts and how it classifies; vault spec/items/NW.23.md,
section 4.3.

Start model_node clusters the way NW.02's launcher does (moirai-interp/tests/nw02/launch.rs
173-239) with a witness probe beside every node as p6.sh's wrapper starts it
(moirai-interp/tests/nw16a/node_wrapper.sh 11-15), one cluster at a time, and classify every
start as ok, port loss, stall or other.

Per cluster of N nodes:
  1. spawn every node with REPLICA_ID, LISTEN_PORT=0, HTTP_PORT=0, PORTS_FILE, PEERS_FILE,
     LOG_ID (ST.05's), METAMODEL_DIR, MOIRAI_TRANSPORT=zenoh, HTTP_BIND_ADDR=127.0.0.1;
     stdout and stderr to one log file; the witness probe beside it, told the node's pid;
  2. wait for every PORTS_FILE (50 ms poll, 30 s);
  3. write every PEERS_FILE atomically, every other node in order, `id:127.0.0.1:port`;
  4. give each node DEADLINE seconds from that write to print its first line ("Zenoh
     transport in session") and answer /api/health as itself.
Classes: ok; port_loss (exited, "cannot open the Zenoh session listening on port"); stall
(alive, no first line at the deadline); other_exit, no_health, pre_report, not_started.
A stall is captured (/proc, sockets, a backtrace of every thread through libbtdump.so's
SIGUSR2 handler) and watched for a late first line until LATE seconds, then killed.

Added for NW.23, none of which changes a start: every record names the binary and its arm;
before every round the harness looks for cargo, rustc and docker processes (a load step runs
alone) and records what it saw and the one-minute load average; SIGTERM and SIGINT kill every
node and probe of the round in progress before the harness exits.
"""
import argparse
import ctypes
import json
import os
import signal
import subprocess
import threading
import time
import urllib.request

LOG_ID = "57015701570157015701570157015701"
FIRST = "Zenoh transport in session"
LOSS = "cannot open the Zenoh session listening on port"
NEIGHBOURS = "^(cargo|rustc|docker|docker-buildx|docker-compose|buildkitd)$"
libc = ctypes.CDLL(None, use_errno=True)
SYS_TGKILL = 234
SIGUSR2 = 12


def now():
    return time.monotonic()


def read(path, limit=None):
    try:
        with open(path, "r", errors="replace") as f:
            return f.read() if limit is None else f.read(limit)
    except OSError:
        return ""


def neighbours():
    out = subprocess.run(["pgrep", "-a", NEIGHBOURS], capture_output=True, text=True).stdout
    return [l[:160] for l in out.splitlines() if l.strip()]


def proc_snapshot(pid):
    lines = []
    status = read(f"/proc/{pid}/status")
    keep = ("State", "Threads", "VmRSS", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches")
    lines += [l for l in status.splitlines() if l.split(":")[0] in keep]
    lines.append(f"wchan(main)={read(f'/proc/{pid}/wchan')}")
    try:
        tids = sorted(int(t) for t in os.listdir(f"/proc/{pid}/task"))
    except OSError:
        tids = []
    for tid in tids:
        stat = read(f"/proc/{pid}/task/{tid}/stat")
        rest = stat.rsplit(")", 1)[-1].split()
        state, utime, stime = (rest[0], rest[11], rest[12]) if len(rest) > 12 else ("?", "?", "?")
        comm = read(f"/proc/{pid}/task/{tid}/comm").strip()
        wchan = read(f"/proc/{pid}/task/{tid}/wchan").strip()
        lines.append(f"tid={tid} comm={comm} state={state} wchan={wchan} utime={utime} stime={stime}")
    return lines


def sockets(pid, port):
    try:
        out = subprocess.run(["ss", "-tanp"], capture_output=True, text=True, timeout=10).stdout
    except Exception as e:  # noqa: BLE001
        return [f"ss failed: {e}"]
    return [l for l in out.splitlines() if f"pid={pid}," in l or f":{port} " in l]


def health(port, rid):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=2) as r:
            if r.status != 200:
                return False
            body = json.loads(r.read().decode())
            return body.get("replica_id") == rid
    except Exception:  # noqa: BLE001
        return False


class Node:
    def __init__(self, gdir, rid):
        self.rid = rid
        self.log = os.path.join(gdir, f"{rid}.log")
        self.ports = os.path.join(gdir, f"{rid}.ports.json")
        self.peers = os.path.join(gdir, f"{rid}.peers")
        self.witness = os.path.join(gdir, f"{rid}.witness")
        self.proc = None
        self.probe = None
        self.rec = {"id": rid}


LIVE = []  # the nodes of the round in progress, for the signal handler


def kill_all(nodes):
    for n in nodes:
        for p in (n.proc, n.probe):
            if p is not None and p.poll() is None:
                p.kill()
    for n in nodes:
        for p in (n.proc, n.probe):
            if p is not None:
                p.wait()


def run_group(cfg, gdir, tag, results, lock):
    os.makedirs(os.path.join(gdir, "mm"), exist_ok=True)
    os.makedirs(os.path.join(gdir, "bt"), exist_ok=True)
    ids = [f"n{i}" for i in range(cfg.size)]
    nodes = [Node(gdir, rid) for rid in ids]
    LIVE.extend(nodes)
    base = {
        "PATH": "/usr/bin:/bin",
        "HOME": os.environ.get("HOME", "/tmp"),
        "LANG": "C.UTF-8",
        "TMPDIR": cfg.tmpdir,
    }
    try:
        t0 = now()
        for n in nodes:
            env = dict(base)
            env.update({
                "REPLICA_ID": n.rid, "LISTEN_PORT": "0", "HTTP_PORT": "0",
                "PORTS_FILE": n.ports, "PEERS_FILE": n.peers, "LOG_ID": LOG_ID,
                "METAMODEL_DIR": os.path.join(gdir, "mm"), "MOIRAI_TRANSPORT": "zenoh",
                "HTTP_BIND_ADDR": "127.0.0.1",
                "NW22_BT_DIR": os.path.join(gdir, "bt"), "LD_PRELOAD": cfg.preload,
            })
            out = open(n.log, "wb")
            n.proc = subprocess.Popen([cfg.bin], env=env, stdin=subprocess.DEVNULL,
                                      stdout=out, stderr=out, cwd=gdir)
            out.close()
            n.rec["pid"] = n.proc.pid
            penv = dict(base)
            penv.update({"NW16A_PROBE_PORTS_FILE": n.ports, "NW16A_PROBE_ID": n.rid,
                         "NW16A_PROBE_PID": str(n.proc.pid), "NW16A_PROBE_OUT": n.witness})
            n.probe = subprocess.Popen([cfg.probe_bin, "--ignored", "--exact", "probe", "--quiet"],
                                       env=penv, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, cwd=gdir)
        # 2. await every ports file
        reported = {}
        deadline = now() + 30
        failed = None
        while len(reported) < len(nodes) and failed is None:
            for n in nodes:
                if n.rid in reported:
                    continue
                try:
                    with open(n.ports) as f:
                        p = json.load(f)
                    if p.get("replica_id") == n.rid and p.get("pid") == n.proc.pid:
                        reported[n.rid] = p
                        n.rec["listen"] = p["listen_port"]
                        n.rec["http"] = p["http_port"]
                        n.rec["t_ports_ms"] = round((now() - t0) * 1000)
                        continue
                except (OSError, ValueError):
                    pass
                if n.proc.poll() is not None:
                    failed = f"{n.rid} exited {n.proc.returncode} before reporting"
            if now() > deadline:
                failed = "ports not all reported within 30 s"
            if len(reported) < len(nodes) and failed is None:
                time.sleep(0.05)
        if failed is not None:
            for n in nodes:
                if n.rid not in reported:
                    n.rec["class"] = "pre_report"
                    n.rec["detail"] = failed
                    n.rec["log"] = read(n.log, 2000)
                    if n.proc.poll() is None:
                        n.rec["snapshot"] = proc_snapshot(n.proc.pid)
                else:
                    n.rec["class"] = "not_started"  # launch failed for its group
            return nodes
        # 3. peers files, in order, atomically
        for i, n in enumerate(nodes):
            text = ",".join(f"{o.rid}:127.0.0.1:{reported[o.rid]['listen_port']}"
                            for j, o in enumerate(nodes) if j != i)
            staging = n.peers.rsplit(".", 1)[0] + ".staging"
            with open(staging, "w") as f:
                f.write(text)
            os.rename(staging, n.peers)
        t_peers = now()
        # 4. first line and health within the deadline
        pending = set(ids)
        while pending and now() < t_peers + cfg.deadline:
            for n in nodes:
                if n.rid not in pending:
                    continue
                if "t_first_ms" not in n.rec and FIRST in read(n.log, 8192):
                    n.rec["t_first_ms"] = round((now() - t_peers) * 1000)
                if "t_first_ms" in n.rec and health(n.rec["http"], n.rid):
                    n.rec["t_health_ms"] = round((now() - t_peers) * 1000)
                    n.rec["class"] = "ok"
                    pending.discard(n.rid)
                    continue
                if n.proc.poll() is not None:
                    text = read(n.log, 4000)
                    n.rec["exit"] = n.proc.returncode
                    n.rec["class"] = "port_loss" if LOSS in text else "other_exit"
                    n.rec["log"] = text[:2000]
                    pending.discard(n.rid)
            if pending:
                time.sleep(0.05)
        for n in nodes:
            if n.rid not in pending:
                continue
            text = read(n.log, 4000)
            if FIRST in text:
                n.rec["class"] = "no_health"
                n.rec["log"] = text[:2000]
                continue
            # a stall: capture before anything else touches it
            pid = n.proc.pid
            n.rec["class"] = "stall"
            n.rec["log_bytes"] = len(read(n.log))
            cap = [f"# stall {tag} {n.rid} pid={pid} listen={n.rec['listen']} http={n.rec['http']} bin={cfg.bin}",
                   f"# t={time.strftime('%Y-%m-%dT%H:%M:%S')} deadline={cfg.deadline}s after peers file",
                   "## snapshot 1"] + proc_snapshot(pid)
            cap += ["## sockets (ss -tanp, this pid or its port)"] + sockets(pid, n.rec["listen"])
            cap += ["## peers file", read(n.peers)]
            cap += ["## maps (model_node, libc)"] + [l for l in read(f"/proc/{pid}/maps").splitlines()
                                                    if ("model_node" in l or "libc.so" in l) and "r-xp" in l]
            time.sleep(5)
            cap += ["## snapshot 2 (5 s later)"] + proc_snapshot(pid)
            try:
                tids = sorted(int(t) for t in os.listdir(f"/proc/{pid}/task"))
            except OSError:
                tids = []
            for tid in tids:
                libc.syscall(SYS_TGKILL, pid, tid, SIGUSR2)
                time.sleep(0.2)
            time.sleep(2)
            cap += ["## backtraces (raw, SIGUSR2 per thread)", read(os.path.join(gdir, "bt", f"bt.{pid}.txt"))]
            late = None
            while now() < t_peers + cfg.late:
                if FIRST in read(n.log, 8192):
                    late = round((now() - t_peers) * 1000)
                    break
                if n.proc.poll() is not None:
                    break
                time.sleep(0.2)
            n.rec["late_first_ms"] = late
            n.rec["alive_at_late"] = n.proc.poll() is None
            cap += [f"## after the signals: late first line at {late} ms; alive at {cfg.late}s: {n.proc.poll() is None}",
                    "## log", read(n.log, 4000)]
            capfile = os.path.join(cfg.captures, f"stall-{tag}-{n.rid}-{pid}.txt")
            with open(capfile, "w") as f:
                f.write("\n".join(cap) + "\n")
            n.rec["capture"] = capfile
        return nodes
    finally:
        # the nodes stay up while the probes write their lines (at most cfg.hold s, as a suite
        # keeps its cluster up), then every process of the group is killed, by pid
        until = now() + cfg.hold
        while now() < until and any(n.probe is not None and n.probe.poll() is None
                                    and n.rec.get("class") == "ok" for n in nodes):
            time.sleep(0.1)
        kill_all(nodes)
        for n in nodes:
            if n.probe is not None:
                n.rec["witness"] = read(n.witness).strip()[:300]
        with lock:
            for n in nodes:
                n.rec["tag"] = tag
                results.append(n.rec)
        del LIVE[:]


def on_signal(signum, _frame):
    kill_all(list(LIVE))
    raise SystemExit(128 + signum)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--probe-bin", required=True)
    ap.add_argument("--preload", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--captures", required=True)
    ap.add_argument("--tmpdir", required=True)
    ap.add_argument("--arm", required=True)
    ap.add_argument("--label", required=True)
    ap.add_argument("--size", type=int, required=True)
    ap.add_argument("--rounds", type=int, required=True)
    ap.add_argument("--deadline", type=float, default=30)
    ap.add_argument("--late", type=float, default=40)
    ap.add_argument("--hold", type=float, default=10)
    cfg = ap.parse_args()
    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    os.makedirs(cfg.out, exist_ok=True)
    os.makedirs(cfg.captures, exist_ok=True)
    recfile = os.path.join(cfg.out, f"{cfg.label}.jsonl")
    started = time.strftime("%Y-%m-%dT%H:%M:%S")
    t_start = now()
    for r in range(cfg.rounds):
        seen = neighbours()
        load1 = read("/proc/loadavg").split(" ")[0]
        results, lock = [], threading.Lock()
        tag = f"{cfg.label}-r{r}"
        gdir = os.path.join(cfg.out, tag)
        os.makedirs(gdir, exist_ok=True)
        run_group(cfg, gdir, tag, results, lock)
        with open(recfile, "a") as f:
            for rec in results:
                rec.update({"arm": cfg.arm, "size": cfg.size, "bin": cfg.bin,
                            "neighbours": seen, "load1": load1})
                f.write(json.dumps(rec) + "\n")
        classes = {}
        for rec in results:
            classes[rec.get("class", "?")] = classes.get(rec.get("class", "?"), 0) + 1
        bad = [rec for rec in results if rec.get("class") != "ok"]
        print(f"[{cfg.label}] round {r}: {classes} slowest first line "
              f"{max((rec.get('t_first_ms', 0) for rec in results), default=0)} ms"
              + (f" NEIGHBOURS {seen}" if seen else ""), flush=True)
        if not bad:
            subprocess.run(["rm", "-rf", "--", gdir], check=False)
    print(f"[{cfg.label}] done: started {started}, {round(now() - t_start)} s", flush=True)


if __name__ == "__main__":
    main()
