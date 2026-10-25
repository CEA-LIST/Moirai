#!/usr/bin/env python3
"""NW.19, the start watcher (vault spec/items/NW.19.md, section 6): sees every node process a
run starts, from outside the tests and whatever launcher started it, and witnesses its transport.
NW.20's watcher (moirai-detune tests/nw20/watch.py at bee3337), copied; the only changes are this
paragraph, the default --names (model_node and Arachne's network_node, which e2e_convergence's
process backend and json_crdt's model_plane tests start), and the reference to the btdump preload
of this branch (tests/nw25/btdump.c).

It polls /proc every --poll-ms. A process is a node of the run when its executable's base name
starts with one of --names and its path lies under one of --exe-dir. For each one it records, the
moment it sees it: its pid and start time, its parent's executable (the test target that started
it, its cargo hash dropped), and from /proc/<pid>/environ the variables that decide its transport
(MOIRAI_TRANSPORT, ZENOH_MODE, ZENOH_ROUTER, ZENOH_CONFIG, BOOTNODE_URL), its REPLICA_ID,
PORTS_FILE, LISTEN_PORT and HTTP_PORT, and whether it is governed (GOV_GENESIS) or carries the
btdump preload. It then watches the PORTS_FILE: the first time it parses while the process lives
is the node's report.

Intended transport: `tcp` when the node's environment says MOIRAI_TRANSPORT=tcp, `zenoh` when it
says zenoh, `default` when it says nothing. In --mode probe every reported node whose intended
transport is `zenoh` or `default` and which is not a client (ZENOH_MODE=client) is witnessed by
NW.16a's probe (moirai-interp/tests/nw16a_witness_probe.rs, NW16A_PROBE_ID='*'): a raw Zenoh
session dials the reported port and must see a liveliness token moirai/*/node/* there. A node on
TCP has no Zenoh session at that port. Nodes the environment pins to TCP are not probed: their
transport is shown by the pinned test's own TCP observable (spec section 4); the watcher records
whether the process holds a LISTEN socket on its reported port. In --mode record nothing is
probed (the baseline at Pp, where probing a TCP listener with Zenoh bytes is avoided).

A node that reported, lives, and whose probe says NONE is a stall suspect: its threads (name,
state, wait channel) are read from /proc, and when it was started with tests/nw25/btdump.c
preloaded and armed (LD_PRELOAD names libbtdump and NW22_BT_DIR is set) every thread is sent
SIGUSR2 and the backtrace file is copied (NW.23's method). SIGUSR2 is never sent otherwise.

Ends when --stop exists: no new process is taken, probes get up to 150 s to finish, and
<out>/nodes.jsonl (one record per node) and <out>/summary.json are written.
"""
import argparse
import ctypes
import json
import os
import re
import shutil
import signal
import subprocess
import time

ENV_KEYS = ("MOIRAI_TRANSPORT", "ZENOH_MODE", "ZENOH_ROUTER", "ZENOH_CONFIG", "BOOTNODE_URL",
            "REPLICA_ID", "PORTS_FILE", "LISTEN_PORT", "HTTP_PORT", "PEERS_FILE", "SESSION_ID",
            "LD_PRELOAD", "NW22_BT_DIR")
HASH = re.compile(r"-[0-9a-f]{16}$")
libc = ctypes.CDLL(None, use_errno=True)
HZ = os.sysconf("SC_CLK_TCK")


def uptime_ticks():
    with open("/proc/uptime") as f:
        return int(float(f.read().split()[0]) * HZ)
SYS_TGKILL = 234


def now_ms():
    return int(time.time() * 1000)


def read(path, binary=False):
    try:
        with open(path, "rb" if binary else "r") as f:
            return f.read()
    except OSError:
        return None


def environ(pid):
    raw = read(f"/proc/{pid}/environ", binary=True)
    if raw is None:
        return None
    env = {}
    for item in raw.split(b"\0"):
        if b"=" in item:
            k, v = item.split(b"=", 1)
            env[k.decode(errors="replace")] = v.decode(errors="replace")
    return env


def start_time(pid):
    stat = read(f"/proc/{pid}/stat")
    if not stat:
        return None
    try:
        return int(stat[stat.rindex(")") + 2:].split()[19])
    except (ValueError, IndexError):
        return None


def ppid_of(pid):
    stat = read(f"/proc/{pid}/stat")
    if not stat:
        return None
    try:
        return int(stat[stat.rindex(")") + 2:].split()[1])
    except (ValueError, IndexError):
        return None


def exe(pid):
    try:
        return os.readlink(f"/proc/{pid}/exe")
    except OSError:
        return None


def target_of(path):
    if not path:
        return "unknown"
    base = os.path.basename(path).replace(" (deleted)", "")
    return HASH.sub("", base)


def ports_of(path, pid):
    text = read(path) if path else None
    if not text:
        return None
    try:
        doc = json.loads(text)
    except ValueError:
        return None
    if doc.get("pid") not in (pid, None):
        return None
    return {"listen_port": doc.get("listen_port"), "http_port": doc.get("http_port"),
            "replica_id": doc.get("replica_id"), "pid": doc.get("pid")}


def listens(pid, port):
    """Whether pid holds a LISTEN socket on port (kernel tables)."""
    inodes = set()
    try:
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                t = os.readlink(f"/proc/{pid}/fd/{fd}")
            except OSError:
                continue
            if t.startswith("socket:["):
                inodes.add(t[8:-1])
    except OSError:
        return None
    for table in ("/proc/net/tcp", "/proc/net/tcp6"):
        for line in (read(table) or "").splitlines()[1:]:
            f = line.split()
            if len(f) > 9 and f[3] == "0A" and int(f[1].rsplit(":", 1)[1], 16) == port and f[9] in inodes:
                return True
    return False


def threads(pid):
    out = []
    try:
        tids = sorted(os.listdir(f"/proc/{pid}/task"), key=int)
    except OSError:
        return out
    for tid in tids:
        stat = read(f"/proc/{pid}/task/{tid}/stat") or ""
        state = stat[stat.rindex(")") + 2:].split()[0] if ")" in stat else "?"
        out.append({"tid": int(tid), "comm": (read(f"/proc/{pid}/task/{tid}/comm") or "").strip(),
                    "state": state, "wchan": (read(f"/proc/{pid}/task/{tid}/wchan") or "").strip()})
    return out


def capture(node, outdir):
    pid = node["pid"]
    node["capture"] = {"t": now_ms(), "threads": threads(pid)}
    env = node["env"]
    maps = read(f"/proc/{pid}/maps") or ""
    armed = "btdump" in env.get("LD_PRELOAD", "") and env.get("NW22_BT_DIR") and "btdump" in maps
    node["capture"]["btdump"] = bool(armed)
    if not armed:
        return
    for t in node["capture"]["threads"]:
        libc.syscall(SYS_TGKILL, pid, t["tid"], signal.SIGUSR2)
    time.sleep(1.5)
    src = os.path.join(env["NW22_BT_DIR"], f"bt.{pid}.txt")
    if os.path.exists(src):
        dst = os.path.join(outdir, f"bt.{pid}.txt")
        shutil.copyfile(src, dst)
        node["capture"]["backtrace"] = dst


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--stop", required=True)
    ap.add_argument("--mode", choices=("probe", "record"), default="probe")
    ap.add_argument("--probe", help="the nw16a_witness_probe test executable")
    ap.add_argument("--names", nargs="+", default=["model_node", "network_node"])
    ap.add_argument("--exe-dir", nargs="+", required=True)
    ap.add_argument("--poll-ms", type=int, default=25)
    ap.add_argument("--step-file", help="a file the driver writes its current step name to")
    ap.add_argument("--probe-intended", nargs="+", default=["zenoh", "default"],
                    help="which intended transports are probed (the rehearsal probes zenoh only)")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    dirs = [os.path.realpath(d) for d in a.exe_dir]
    seen = {}       # (pid, starttime) -> node
    known = set()   # (pid, starttime) of every process looked at
    probes = []
    polls = 0
    t0 = now_ms()
    stopping = None
    while True:
        polls += 1
        if stopping is None and os.path.exists(a.stop):
            stopping = time.monotonic()
        if stopping is None:
            for name in os.listdir("/proc"):
                if not name.isdigit():
                    continue
                pid = int(name)
                st = start_time(pid)
                if st is None or (pid, st) in known:
                    continue
                path = exe(pid)
                if path is None:
                    continue
                base = os.path.basename(path).replace(" (deleted)", "")
                if not any(base.startswith(n) for n in a.names):
                    # A forked child not yet exec'd keeps its parent's executable: look again
                    # while it is young, then never.
                    if uptime_ticks() - st > 2 * HZ:
                        known.add((pid, st))
                    continue
                known.add((pid, st))
                if not any(os.path.realpath(path.replace(" (deleted)", "")).startswith(d + os.sep) for d in dirs):
                    continue
                env = environ(pid) or {}
                ppid = ppid_of(pid)
                pexe = exe(ppid) if ppid else None
                mt = env.get("MOIRAI_TRANSPORT", "")
                step = (read(a.step_file) or "").strip() if a.step_file else ""
                node = {"pid": pid, "start": st, "seen_ms": now_ms(), "exe": path, "ppid": ppid, "step": step,
                        "parent_exe": pexe, "target": target_of(pexe),
                        "env": {k: env[k] for k in ENV_KEYS if k in env},
                        "governed": bool(env.get("GOV_GENESIS")),
                        "intended": "tcp" if mt == "tcp" else "zenoh" if mt == "zenoh" else
                                    "default" if mt == "" else "other:" + mt,
                        "client": env.get("ZENOH_MODE") == "client",
                        "report": None, "report_ms": None, "exit_ms": None, "witness": None,
                        "listens": None}
                seen[(pid, st)] = node
        # reports, exits, probes
        for key, node in seen.items():
            pid = node["pid"]
            alive = start_time(pid) == node["start"]
            if node["exit_ms"] is None and not alive:
                node["exit_ms"] = now_ms()
                continue
            if node["report"] is None and alive:
                r = ports_of(node["env"].get("PORTS_FILE"), pid)
                lp = node["env"].get("LISTEN_PORT", "")
                if r is None and not node["env"].get("PORTS_FILE") and lp.isdigit() and int(lp) and listens(pid, int(lp)):
                    # No PORTS_FILE (Arachne's model_plane gives a fixed LISTEN_PORT): the
                    # report is the kernel's, a LISTEN socket of this pid on LISTEN_PORT, written
                    # as a ports file of the watcher's own for the probe.
                    synth = os.path.join(a.out, f"{pid}-{node['start']}.ports.json")
                    hp = node["env"].get("HTTP_PORT", "0")
                    with open(synth, "w") as f:
                        json.dump({"replica_id": node["env"].get("REPLICA_ID", ""), "pid": pid,
                                   "listen_port": int(lp), "http_port": int(hp) if hp.isdigit() else None}, f)
                    node["env"]["PORTS_FILE"] = synth
                    node["synthetic_report"] = True
                    r = ports_of(synth, pid)
                if r and r.get("listen_port"):
                    node["report"] = r
                    node["report_ms"] = now_ms()
                    node["listens"] = listens(pid, int(r["listen_port"]))
                    if a.mode == "probe" and node["intended"] in a.probe_intended and not node["client"]:
                        wfile = os.path.join(a.out, f"{pid}-{node['start']}.witness")
                        penv = {"PATH": os.environ.get("PATH", "/usr/bin"),
                                "NW16A_PROBE_PORTS_FILE": node["env"]["PORTS_FILE"],
                                "NW16A_PROBE_ID": "*", "NW16A_PROBE_PID": str(pid),
                                "NW16A_PROBE_OUT": wfile}
                        p = subprocess.Popen([a.probe, "--ignored", "--exact", "probe", "--quiet"],
                                             env=penv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                             stderr=subprocess.DEVNULL)
                        probes.append((p, node, wfile))
        for item in list(probes):
            p, node, wfile = item
            if p.poll() is None:
                continue
            probes.remove(item)
            text = (read(wfile) or "").strip()
            node["witness"] = text or f"NONE the probe exited {p.returncode} writing nothing"
            if "token=" not in text and node["exit_ms"] is None and start_time(node["pid"]) == node["start"]:
                capture(node, a.out)
        if stopping is not None and (not probes or time.monotonic() - stopping > 150):
            break
        time.sleep(a.poll_ms / 1000.0)
    for p, node, _ in probes:
        p.kill()
        node["witness"] = node["witness"] or "NONE the probe did not finish within 150 s of the stop"
    with open(os.path.join(a.out, "nodes.jsonl"), "w") as f:
        for node in sorted(seen.values(), key=lambda n: n["seen_ms"]):
            f.write(json.dumps(node, sort_keys=True) + "\n")
    summary = {"polls": polls, "seconds": (now_ms() - t0) / 1000.0, "nodes": len(seen), "mode": a.mode}
    with open(os.path.join(a.out, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1)


if __name__ == "__main__":
    main()
