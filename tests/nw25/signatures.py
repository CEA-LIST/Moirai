#!/usr/bin/env python3
"""NW.23: NW.22's signatures.py (vault results/NW.22/harness.txt), generalized to any binary
name and to the vendored zenoh's paths; vault spec/items/NW.23.md, section 4.3.

Symbolizes every stall capture given (one addr2line call over all of them) against the binary
that produced it, writes <capture>.sym.txt beside each capture with the full symbolized stacks,
and prints one JSON object per capture:
  {"capture": ..., "threads": {name: "frame <- frame <- ..."}, "S": bool, "why": ...}
"S" is stall S's signature as NW.22 found it in all 23 of its captures: one thread parked in
the peer hat's route_declare_final at zenoh-1.10.1/src/net/routing/hat/peer/interests.rs:283
(the ZRuntime::Net.block_in_place that PR #2779 deletes) while our ZenohTransport::open waits
for ctrl_lock at zenoh-1.10.1/src/net/routing/dispatcher/face.rs:560.

usage: signatures.py <binary> <capture>...
"""
import json
import os
import re
import subprocess
import sys

binary, caps = sys.argv[1], sys.argv[2:]
PAT = re.compile(re.escape(os.path.basename(binary)) + r"\(\+(0x[0-9a-f]+)\)")
HAT = "zenoh-1.10.1/src/net/routing/hat/peer/interests.rs:283"
LOCK = "zenoh-1.10.1/src/net/routing/dispatcher/face.rs:560"


def threads_of(path):
    text = open(path, errors="replace").read()
    if "## backtraces (raw, SIGUSR2 per thread)" not in text:
        return []
    sec = text.split("## backtraces (raw, SIGUSR2 per thread)", 1)[1].split("## after the signals", 1)[0]
    out = []
    for line in sec.splitlines():
        if line.startswith("=== thread"):
            out.append((line, []))
        elif out and line.strip():
            out[-1][1].append(line.strip())
    return out


def trim(loc):
    loc = re.sub(r"^.*/registry/src/[^/]+/", "", loc)
    loc = re.sub(r"^.*/vendor/(zenoh-1\.10\.1/)", r"\1", loc)
    loc = re.sub(r"^.*/(moirai-[a-z]+/(src|examples|tests)/)", r"\1", loc)
    loc = re.sub(r"^.*/(library|rustlib/src/rust/library)/", "std:", loc)
    return re.sub(r" \(discriminator \d+\)", "", loc)


per = {c: threads_of(c) for c in caps}
offs = sorted({m.group(1) for ts in per.values() for _, fr in ts for l in fr for m in [PAT.search(l)] if m})
res = subprocess.run(["addr2line", "-a", "-f", "-C", "-i", "-e", binary] + offs,
                     capture_output=True, text=True).stdout.splitlines() if offs else []
table, cur, i = {}, None, 0
while i < len(res):
    if re.fullmatch(r"0x[0-9a-f]+", res[i]):
        cur = int(res[i], 16)
        table[cur] = []
        i += 1
        continue
    fn = re.sub(r"::h[0-9a-f]{16}$", "", res[i])
    loc = trim(res[i + 1] if i + 1 < len(res) else "?")
    table[cur].append((fn, loc))
    i += 2

for c, ts in per.items():
    lines, sig, hat, lock = [], {}, False, False
    for head, frames in ts:
        name = re.search(r"name=(\S*)", head).group(1)
        lines.append(head)
        picked, locs = [], []
        for l in frames:
            m = PAT.search(l)
            if not m:
                lines.append("  " + re.sub(r"^.*/", "", l))
                continue
            for k, (fn, loc) in enumerate(table.get(int(m.group(1), 16), [("?", "?")])):
                lines.append(("    " if k else "  ") + f"{fn[:160]}  [{loc}]")
                locs.append((fn, loc))
                ours = loc.startswith("moirai") or "model_node" in loc
                if (ours and not any("[moirai" in x or "model_node" in x for x in picked)) or (
                        len(picked) < 2 and loc.startswith("zenoh") and not loc.startswith("zenoh-runtime")):
                    short = re.sub(r"<([^<>]*?) as [^<>]*?>", r"\1", fn)
                    picked.append(f"{short[:90]} [{loc}]")
        if any(loc.startswith(HAT) and "route_declare_final" in fn for fn, loc in locs):
            hat = True
        if any(loc.startswith(LOCK) for _, loc in locs) and any("ZenohTransport" in fn and "open" in fn for fn, _ in locs):
            lock = True
        first = " <- ".join(picked) if picked else (
            "epoll_wait (idle)" if any("epoll_wait" in l for l in frames) else "?")
        sig[f"{name}#{len(sig)}"] = first
    with open(c[:-4] + ".sym.txt", "w") as f:
        f.write("\n".join(lines) + "\n")
    why = []
    if not ts:
        why.append("no backtraces in the capture")
    if not hat:
        why.append(f"no thread in route_declare_final at {HAT}")
    if not lock:
        why.append(f"ZenohTransport::open not waiting at {LOCK}")
    print(json.dumps({"capture": c, "threads": sig, "S": hat and lock, "why": "; ".join(why)}))
