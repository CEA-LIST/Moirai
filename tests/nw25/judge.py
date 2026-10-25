#!/usr/bin/env python3
"""NW.25: the load step's verdict (vault spec/items/NW.25.md, sections 3 and 7), from the
harness's records and the capture signatures. Ported from NW.23's tests/nw23/judge.py at 57c3f1c,
with two arms.

usage: judge.py <load-dir>    reads <load-dir>/runs/*/*.jsonl and <load-dir>/signatures.jsonl

Arms and their registered counts:
  CONTROL  the unpatched model_node built from 9fbb6fd, 16 nodes, 2,400 starts
  CAND16   the candidate built from HEAD,                16 nodes, 4,800 starts
A candidate start counts as good only when it is ok (first line and /api/health as itself within
the deadline) and its witness probe saw the node's token. Everything else in the candidate arm (a
stall of any signature, a port loss, any other class, an ok start nobody witnessed) is a failure.
Stall S's signature cannot appear in a patched binary (the line it names is deleted by the fix), so
a candidate stall counts whatever its stacks show.
The control qualifies when it stalled at least once with stall S's signature in the stacks.

Verdict, in this order:
  FAIL          the candidate arm has any failing start                       (exit 1)
  INCONCLUSIVE  an arm did not reach its count, or a cargo, rustc or docker
                process was seen before a round                              (exit 4)
  UNVERIFIED    the control did not qualify                                   (exit 3)
  PASS          otherwise                                                     (exit 0)
"""
import glob
import json
import os
import sys

d = sys.argv[1]
WANT = {"CONTROL": (16, 2400), "CAND16": (16, 4800)}
recs = []
for p in sorted(glob.glob(os.path.join(d, "runs", "*", "*.jsonl"))):
    recs += [json.loads(l) for l in open(p) if l.strip()]
sigs = {}
sp = os.path.join(d, "signatures.jsonl")
if os.path.exists(sp):
    for l in open(sp):
        if l.strip():
            s = json.loads(l)
            sigs[os.path.basename(s["capture"])] = s

lines, failing, short, disturbed = [], 0, [], 0
ctrl_stalls = ctrl_s = 0
for arm, (size, want) in WANT.items():
    rs = [r for r in recs if r.get("arm") == arm]
    cls = {}
    for r in rs:
        c = r.get("class", "?")
        if c == "ok" and "token=" not in r.get("witness", ""):
            c = "ok_unwitnessed"
        cls[c] = cls.get(c, 0) + 1
    bad = [r for r in rs if r.get("class") != "ok" or "token=" not in r.get("witness", "")]
    sizes = sorted({r.get("size") for r in rs})
    bins = sorted({r.get("bin") for r in rs})
    rounds_disturbed = len({r["tag"] for r in rs if r.get("neighbours")})
    disturbed += rounds_disturbed
    first = sorted(r["t_first_ms"] for r in rs if "t_first_ms" in r)
    p50 = first[len(first) // 2] if first else None
    stalls = [r for r in rs if r.get("class") == "stall"]
    with_s = [r for r in stalls if sigs.get(os.path.basename(r.get("capture", "")), {}).get("S")]
    lines.append(f"ARM {arm} size={sizes} starts={len(rs)}/{want} clusters={len({r['tag'] for r in rs})} "
                 f"classes={dict(sorted(cls.items()))} stalls={len(stalls)} with_S_signature={len(with_s)} "
                 f"rounds_with_neighbours={rounds_disturbed} first_line_ms p50={p50} max={first[-1] if first else None} "
                 f"bin={bins}")
    for r in bad:
        sg = sigs.get(os.path.basename(r.get("capture", "")), {})
        lines.append(f"  {arm} {r['tag']} {r['id']} pid={r.get('pid')} class={r.get('class')} listen={r.get('listen')} "
                     f"first={r.get('t_first_ms')} exit={r.get('exit')} late={r.get('late_first_ms')} "
                     f"alive_at_late={r.get('alive_at_late')} S={sg.get('S')} "
                     f"witness={r.get('witness', '')[:120]!r} capture={os.path.basename(r.get('capture', ''))}")
    if len(rs) != want or sizes != [size]:
        short.append(f"{arm} {len(rs)}/{want} sizes {sizes}")
    if arm == "CONTROL":
        ctrl_stalls, ctrl_s = len(stalls), len(with_s)
    else:
        failing += len(bad)

if failing:
    verdict, code = f"FAIL: {failing} failing candidate starts", 1
elif short or disturbed:
    verdict, code = f"INCONCLUSIVE: arms short {short}, {disturbed} rounds with a neighbour process", 4
elif ctrl_s < 1:
    verdict, code = f"UNVERIFIED: the control stalled {ctrl_stalls} times, {ctrl_s} with stall S's signature", 3
else:
    verdict, code = (f"PASS: candidate 0 of 4800 sixteen-node starts failed; "
                     f"control stalled {ctrl_stalls} times in 2400, {ctrl_s} with stall S's signature"), 0
print("\n".join(lines))
print(f"NW.25 LOAD: {verdict}")
sys.exit(code)
