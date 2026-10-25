#!/usr/bin/env python3
"""NW.19, the per-site accounting of the nodes a run started (vault spec/items/NW.19.md,
sections 4 and 6): reads tests/nw19/watch.py's nodes.jsonl and judges it. NW.20's account
(moirai-detune tests/nw20/account.py at bee3337), copied; the changes are the names and W5.

Every node record names the step it started in, the test target that started it (its parent's
executable), its intended transport (tcp, zenoh, default) and, for a node the watcher probed,
the witness. A node is:
  - live: it reported its ports and lived at least LIVE_MS after the report;
  - short: it reported and was gone within LIVE_MS (a test that stops a node at once, a node lost
    to NW.16a's P5b gap); counted apart, never witnessed here;
  - unreported: it never reported and exited (an expected refusal, a node started without a
    PORTS_FILE and a fixed port the watcher cannot read, or a node that failed to start; the
    test that started it says which);
  - running-unreported: it never reported and was alive when the watcher stopped.

  account.py table <nodes.jsonl>
      the counts per step, target and intended transport, and per class (the baseline tables).
  account.py judge <nodes.jsonl> <pins.tsv> --mode h|rehearsal|pins
      exit 0 only when:
      W1 (h)         every live node whose intended transport is default or zenoh and which is
                     not a client is witnessed: its probe line holds `token=`;
      W1 (rehearsal) every live node whose intended transport is zenoh and which is not a
                     client is witnessed;
      W2             no live node intended default or zenoh carries a capture (silent after
                     its report: a stall suspect);
      W3             for every row of pins.tsv, the number of nodes started in that step by that
                     target with MOIRAI_TRANSPORT=tcp is the registered count (`+N`: at least N,
                     for nodes that may live under the watcher's reach), and the number started
                     there without it is the registered count of the row (`+N` likewise);
      W4             no node names a transport other than tcp or zenoh;
      W5 (h)         at least one live node intended default was probed (a run that witnessed
                     nobody proves nothing about the default).
      --mode pins judges W3 and W4 only (a run whose watcher only records: P3, P4).
"""
import collections
import json
import sys

LIVE_MS = 2000


def load(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


def klass(n):
    if n.get("report"):
        end = n.get("exit_ms")
        if end is not None and end - n["report_ms"] < LIVE_MS:
            return "short"
        return "live"
    return "unreported" if n.get("exit_ms") is not None else "running-unreported"


def witnessed(n):
    return "token=" in (n.get("witness") or "")


def table(nodes):
    rows = collections.Counter()
    for n in nodes:
        rows[(n.get("step", ""), n["target"], n["intended"], klass(n),
              "client" if n.get("client") else "peer",
              "witnessed" if witnessed(n) else ("probed-none" if n.get("witness") else "not-probed"))] += 1
    out = ["step\ttarget\tintended\tclass\tmode\twitness\tcount"]
    for k in sorted(rows):
        out.append("\t".join(k) + f"\t{rows[k]}")
    return out


def pins(path):
    rows = []
    with open(path) as f:
        for line in f:
            if not line.strip() or line.startswith("#"):
                continue
            site, step, target, tcp, other = line.rstrip("\n").split("\t")[:5]
            rows.append((site, step, target, tcp, other))
    return rows


def judge(nodes, pin_rows, mode):
    bad = []
    probed = ("zenoh", "default") if mode == "h" else ("zenoh",) if mode == "rehearsal" else ()
    live_probed = [n for n in nodes if klass(n) == "live" and n["intended"] in probed and not n.get("client")]
    for n in live_probed:
        if not witnessed(n):
            bad.append(f"W1 UNWITNESSED step {n.get('step')} target {n['target']} pid {n['pid']} "
                       f"intended {n['intended']} report {n['report']}: {n.get('witness')}")
        if n.get("capture"):
            bad.append(f"W2 SILENT step {n.get('step')} target {n['target']} pid {n['pid']}: "
                       f"capture {json.dumps(n['capture'])[:600]}")
    count = collections.Counter()
    for n in nodes:
        count[(n.get("step", ""), n["target"], "tcp" if n["intended"] == "tcp" else "other")] += 1
    for site, step, target, tcp, other in pin_rows:
        got_tcp, got_other = count[(step, target, "tcp")], count[(step, target, "other")]
        line = f"W3 {site} step {step} target {target}: tcp {got_tcp} (registered {tcp}), other {got_other} (registered {other})"
        def meets(got, want):
            return got >= int(want[1:]) if want.startswith("+") else got == int(want)
        if not (meets(got_tcp, tcp) and meets(got_other, other)):
            bad.append(line + " NOT AS REGISTERED")
        else:
            print("PASS " + line)
    if mode == "h" and not any(n["intended"] == "default" for n in live_probed):
        bad.append("W5 no live node started without MOIRAI_TRANSPORT was probed")
    for n in nodes:
        if n["intended"].startswith("other:"):
            bad.append(f"W4 step {n.get('step')} target {n['target']} pid {n['pid']} names {n['intended']}")
    classes = collections.Counter((n["intended"], klass(n)) for n in nodes)
    print(f"NODES {len(nodes)}; live probed {len(live_probed)}, witnessed "
          f"{sum(witnessed(n) for n in live_probed)}; by intended and class: {dict(sorted(classes.items()))}")
    for b in bad:
        print("FAIL " + b)
    print(f"NW.19 ACCOUNT ({mode}): {'PASS' if not bad else 'FAIL'} ({len(bad)} failing lines)")
    return not bad


def main():
    if len(sys.argv) >= 3 and sys.argv[1] == "table":
        print("\n".join(table(load(sys.argv[2]))))
        return 0
    if len(sys.argv) >= 4 and sys.argv[1] == "judge":
        mode = "h"
        if "--mode" in sys.argv:
            mode = sys.argv[sys.argv.index("--mode") + 1]
        return 0 if judge(load(sys.argv[2]), pins(sys.argv[3]), mode) else 1
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
