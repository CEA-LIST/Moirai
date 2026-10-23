#!/bin/bash
# NW.17, claim point 3 (vault spec/items/NW.17.md): reads a zenoh run of
# nw17/modelstore_l3.sh against a tcp run and says, suite by suite, whether
# the zenoh run failed exactly the tests the tcp run failed.
#
#   moirai-interp/tests/nw17/compare.sh <tcp-dir> <zenoh-dir>
#
# Each dir is <out-dir>/<transport> of one run. A suite is off when, on
# zenoh: a test that is not N/A ends otherwise than on tcp (ok, FAILED or
# ignored), or is missing, or appears only there; an N/A test is unknown to
# the tcp run, or ran anyway, or the skips filtered out a number of tests
# other than the N/A count; either run printed E2E-SKIP (a skip is not a
# pass); a node it started was not witnessed on Zenoh; or a test that starts
# nodes ran while the suite started none (si13_scale_l3's premise test is
# the one test of the eight suites that starts no node). Prints one line per
# suite and the totals; exit 0 when no suite is off, 1 otherwise, 2 when a
# run is missing.
set -u
TCP=${1:?usage: compare.sh <tcp-dir> <zenoh-dir>}
ZEN=${2:?usage: compare.sh <tcp-dir> <zenoh-dir>}
[ -f "$TCP/summary.txt" ] && [ -f "$ZEN/summary.txt" ] || { echo "a summary is missing" >&2; exit 2; }
python3 -I - "$TCP" "$ZEN" <<'EOF'
import os, re, sys

tcp_dir, zen_dir = sys.argv[1], sys.argv[2]
NODE_FREE = {"si13_scale_premise_the_generator_is_si12s"}


def summary(path):
    rows = {}
    for line in open(os.path.join(path, "summary.txt")):
        if line.startswith("#") or line.startswith("status="):
            continue
        parts = line.split(" ", 1)
        if len(parts) < 2:
            continue
        fields = dict(re.findall(r"(\w+)=(\[[^\]]*\]|\S+)", parts[1]))
        rows[parts[0]] = fields
    return rows


def results(path, suite):
    out = {}
    name = os.path.join(path, suite, "results.txt")
    if os.path.exists(name):
        for line in open(name):
            test, _, status = line.strip().partition(" ")
            if test:
                out[test] = status
    return out


def na(path, suite):
    name = os.path.join(path, suite, "na.txt")
    if not os.path.exists(name):
        return {}
    return dict(line.rstrip("\n").split("\t", 1) for line in open(name) if line.strip())


tcp, zen = summary(tcp_dir), summary(zen_dir)
off = 0
totals = {"tcp": [0, 0, 0], "zenoh": [0, 0, 0, 0]}
for suite in list(dict.fromkeys(list(tcp) + list(zen))):
    why = []
    if suite not in tcp or suite not in zen:
        why.append("run on one transport only")
        print(f"{suite} OFF: {'; '.join(why)}")
        off += 1
        continue
    t, z, n = results(tcp_dir, suite), results(zen_dir, suite), na(zen_dir, suite)
    for status, k in (("ok", 0), ("FAILED", 1), ("ignored", 2)):
        totals["tcp"][k] += sum(1 for v in t.values() if v == status)
        totals["zenoh"][k] += sum(1 for v in z.values() if v == status)
    totals["zenoh"][3] += len(n)
    for test in n:
        if test not in t:
            why.append(f"N/A test {test} unknown to the tcp run")
        if test in z:
            why.append(f"N/A test {test} ran on zenoh")
    differ = [f"{test}: tcp {t[test]}, zenoh {z.get(test, 'missing')}"
              for test in sorted(t) if test not in n and z.get(test) != t[test]]
    if differ:
        why.append("differs: " + "; ".join(differ))
    extra = sorted(set(z) - set(t))
    if extra:
        why.append(f"only on zenoh: {extra}")
    filtered = int(zen[suite].get("filtered", "0"))
    if filtered != len(n):
        why.append(f"zenoh filtered out {filtered} tests for {len(n)} N/A")
    for side, rows in (("tcp", tcp), ("zenoh", zen)):
        if rows[suite].get("e2e_skips", "0") != "0":
            why.append(f"{side} printed E2E-SKIP {rows[suite]['e2e_skips']} time(s)")
    started = int(zen[suite].get("started", "0"))
    witnessed = int(zen[suite].get("witnessed", "0"))
    if witnessed != started:
        why.append(f"zenoh witnessed {witnessed} of {started} nodes: {zen[suite].get('missing')}")
    ran_nodes = [test for test, v in z.items() if v in ("ok", "FAILED") and test not in NODE_FREE]
    if ran_nodes and started == 0:
        why.append(f"zenoh ran {ran_nodes} and started no node")
    failing_t = sorted(test for test, v in t.items() if v == "FAILED")
    failing_z = sorted(test for test, v in z.items() if v == "FAILED")
    line = (f"{suite} tcp_failing={failing_t} zenoh_failing={failing_z} "
            f"na={sorted(n)} ran_zenoh={len([v for v in z.values() if v != 'ignored'])} "
            f"started={started} witnessed={witnessed}")
    if why:
        off += 1
        print(f"{line} OFF: {' | '.join(why)}")
    else:
        print(f"{line} same")
print(f"totals tcp passed={totals['tcp'][0]} failed={totals['tcp'][1]} ignored={totals['tcp'][2]}; "
      f"zenoh passed={totals['zenoh'][0]} failed={totals['zenoh'][1]} ignored={totals['zenoh'][2]} "
      f"na={totals['zenoh'][3]}")
print(f"suites_off={off}")
sys.exit(1 if off else 0)
EOF
