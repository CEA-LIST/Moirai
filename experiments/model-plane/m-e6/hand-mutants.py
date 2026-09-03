#!/usr/bin/env python3
"""The hand-written mutants of M-E6, applied one at a time and reverted.

For each `mutants/*.json`: check the worktree file is clean under git, apply
the one-place replacement, run the named test command from that worktree,
record whether it failed (the mutant is killed) or passed (it survived), and
restore the file from git whatever happened. A mutant whose `find` text is
not present exactly once is an error, not a survivor: the code moved and the
mutant no longer says what it claims.

Writes one CSV row per mutant, `run,crate,file,mutant_class,mutant,killed_by`
plus `status` and `seconds`, appended to the path given.
"""

import argparse
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
MOIRAI = HERE.parents[2]
ARACHNE = Path(os.environ.get("ARACHNE_ROOT", MOIRAI.parent / ("arachne" + MOIRAI.name.removeprefix("moirai"))))
ROOTS = {"moirai": MOIRAI, "arachne": ARACHNE}
CWD_OF = {
    "moirai:moirai-network/src/generic.rs": MOIRAI,
    "moirai:moirai-protocol/src/broadcast/tcsb.rs": MOIRAI,
    "arachne:generated/json_crdt/examples/network_node.rs": ARACHNE / "generated" / "json_crdt",
    "arachne:clients/model-editor/src/model/binding.ts": ARACHNE / "clients" / "model-editor",
    "arachne:clients/model-editor/src/model/projection.ts": ARACHNE / "clients" / "model-editor",
}


def git(root, *args):
    return subprocess.run(["git", "-C", str(root), *args], capture_output=True, text=True, check=True).stdout


def run_mutant(spec, name, run_id):
    prefix, rel = spec["file"].split(":", 1)
    root = ROOTS[prefix]
    path = root / rel
    cwd = CWD_OF[spec["file"]]
    if git(root, "status", "--porcelain", "--", rel).strip():
        raise SystemExit(f"{rel} is not clean in {root}; refusing to mutate over local changes")
    original = path.read_text()
    if original.count(spec["find"]) != 1:
        raise SystemExit(f"{name}: the text to mutate occurs {original.count(spec['find'])} times in {rel}")
    print(f"--- {name}: {spec['class']}", file=sys.stderr)
    started = time.monotonic()
    try:
        path.write_text(original.replace(spec["find"], spec["replace"], 1))
        result = subprocess.run(spec["test"], shell=True, cwd=str(cwd), capture_output=True, text=True)
    finally:
        git(root, "checkout", "--", rel)
        if git(root, "status", "--porcelain", "--", rel).strip():
            raise SystemExit(f"{rel} did not revert cleanly")
        # A mutant of the node binary leaves the mutated example built on
        # disk after its source is reverted; `after` rebuilds it, so the
        # next thing to spawn that binary (M-E5's calibration, the editor's
        # live-node tests) runs the source and not the last mutant.
        if spec.get("after"):
            subprocess.run(spec["after"], shell=True, cwd=str(cwd), check=True, capture_output=True)
    seconds = time.monotonic() - started
    killed = result.returncode != 0
    tail = "\n".join((result.stdout + result.stderr).strip().splitlines()[-6:])
    print(f"    {'killed' if killed else 'SURVIVED'} in {seconds:.0f}s (exit {result.returncode})\n"
          + "\n".join("    | " + line for line in tail.splitlines()), file=sys.stderr)
    return {
        "run": run_id,
        "crate": "json-crdt" if "generated" in rel else ("model-editor" if rel.endswith(".ts") else rel.split("/")[0]),
        "file": rel,
        "mutant_class": spec["class"],
        "mutant": name,
        "killed_by": spec["killed_by"] if killed else "",
        "status": "killed" if killed else "survived",
        "seconds": round(seconds, 1),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--run", default="1")
    parser.add_argument("--only", nargs="*", help="mutant names to run (default: all)")
    args = parser.parse_args()
    specs = sorted((HERE / "mutants").glob("*.json"))
    rows = []
    for spec_path in specs:
        name = spec_path.stem
        if args.only and name not in args.only:
            continue
        rows.append(run_mutant(json.loads(spec_path.read_text()), name, args.run))
    exists = Path(args.out).exists()
    with open(args.out, "a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        if not exists:
            writer.writeheader()
        writer.writerows(rows)
    survived = [r["mutant"] for r in rows if r["status"] != "killed"]
    print(f"{len(rows) - len(survived)}/{len(rows)} hand-written mutants killed"
          + (f"; survived: {', '.join(survived)}" if survived else ""), file=sys.stderr)
    sys.exit(1 if survived else 0)


if __name__ == "__main__":
    main()
