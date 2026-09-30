#!/usr/bin/env python3
"""FMT.01's test-list comparison (cea-cdrt-knowledge/spec/items/FMT.01.md).

`cargo test <args> -- --list > X.list 2> X.err` writes each test binary's
list to stdout and the binary's header ("Running tests/foo.rs (...)" or
"Doc-tests crate") to stderr, in the same order. This script pairs them
back up, keys each binary by its header with the build hash dropped, and
compares two such runs binary by binary and name by name.

Usage:
  test_lists.py show LIST ERR
  test_lists.py compare BASE.list BASE.err AFTER.list AFTER.err

`compare` prints one line per binary and exits 0 when every binary lists
the same tests (names and kinds, ignored ones included, since `--list`
lists them all), 1 when one does not, 2 when a file cannot be paired.
"""

import re
import sys

HEADER = re.compile(r"^\s*(?:Running (\S+(?: \S+)?) \((?:.*/)?([^/()]+?)(?:-[0-9a-f]{16})?(?:\.exe)?\)|Doc-tests (\S+))\s*$")
SUMMARY = re.compile(r"^(\d+) tests?, (\d+) benchmarks?$")


def read(list_path, err_path):
    headers = []
    for line in open(err_path, encoding="utf-8", errors="replace"):
        m = HEADER.match(line.rstrip("\n"))
        if m:
            headers.append(f"{m.group(1)} [{m.group(2)}]" if m.group(1) else f"Doc-tests {m.group(3)}")
    blocks, current = [], []
    for line in open(list_path, encoding="utf-8", errors="replace"):
        line = line.rstrip("\n")
        m = SUMMARY.match(line)
        if m:
            blocks.append((current, int(m.group(1)), int(m.group(2))))
            current = []
        elif line.endswith(": test") or line.endswith(": benchmark"):
            current.append(line)
    if len(headers) != len(blocks):
        print(f"FMT01-LISTS-ERROR {list_path}: {len(blocks)} lists against {len(headers)} headers in {err_path}")
        sys.exit(2)
    out = {}
    for header, (names, tests, benches) in zip(headers, blocks):
        if len(names) != tests + benches:
            print(f"FMT01-LISTS-ERROR {header}: {len(names)} names against a summary of {tests} + {benches}")
            sys.exit(2)
        if header in out:
            print(f"FMT01-LISTS-ERROR {header} appears twice")
            sys.exit(2)
        out[header] = sorted(names)
    return out


def main():
    if len(sys.argv) == 4 and sys.argv[1] == "show":
        lists = read(sys.argv[2], sys.argv[3])
        for header, names in lists.items():
            print(f"FMT01-LIST {header}: {len(names)}")
        print(f"FMT01-LISTS binaries={len(lists)} tests={sum(len(v) for v in lists.values())}")
        return 0
    if len(sys.argv) != 6 or sys.argv[1] != "compare":
        print(__doc__)
        return 2
    base, after = read(sys.argv[2], sys.argv[3]), read(sys.argv[4], sys.argv[5])
    differing = 0
    for header in sorted(set(base) | set(after)):
        b, a = base.get(header), after.get(header)
        if b == a:
            print(f"FMT01-LIST {header}: same, {len(b)}")
            continue
        differing += 1
        if b is None or a is None:
            print(f"FMT01-LIST {header}: DIFFERS, only in {'after' if b is None else 'base'}")
            continue
        gone, new = sorted(set(b) - set(a)), sorted(set(a) - set(b))
        print(f"FMT01-LIST {header}: DIFFERS, {len(b)} -> {len(a)}; gone {gone}; new {new}")
    total_b, total_a = sum(map(len, base.values())), sum(map(len, after.values()))
    print(f"FMT01-LISTS binaries={len(base)}->{len(after)} tests={total_b}->{total_a} differing={differing} "
          f"result={'PASS' if not differing else 'FAIL'}")
    return 0 if not differing else 1


if __name__ == "__main__":
    sys.exit(main())
