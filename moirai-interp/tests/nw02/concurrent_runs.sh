#!/usr/bin/env bash
# NW.02, the repeated concurrent runs (spec: cea-cdrt-knowledge/spec/items/NW.02.md).
#
# Each round starts, at the same moment, the suites that start their own
# loopback model_node processes, and waits for all of them:
#   - INTERP_STREAMS copies of this crate's L3 test binaries (the ones that
#     start model_node processes), each copy running them one after another
#     with --test-threads=1;
#   - the editor's e2e files and the agent's e2e files (vitest), NW.02's own
#     files left out so that no squeeze runs during the rounds.
# Nothing is built while a round runs: the binaries are built (or given)
# first. Every log is kept under <outdir>; the summary is the last thing
# printed and is also written to <outdir>/summary.txt.
#
# usage: concurrent_runs.sh <rounds> <outdir>
# env:   INTERP_STREAMS  copies of the L3 binaries per round (default 2)
#        BIN_DIR         a directory holding the L3 test binaries by test name
#                        and a model_node, used instead of building (the
#                        baseline run of the tree before NW.02 uses this)
#        SKIP_TS=1       leave the editor and agent streams out
set -u

here=$(cd "$(dirname "$0")" && pwd)
interp=$(cd "$here/../.." && pwd)
workspace=$(cd "$interp/.." && pwd)
editor=$(cd "$workspace/../model-editor" && pwd)
rounds=${1:?usage: concurrent_runs.sh <rounds> <outdir>}
out=${2:?usage: concurrent_runs.sh <rounds> <outdir>}
streams=${INTERP_STREAMS:-2}
mkdir -p "$out"

L3_TESTS="e2e_interp si01_store_identity_l3 st01_optional_conflict_race_l3 st02_emptied_optional_object_l3 st04_emptied_optional_attribute_l3 st05_removed_key_l3"

declare -A exe
if [ -n "${BIN_DIR:-}" ]; then
  for t in $L3_TESTS; do exe[$t]="$BIN_DIR/$t"; done
  node_bin="$BIN_DIR/model_node"
else
  (cd "$workspace" && nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node) \
    > "$out/build.log" 2>&1 || { echo "build failed, see $out/build.log"; exit 2; }
  (cd "$workspace" && nice -n 10 cargo test -j 2 -p moirai-interp --features network --no-run) \
    >> "$out/build.log" 2>&1 || { echo "build failed, see $out/build.log"; exit 2; }
  for t in $L3_TESTS; do
    exe[$t]=$(sed -nE "s#.*Executable tests/$t\.rs \((target/[^)]*)\).*#\1#p" "$out/build.log" | tail -1)
    exe[$t]="$workspace/${exe[$t]}"
  done
  node_bin="$workspace/target/debug/examples/model_node"
fi
for t in $L3_TESTS; do
  [ -x "${exe[$t]}" ] || { echo "no executable for $t (${exe[$t]})"; exit 2; }
done
[ -x "$node_bin" ] || { echo "no model_node at $node_bin"; exit 2; }

editor_files=$(cd "$editor" && ls e2e/*.e2e.ts | grep -v nw02 | tr '\n' ' ')
agent_files=$(cd "$editor/agent" && ls e2e/*.e2e.ts | grep -v nw02 | tr '\n' ' ')

interp_stream() { # round stream
  local log="$out/r$1-interp$2.log"
  : > "$log"
  for t in $L3_TESTS; do
    echo "=== NW02-STREAM $t" >> "$log"
    (cd "$interp" && MOIRAI_E2E_INTERP_NODE_BIN="$node_bin" "${exe[$t]}" --test-threads=1) >> "$log" 2>&1
    echo "=== NW02-EXIT $t $?" >> "$log"
  done
}

for round in $(seq 1 "$rounds"); do
  echo "round $round: $(date -Iseconds)"
  pids=()
  for s in $(seq 1 "$streams"); do interp_stream "$round" "$s" & pids+=($!); done
  if [ "${SKIP_TS:-0}" != 1 ]; then
    (cd "$editor" && MOIRAI_E2E_NODE_BIN="$node_bin" nice -n 10 npx vitest run --config vitest.e2e.config.ts $editor_files; echo "=== NW02-EXIT editor-e2e $?") > "$out/r$round-editor.log" 2>&1 & pids+=($!)
    (cd "$editor/agent" && MOIRAI_E2E_NODE_BIN="$node_bin" nice -n 10 npx vitest run --config vitest.e2e.config.ts $agent_files; echo "=== NW02-EXIT agent-e2e $?") > "$out/r$round-agent.log" 2>&1 & pids+=($!)
  fi
  wait "${pids[@]}"
  echo "round $round done: $(date -Iseconds)"
done

# A failure is port-related when its output says an address was taken, a
# node never came up or reported, or a port answered as another replica
# (including ST.05's OB2, a joiner's first registration answered 409 by a
# node that already hosted the model).
PORT_PATTERN='AddrInUse|Address already in use|EADDRINUSE|os error 98|is already in use|never answered /api/health|to answer /api/health|never reported its ports|reporting its ports|answered as|already hosted here|mesh never formed|the mesh to form'

python3 - "$out" "$PORT_PATTERN" <<'PY' | tee "$out/summary.txt"
import glob, os, re, sys
out, pattern = sys.argv[1], re.compile(sys.argv[2])
rows, port_failures, other_failures, skips = [], 0, 0, 0
passed_ip19 = runs_ip19 = 0
for log in sorted(glob.glob(os.path.join(out, "r*-*.log"))):
    text = open(log, errors="replace").read()
    name = os.path.basename(log)
    failures = []  # (test, the text its failure is reported in)
    if "-interp" in name:
        runs_ip19 += len(re.findall(r"^test ip19_\S+ \.\.\. (?:ok|FAILED)$", text, re.M))
        passed_ip19 += len(re.findall(r"^test ip19_\S+ \.\.\. ok$", text, re.M))
        sections = {m.group(1): m.group(2) for m in re.finditer(
            r"^---- (\S+) stdout ----\n(.*?)(?=^---- |^failures:$|\Z)", text, re.M | re.S)}
        for test in re.findall(r"^test (\S+) \.\.\. FAILED$", text, re.M):
            failures.append((test, sections.get(test, "")))
    else:
        heads = list(re.finditer(r"^ ?FAIL\s+(.+)$", text, re.M))
        for i, head in enumerate(heads):
            end = heads[i + 1].start() if i + 1 < len(heads) else len(text)
            failures.append((head.group(1).strip(), text[head.end():end]))
    for test, body in failures:
        hits = sorted(set(pattern.findall(body)))
        if hits:
            port_failures += 1
        else:
            other_failures += 1
        rows.append(f"{name}: FAILED {test} [{'port' if hits else 'other'}] {', '.join(hits)}")
    # mp26's headerless skip is a registered decision, not a port: left out.
    skipped = len([l for l in re.findall(r"^.*E2E-SKIP.*$", text, re.M) if "no __model header" not in l])
    skips += skipped
    exits = re.findall(r"^=== NW02-EXIT (\S+) (\d+)$", text, re.M)
    nonzero = [f"{t}={c}" for t, c in exits if c != "0"]
    rows.append(f"{name}: {len(failures)} failed, {skipped} E2E-SKIP; exits {'all 0' if not nonzero else ' '.join(nonzero)}")
print("\n".join(rows))
print(f"NW02-CONCURRENT port-related failures {port_failures}, other failures {other_failures}, "
      f"E2E-SKIP lines {skips}, ip19 passed {passed_ip19} of {runs_ip19}")
PY
