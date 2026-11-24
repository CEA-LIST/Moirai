#!/usr/bin/env bash
# NW.31d record step, not a gate: the six contract cases through the adapter at
# the newest upstream Moirai master this machine already holds (no fetch).
# Prints that sha and PASS or FAIL per case; always exits 0 unless it cannot run.
# The newest master is the newest refs/heads/master or refs/remotes/origin/master
# by committer date among local clones whose origin is CEA-LIST/Moirai, under
# the repos folder and the scratch folder. When it is 7fe5ea2 (the pin, whose
# four crates this branch holds unchanged) the cases run in this tree; otherwise
# in a scratch copy of HEAD with moirai-protocol, moirai-crdt, moirai-macros and
# moirai-fuzz taken from that sha, without --locked.
set -uo pipefail
export LC_ALL=C CARGO_NET_OFFLINE=true CARGO_INCREMENTAL=0
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PIN=7fe5ea2
SCRATCH=$HOME/.cache/cea-crdt-scratch/NW.31d/master-head
LOCK=${NW31D_CARGO_LOCK:-$HOME/.cache/cea-crdt-scratch/cargo.lock}
CASES=(exchange_converges refusal_with_cause has_history batch_stops_at_first_refusal port_defaults progress_counts)
cargo_() { if [ -e "$LOCK" ]; then flock "$LOCK" nice -n 10 cargo "$@"; else nice -n 10 cargo "$@"; fi; }

newest=$(for g in $(find "$ROOT/../../.." "$HOME/.cache/cea-crdt-scratch" -maxdepth 5 -name .git 2>/dev/null); do
  d=$(dirname "$g")
  case "$(git -C "$d" remote get-url origin 2>/dev/null)" in *CEA-LIST/Moirai*) ;; *) continue ;; esac
  git -C "$d" for-each-ref --format="%(committerdate:unix) %(objectname) $d" refs/heads/master refs/remotes/origin/master
done | sort -n | tail -n 1)
[ -n "$newest" ] || { echo "no local upstream master found"; exit 2; }
read -r _ sha clone <<<"$newest"
echo "MASTER_HEAD $sha from $clone"

dir=$ROOT
if [ "$(git -C "$clone" rev-parse "$PIN^{commit}" 2>/dev/null)" != "$sha" ]; then
  rm -rf "$SCRATCH" && mkdir -p "$SCRATCH"
  git -C "$ROOT" archive HEAD | tar -x -C "$SCRATCH"
  rm -rf "$SCRATCH"/moirai-protocol "$SCRATCH"/moirai-crdt "$SCRATCH"/moirai-macros "$SCRATCH"/moirai-fuzz
  git -C "$clone" archive "$sha" moirai-protocol moirai-crdt moirai-macros moirai-fuzz | tar -x -C "$SCRATCH"
  dir=$SCRATCH
  locked=()
else
  echo "MASTER_HEAD is the pin $PIN: cases run in this tree"
  locked=(--locked)
fi
out=$(cd "$dir" && CARGO_TARGET_DIR=${CARGO_TARGET_DIR:-$HOME/.cache/cea-crdt-scratch/NW.31d/target} cargo_ test -p moirai-netstack-crdt "${locked[@]}" -j 2 --test nw31d_upstream upstream_passes_ 2>&1)
for c in "${CASES[@]}"; do
  if echo "$out" | grep -q "^test upstream_passes_$c \.\.\. ok$"; then echo "PASS $c"; else echo "FAIL $c"; fi
done
echo "$out" | grep -E '^error|^test result|panicked' | head -n 20
exit 0
