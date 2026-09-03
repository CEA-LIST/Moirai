#!/usr/bin/env bash
#
# M-E6 — the mutation run.
#
# Three phases, each its own foreground invocation so a long run is a series
# of bounded steps rather than one unbounded one:
#
#   hand      the hand-written mutants of mutants/*.json, applied and reverted
#             by hand-mutants.py: the classes the validation plan names that
#             `cargo mutants` cannot express (a swapped LogId is a change of
#             key, not of operator) or reach (the header code is an example
#             binary of the Arachne workspace; the digest comparison and the
#             store write are TypeScript). Each must be killed by the test
#             its spec names.
#   mutants   `cargo mutants` over the scope in .cargo/mutants.toml, for one
#             PACKAGE (moirai-network or moirai-protocol), optionally one
#             SHARD (k/n), unit tests only; its `mutants.out` lands under
#             mutants-out/<package>[-k-of-n]/.
#   report    results.csv from every outcome so far, the score, the manifest.
#
# Usage, from this directory:
#
#     PHASE=hand ./run.sh
#     PHASE=mutants PACKAGE=moirai-network SHARD=0/4 ./run.sh   (and 1/4 .. 3/4)
#     PHASE=mutants PACKAGE=moirai-protocol ./run.sh
#     PHASE=report ./run.sh
#
# Threshold: every hand-written mutant killed; the overall score is reported
# without one. Knobs: JOBS (2, parallel mutants), CARGO_BUILD_JOBS (4, cores
# per mutant build).

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

PHASE=${PHASE:-report}
PACKAGE=${PACKAGE:-moirai-network}
SHARD=${SHARD:-}
JOBS=${JOBS:-2}
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}

stamp=$(mp_stamp)
hand_csv="$HERE/hand.csv"
out_root="$HERE/mutants-out"

phase_hand() {
    mp_say "hand-written mutants (load $(mp_load_average))"
    rm -f "$hand_csv"
    python3 "$HERE/hand-mutants.py" --out "$hand_csv" --run "$stamp"
}

phase_mutants() {
    local dir="$out_root/$PACKAGE" features=() shard=()
    [ "$PACKAGE" = moirai-protocol ] && features=(--features serde)
    if [ -n "$SHARD" ]; then
        dir="$dir-${SHARD/\//-of-}"
        shard=(--shard "$SHARD")
    fi
    mkdir -p "$dir"
    mp_say "cargo mutants -p $PACKAGE ${shard[*]} (load $(mp_load_average)); output in $dir"
    ( cd "$MOIRAI_ROOT" && nice -n 10 cargo mutants -p "$PACKAGE" "${features[@]}" "${shard[@]}" \
        -j "$JOBS" --no-shuffle -o "$dir" ) || true
    printf '%-14s%s\n' started "$stamp" > "$dir/run.txt"
    printf '%-14s%s\n' command "PHASE=mutants PACKAGE=$PACKAGE SHARD=$SHARD JOBS=$JOBS CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS ./run.sh" >> "$dir/run.txt"
    for kind in caught missed unviable timeout; do
        printf '%-14s%s\n' "$kind" "$(wc -l < "$dir/mutants.out/$kind.txt" 2>/dev/null || echo 0)" >> "$dir/run.txt"
    done
    cat "$dir/run.txt"
}

phase_report() {
    python3 - "$HERE" "$stamp" <<'PY'
import csv, json, sys
from pathlib import Path
here, stamp = Path(sys.argv[1]), sys.argv[2]
rows = []
hand = here / "hand.csv"
if hand.exists():
    for row in csv.DictReader(hand.open()):
        rows.append({"run": row["run"], "crate": row["crate"], "file": row["file"], "mutant_class": row["mutant_class"],
                     "mutant": row["mutant"], "killed_by": row["killed_by"], "status": row["status"], "source": "hand"})
counts = {}
for outcomes in sorted((here / "mutants-out").glob("*/mutants.out/outcomes.json")):
    package = outcomes.parent.parent.name.split("-")[0] + "-" + outcomes.parent.parent.name.split("-")[1]
    data = json.loads(outcomes.read_text())
    for outcome in data.get("outcomes", []):
        scenario = outcome.get("scenario")
        mutant = scenario.get("Mutant") if isinstance(scenario, dict) else None
        if mutant is None:
            continue
        summary = outcome.get("summary", "")
        status = {"CaughtMutant": "killed", "MissedMutant": "survived", "Unviable": "unviable", "Timeout": "timeout"}.get(summary, summary)
        counts[status] = counts.get(status, 0) + 1
        function = (mutant.get("function") or {}).get("function_name", "")
        line = ((mutant.get("span") or {}).get("start") or {}).get("line", "")
        rows.append({"run": stamp, "crate": package, "file": mutant.get("file", ""),
                     "mutant_class": f"cargo mutants: {mutant.get('genre', '')} in {function}",
                     "mutant": f"{mutant.get('file','')}:{line}: {mutant.get('replacement','')}"[:160],
                     "killed_by": f"cargo test -p {package} --lib" if status == "killed" else "",
                     "status": status, "source": "cargo-mutants"})
with (here / "results.csv").open("w", newline="") as handle:
    writer = csv.DictWriter(handle, fieldnames=["run", "crate", "file", "mutant_class", "mutant", "killed_by", "status", "source"])
    writer.writeheader(); writer.writerows(rows)
hand_rows = [r for r in rows if r["source"] == "hand"]
tool_rows = [r for r in rows if r["source"] == "cargo-mutants"]
killed = sum(1 for r in tool_rows if r["status"] == "killed")
survived = sum(1 for r in tool_rows if r["status"] == "survived")
timeout = sum(1 for r in tool_rows if r["status"] == "timeout")
unviable = sum(1 for r in tool_rows if r["status"] == "unviable")
tested = killed + survived + timeout
score = (100.0 * killed / tested) if tested else float("nan")
lines = [
    f"hand-written mutants: {sum(1 for r in hand_rows if r['status'] == 'killed')}/{len(hand_rows)} killed",
    f"cargo mutants: {killed} caught, {survived} missed, {timeout} timed out, {unviable} unviable; score {score:.1f}% of {tested} viable",
]
for r in hand_rows:
    lines.append(f"  hand {r['status']:8} {r['mutant']}: {r['mutant_class']}")
for r in tool_rows:
    if r["status"] != "killed":
        lines.append(f"  tool {r['status']:8} {r['mutant']}")
(here / "summary.txt").write_text("\n".join(lines) + "\n")
print("\n".join(lines))
PY
    {
        mp_manifest_common "$stamp" "PHASE=hand ./run.sh; PHASE=mutants PACKAGE=moirai-network SHARD=k/4 ./run.sh (k=0..3); PHASE=mutants PACKAGE=moirai-protocol ./run.sh; PHASE=report ./run.sh"
        printf '%-14s%s\n' cargo_mutants "$(cargo mutants --version 2>/dev/null)"
        printf '%-14s%s\n' scope "$MOIRAI_ROOT/.cargo/mutants.toml: generic.rs, http_api.rs, tcsb.rs, message.rs, the functions its examine_re names; unit tests only (-- --lib); protocol with --features serde"
        printf '%-14s%s\n' hand "mutants/*.json applied and reverted by hand-mutants.py; the classes cargo mutants cannot express or reach"
        printf '%-14s%s\n' threshold "every hand-written mutant killed by the test its spec names; the score is reported without one"
        printf '%-14s%s\n' node_binary "the json-crdt hand mutants rebuild the network_node example under cargo test and spawn it"
        printf '%-14s%s\n' summary "$(tr '\n' ';' < "$HERE/summary.txt")"
    } > "$HERE/manifest.txt"
    cat "$HERE/manifest.txt"
}

case "$PHASE" in
    hand) phase_hand ;;
    mutants) phase_mutants ;;
    report) phase_report ;;
    all) phase_hand; PACKAGE=moirai-network phase_mutants; PACKAGE=moirai-protocol phase_mutants; phase_report ;;
    *) mp_die "PHASE must be hand, mutants, report or all" ;;
esac
