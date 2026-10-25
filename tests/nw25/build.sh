#!/usr/bin/env bash
# NW.25, the build step (vault spec/items/NW.25.md, section 4): the candidate model_node built from
# this worktree at HEAD as the NW.22 and NW.23 pins were built (debug,
# cargo build --locked -p moirai-interp --features network --example model_node), the NW.16a witness
# probe from HEAD, the backtrace preload; and the CONTROL, model_node built the same way from a git
# archive of the baseline 9fbb6fd (zenoh 1.10.1 from the registry, unpatched) in the scratch
# directory, never in the shared bin directory. Copies every binary to <pins-dir> so that no later
# build can swap one under the load step. Then the binary checks:
#   B1 CAND-SOURCE   the candidate names vendor/zenoh-1.10.1/src/net/runtime/orchestrator.rs (cargo
#                    passes a path package under the workspace root by its relative path) and never
#                    the registry's .../index.crates.io-1949cf8c6b5b557f/zenoh-1.10.1/src
#   B2 CAND-FIX      nm -C --defined-only: no ZRuntime::block_in_place instantiated over
#                    StartConditions or route_declare_final, no tokio::sync::Mutex<Vec<PeerConnector>>,
#                    and at least one std::sync Mutex<Vec<PeerConnector>>
#   C1 CONTROL-SOURCE the control names the registry's zenoh-1.10.1/src and never the vendored path
#   C2 CONTROL-UNFIXED the control holds the unpatched code: block_in_place over StartConditions or
#                    route_declare_final > 0, tokio Mutex<Vec<PeerConnector>> > 0, std one 0
# C1 and C2 qualify the instrument, not the claim: if either fails, the control is not the
# unpatched build and the script refuses (exit 2), so the load step does not run.
# Writes <pins-dir>/pins.txt (kind, path, sha256 per pinned file). Exit 0 when every build and all
# four checks pass, 1 when B1 or B2 fails, 2 when a build fails or C1 or C2 fails.
#
#   tests/nw25/build.sh <pins-dir>
set -uo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
PINS="${1:?pins dir}"
BASE=9fbb6fdcb770cd3ddcb346aba0f103fc678883e7
SCR="${NW25_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.25}"
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
TARGET="${CARGO_TARGET_DIR:-$ROOT/target}"
CONTROL_TARGET="$SCR/target-control"
C=(flock "$LOCK" nice -n 10 cargo)
export CARGO_BUILD_JOBS=2
REGISTRY_ZENOH="index.crates.io-1949cf8c6b5b557f/zenoh-1.10.1/src"
VENDOR_ZENOH="vendor/zenoh-1.10.1/src/net/runtime/orchestrator.rs"
[ -e "$PINS" ] && { echo "NW.25 BUILD: REFUSED $PINS exists (a pins directory is used once)"; exit 2; }
case "$PINS" in /volatile/home/vp242103/.cache/cea-crdt-scratch/bin*) echo "NW.25 BUILD: REFUSED $PINS is the shared bin directory"; exit 2 ;; esac
mkdir -p "$PINS" "$CONTROL_TARGET"
cd "$ROOT" || exit 2
HEAD="$(git rev-parse HEAD)"; SHORT="${HEAD:0:7}"
echo "NW.25 BUILD: tree $ROOT at $HEAD on $(git branch --show-current), $(git status --porcelain --untracked-files=no | wc -l) changed tracked paths" | tee "$PINS/build.txt"

"${C[@]}" build -j 2 --locked -p moirai-interp --features network --example model_node > "$PINS/b-model_node.log" 2>&1 || { echo "NW.25 BUILD: FAIL model_node does not build: $(grep -m3 '^error' "$PINS/b-model_node.log" | tr '\n' ' ')" | tee -a "$PINS/build.txt"; exit 2; }
probe="$("${C[@]}" test -j 2 --locked -p moirai-interp --features network --test nw16a_witness_probe --no-run --message-format=json 2> "$PINS/b-probe.log" \
    | python3 -I -c 'import json, sys
out = [m["executable"] for m in map(json.loads, sys.stdin) if m.get("reason") == "compiler-artifact" and m.get("executable")]
print(out[-1] if out else "")')"
[ -x "$probe" ] || { echo "NW.25 BUILD: FAIL no witness probe built" | tee -a "$PINS/build.txt"; exit 2; }
gcc -O1 -g -shared -fPIC -o "$PINS/libbtdump.so" "$HERE/btdump.c" > "$PINS/b-btdump.log" 2>&1 || { echo "NW.25 BUILD: FAIL btdump.c" | tee -a "$PINS/build.txt"; exit 2; }
cp "$TARGET/debug/examples/model_node" "$PINS/model_node-cand-$SHORT"
cp "$probe" "$PINS/nw16a_witness_probe-$SHORT"
vw="$(grep -cE '^warning: .*' "$PINS/b-model_node.log")"
echo "NW.25 BUILD note: candidate build log warning lines $vw, of which naming vendor/: $(grep -A3 '^warning' "$PINS/b-model_node.log" | grep -c 'vendor/zenoh-1.10.1')" | tee -a "$PINS/build.txt"

# The control: model_node from the baseline, in its own extraction and target directory.
CSRC="$PINS/control-src/moirai-model-plane"
mkdir -p "$CSRC"
git archive "$BASE" | tar -x -C "$CSRC" || { echo "NW.25 BUILD: FAIL cannot extract $BASE" | tee -a "$PINS/build.txt"; exit 2; }
(cd "$CSRC" && CARGO_TARGET_DIR="$CONTROL_TARGET" "${C[@]}" build -j 2 --locked --offline -p moirai-interp --features network --example model_node) > "$PINS/b-control.log" 2>&1 \
    || { echo "NW.25 BUILD: FAIL the control does not build: $(grep -m3 '^error' "$PINS/b-control.log" | tr '\n' ' ')" | tee -a "$PINS/build.txt"; exit 2; }
cp "$CONTROL_TARGET/debug/examples/model_node" "$PINS/model_node-control-${BASE:0:7}"
rm -rf "$PINS/control-src"

chmod a-w "$PINS"/model_node-* "$PINS"/nw16a_witness_probe-* "$PINS/libbtdump.so"
{
    printf 'candidate %s %s\n' "$PINS/model_node-cand-$SHORT" "$(sha256sum "$PINS/model_node-cand-$SHORT" | cut -c1-64)"
    printf 'control %s %s\n' "$PINS/model_node-control-${BASE:0:7}" "$(sha256sum "$PINS/model_node-control-${BASE:0:7}" | cut -c1-64)"
    printf 'probe %s %s\n' "$PINS/nw16a_witness_probe-$SHORT" "$(sha256sum "$PINS/nw16a_witness_probe-$SHORT" | cut -c1-64)"
    printf 'preload %s %s\n' "$PINS/libbtdump.so" "$(sha256sum "$PINS/libbtdump.so" | cut -c1-64)"
    printf 'head %s\n' "$HEAD"
    printf 'control-source %s\n' "$BASE"
} > "$PINS/pins.txt"
tee -a "$PINS/build.txt" < "$PINS/pins.txt"

counts() { # binary -> "reg ven f1 f2 f3"
    local names reg ven f1 f2 f3
    reg="$(grep -c -a -F "$REGISTRY_ZENOH" "$1")"
    ven="$(grep -c -a -F "$VENDOR_ZENOH" "$1")"
    names="$1.names"
    nm -C --defined-only "$1" | cut -d' ' -f3- > "$names"
    f1="$(grep -cE '^<zenoh_runtime::ZRuntime>::block_in_place::<.*(StartConditions|route_declare_final)' "$names")"
    f2="$(grep -cF 'tokio::sync::mutex::Mutex<alloc::vec::Vec<zenoh::net::runtime::orchestrator::PeerConnector>>' "$names")"
    f3="$(grep -cE 'std::sync::(poison::)?(mutex::)?Mutex<alloc::vec::Vec<zenoh::net::runtime::orchestrator::PeerConnector>>' "$names")"
    rm -f "$names"
    echo "$reg $ven $f1 $f2 $f3"
}
fails=0
read -r reg ven f1 f2 f3 < <(counts "$PINS/model_node-cand-$SHORT")
if [ "$reg" -eq 0 ] && [ "$ven" -gt 0 ]; then echo "B1 CAND-SOURCE PASS model_node-cand-$SHORT: $ven strings name $VENDOR_ZENOH, none the registry's zenoh-1.10.1" | tee -a "$PINS/build.txt"
else echo "B1 CAND-SOURCE FAIL model_node-cand-$SHORT: $reg strings name the registry's $REGISTRY_ZENOH, $ven the vendored orchestrator.rs" | tee -a "$PINS/build.txt"; fails=$((fails + 1)); fi
if [ "$f1" -eq 0 ] && [ "$f2" -eq 0 ] && [ "$f3" -gt 0 ]; then echo "B2 CAND-FIX PASS model_node-cand-$SHORT: block_in_place over StartConditions or route_declare_final 0, tokio Mutex<Vec<PeerConnector>> 0, std Mutex<Vec<PeerConnector>> $f3" | tee -a "$PINS/build.txt"
else echo "B2 CAND-FIX FAIL model_node-cand-$SHORT: block_in_place over StartConditions or route_declare_final $f1, tokio Mutex<Vec<PeerConnector>> $f2, std Mutex<Vec<PeerConnector>> $f3" | tee -a "$PINS/build.txt"; fails=$((fails + 1)); fi
cfail=0
read -r reg ven f1 f2 f3 < <(counts "$PINS/model_node-control-${BASE:0:7}")
if [ "$reg" -gt 0 ] && [ "$ven" -eq 0 ]; then echo "C1 CONTROL-SOURCE PASS model_node-control-${BASE:0:7}: $reg strings name the registry's zenoh-1.10.1, none the vendored path" | tee -a "$PINS/build.txt"
else echo "C1 CONTROL-SOURCE FAIL model_node-control-${BASE:0:7}: $reg registry strings, $ven vendored" | tee -a "$PINS/build.txt"; cfail=1; fi
if [ "$f1" -gt 0 ] && [ "$f2" -gt 0 ] && [ "$f3" -eq 0 ]; then echo "C2 CONTROL-UNFIXED PASS model_node-control-${BASE:0:7}: block_in_place over StartConditions or route_declare_final $f1, tokio Mutex<Vec<PeerConnector>> $f2, std Mutex<Vec<PeerConnector>> 0" | tee -a "$PINS/build.txt"
else echo "C2 CONTROL-UNFIXED FAIL model_node-control-${BASE:0:7}: block_in_place $f1, tokio Mutex $f2, std Mutex $f3" | tee -a "$PINS/build.txt"; cfail=1; fi
if [ "$cfail" -ne 0 ]; then echo "NW.25 BUILD: REFUSED the control is not the unpatched build" | tee -a "$PINS/build.txt"; exit 2; fi
if [ "$fails" -eq 0 ]; then echo "NW.25 BUILD: PASS (B1, B2; control C1, C2 qualified)" | tee -a "$PINS/build.txt"; exit 0
else echo "NW.25 BUILD: FAIL ($fails of 2 candidate checks; control C1, C2 qualified)" | tee -a "$PINS/build.txt"; exit 1; fi
