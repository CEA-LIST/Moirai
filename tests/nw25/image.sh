#!/usr/bin/env bash
# NW.25, Arachne's network_node built as the image builds it (vault spec/items/NW.25.md, section 4;
# docker/e2e/Dockerfile lines 40-41 and 67-70 at 9fbb6fd), without Docker. The build context of
# that Dockerfile is the parent of both checkouts, with Dockerfile.dockerignore keeping **/target,
# **/.git and **/.github out; the builder copies the whole moirai checkout and only
# generated/json_crdt of Arachne, each under its real directory name, then runs
# `cargo build --example network_node` from the crate (debug, default features, no dev-dependencies,
# target inside the crate). This script lays the two trees out the same way from git (HEAD of this
# checkout and HEAD of Arachne's), drops .github, and runs that command with --offline --locked
# added, so the lock as committed is what builds.
#
#   tests/nw25/image.sh <out-dir>          (NW25_ARACHNE_GIT as in arachne.sh)
#
# Checks, each printed PASS or FAIL; exit 0 only when every one passes, 1 otherwise, 2 refused:
#   I1 BUILD   `cargo build --offline --locked -j 2 --example network_node` exits 0. When it fails,
#              the build is run once more without --locked in the same layout (cargo then rewrites
#              the layout's copy of the lock, never a repository's) so that I2 to I4 still say what
#              that build holds; I1 stays FAIL.
#   I2 TREE    `cargo tree -i zenoh -e normal,build` from the crate roots at
#              zenoh v1.10.1 (<layout>/moirai-model-plane/vendor/zenoh-1.10.1), and no other zenoh
#              is in the crate's normal and build tree
#   I3 SOURCE  the artifact names vendor/zenoh-1.10.1/src/net/runtime/orchestrator.rs at least once
#              and never the registry's .../index.crates.io-1949cf8c6b5b557f/zenoh-1.10.1/src
#   I4 FIX     nm -C --defined-only: no ZRuntime::block_in_place instantiated over StartConditions or
#              route_declare_final, no tokio::sync::Mutex<Vec<PeerConnector>>, and at least one
#              std::sync Mutex<Vec<PeerConnector>> (NW.23's B2)
# Recorded, not checked: the artifact's sha256, its moirai_network::zenoh_transport symbol count,
# whether the layout's lock differs from the committed one after the build(s).
set -uo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${1:?out dir}"
AR="${NW25_ARACHNE_GIT:-$ROOT/../arachne-model-plane}"
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
REGISTRY_ZENOH="index.crates.io-1949cf8c6b5b557f/zenoh-1.10.1/src"
VENDOR_ZENOH="vendor/zenoh-1.10.1/src/net/runtime/orchestrator.rs"
[ -e "$OUT" ] && { echo "NW.25 IMAGE: REFUSED $OUT exists (an output directory is used once)"; exit 2; }
[ -d "$AR" ] || { echo "NW.25 IMAGE: REFUSED no Arachne checkout at $AR"; exit 2; }
mkdir -p "$OUT"
CTX="$OUT/ctx"
MD=moirai-model-plane AD=arachne-model-plane
CR="$CTX/$AD/generated/json_crdt"
mkdir -p "$CTX/$MD" "$CTX/$AD"
NCHECKS=4
fails=0
check() { printf '%s %s %s\n' "$1" "$2" "$3" | tee -a "$OUT/checks.txt"; [ "$2" = PASS ] || fails=$((fails + 1)); }
echo "NW.25 IMAGE: moirai $ROOT at $(git -C "$ROOT" rev-parse HEAD), arachne $AR at $(git -C "$AR" rev-parse HEAD); layout $CTX" | tee "$OUT/checks.txt"
git -C "$ROOT" archive HEAD | tar -x -C "$CTX/$MD" || { echo "NW.25 IMAGE: REFUSED cannot extract $MD"; exit 2; }
git -C "$AR" archive HEAD generated/json_crdt | tar -x -C "$CTX/$AD" || { echo "NW.25 IMAGE: REFUSED cannot extract json_crdt"; exit 2; }
# Dockerfile.dockerignore: **/target, **/.git, **/.github
find "$CTX" \( -name target -o -name .git -o -name .github \) -prune -exec rm -rf {} + 2>/dev/null
cp "$CR/Cargo.lock" "$OUT/Cargo.lock.committed"

# The image's command, CARGO_TARGET_DIR unset so the target sits in the crate as in the image.
build() { (cd "$CR" && env -u CARGO_TARGET_DIR CARGO_BUILD_JOBS=2 flock "$LOCK" nice -n 10 cargo build "$@" -j 2 --example network_node); }
build --offline --locked > "$OUT/build-locked.log" 2>&1; brc=$?
if [ "$brc" -eq 0 ]; then
    check I1-BUILD PASS "cargo build --offline --locked --example network_node exit 0"
else
    check I1-BUILD FAIL "cargo build --offline --locked --example network_node exit $brc: $(grep -m2 -E '^(error|warning: .*lock)' "$OUT/build-locked.log" | tr '\n' ' ' | head -c 300)"
    build --offline > "$OUT/build-unlocked.log" 2>&1; urc=$?
    echo "NW.25 IMAGE note: the same build without --locked exit $urc" | tee -a "$OUT/checks.txt"
fi
cmp -s "$OUT/Cargo.lock.committed" "$CR/Cargo.lock" && lockw=no || lockw=yes
echo "NW.25 IMAGE note: layout lock rewritten by cargo: $lockw" | tee -a "$OUT/checks.txt"

# I2
want_root="zenoh v1.10.1 ($CTX/$MD/vendor/zenoh-1.10.1)"
ti="$(cd "$CR" && env -u CARGO_TARGET_DIR flock "$LOCK" nice -n 10 cargo tree --offline -i zenoh -e normal,build --prefix none 2>&1)"
ta="$(cd "$CR" && env -u CARGO_TARGET_DIR flock "$LOCK" nice -n 10 cargo tree --offline -e normal,build --prefix none 2>&1)"
printf '%s\n--\n%s\n' "$ti" "$ta" > "$OUT/tree.txt"
others="$(printf '%s\n' "$ta" | grep -E '^zenoh v' | grep -vF "$want_root" | sort -u | head -3 | tr '\n' ' ')"
if [ "$(printf '%s\n' "$ti" | head -1)" = "$want_root" ] && [ -z "$others" ]; then
    check I2-TREE PASS "the inverted tree roots at '$want_root', no other zenoh"
else check I2-TREE FAIL "root '$(printf '%s\n' "$ti" | head -1)'; other zenoh: [$others]"; fi

# I3, I4
ART="$CR/target/debug/examples/network_node"
if [ -x "$ART" ]; then
    cp "$ART" "$OUT/network_node"
    bin="$OUT/network_node"
    echo "NW.25 IMAGE note: artifact sha256 $(sha256sum "$bin" | cut -c1-64)" | tee -a "$OUT/checks.txt"
    reg="$(grep -c -a -F "$REGISTRY_ZENOH" "$bin")"
    ven="$(grep -c -a -F "$VENDOR_ZENOH" "$bin")"
    if [ "$reg" -eq 0 ] && [ "$ven" -gt 0 ]; then check I3-SOURCE PASS "$ven strings name $VENDOR_ZENOH, none the registry's zenoh-1.10.1"
    else check I3-SOURCE FAIL "$reg strings name the registry's $REGISTRY_ZENOH, $ven the vendored orchestrator.rs"; fi
    nm -C --defined-only "$bin" | cut -d' ' -f3- > "$OUT/names.txt"
    f1="$(grep -cE '^<zenoh_runtime::ZRuntime>::block_in_place::<.*(StartConditions|route_declare_final)' "$OUT/names.txt")"
    f2="$(grep -cF 'tokio::sync::mutex::Mutex<alloc::vec::Vec<zenoh::net::runtime::orchestrator::PeerConnector>>' "$OUT/names.txt")"
    f3="$(grep -cE 'std::sync::(poison::)?(mutex::)?Mutex<alloc::vec::Vec<zenoh::net::runtime::orchestrator::PeerConnector>>' "$OUT/names.txt")"
    zt="$(grep -c 'moirai_network::zenoh_transport' "$OUT/names.txt")"
    echo "NW.25 IMAGE note: moirai_network::zenoh_transport symbols $zt" | tee -a "$OUT/checks.txt"
    if [ "$f1" -eq 0 ] && [ "$f2" -eq 0 ] && [ "$f3" -gt 0 ]; then
        check I4-FIX PASS "block_in_place over StartConditions or route_declare_final 0, tokio Mutex<Vec<PeerConnector>> 0, std Mutex<Vec<PeerConnector>> $f3"
    else check I4-FIX FAIL "block_in_place over StartConditions or route_declare_final $f1, tokio Mutex<Vec<PeerConnector>> $f2, std Mutex<Vec<PeerConnector>> $f3"; fi
    rm -f "$OUT/names.txt"
else
    check I3-SOURCE FAIL "no artifact at $ART"
    check I4-FIX FAIL "no artifact at $ART"
fi
rm -rf "$CR/target"

if [ "$fails" -eq 0 ]; then echo "NW.25 IMAGE: PASS ($NCHECKS of $NCHECKS)" | tee -a "$OUT/checks.txt"; exit 0
else echo "NW.25 IMAGE: FAIL ($fails of $NCHECKS failing)" | tee -a "$OUT/checks.txt"; exit 1; fi
