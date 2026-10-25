#!/usr/bin/env bash
# NW.19, P0, P1 and P7 (vault spec/items/NW.19.md, sections 1 and 2). NW.20's static checks
# (moirai-detune tests/nw20/static.sh at bee3337) carried to this branch and its claim.
#
#   tests/nw19/static.sh <spec-commit> <out-dir>
#
# P0  P0-TREE the tree clean on akira/moirai-modelstore. P0-SCOPE every non-merge commit on HEAD's
#     first-parent line since the spec commit, and `git diff --name-only <spec> HEAD`, inside the
#     scope as tests/nw19/scope.txt reads at the spec commit (a `/**` line takes every path under
#     its directory; a line ending ` doc` takes that file for comment lines only: every line the
#     commit or the diff adds or removes there is blank or a `//` comment); no merge commit.
#     P0-ORDER (Cam's C5, the plan's step 3) at least two such commits, a commit S before the
#     first commit F whose transport_config.rs reads `DEFAULT_TRANSPORT ... = TransportKind::Zenoh`,
#     such that at S: the default still reads TransportKind::Tcp, the Dockerfile builds and ships
#     moirai-router, docker/compose differs from the spec commit, and from a git archive of S the
#     stack script's dry run (I-4) shows one router and no bootnode or relay for
#     MOIRAI_TRANSPORT=zenoh and a bootnode and a relay for --tcp, a replica told
#     MOIRAI_TRANSPORT=zenoh with ZENOH_ROUTER and no BOOTNODE_URL for zenoh and told
#     MOIRAI_TRANSPORT=tcp with the bootnode for --tcp, and the rig's replicas all told
#     MOIRAI_TRANSPORT=tcp; F exists. P0-NO-INLINE-TEST no `#[cfg(test)]` added in any */src/ or
#     */examples/ file since the spec commit.
# P1  P1-DEFAULT DEFAULT_TRANSPORT reads TransportKind::Zenoh; P1-L1 nw19_default_l1 in the
#     default build, 3 passed; P1-LITE the same file with --no-default-features, 6 passed; P1-LIB
#     nw19_default_zenoh, 3 passed; P1-NW15 nw15_default_tcp (1) and nw15_zenoh_config (10), 11
#     passed; P1-NODES model_node built, Arachne's network_node
#     built from a git archive of NW19_ARACHNE_REV (default bd7b647) beside this tree, then
#     nw19_default_node_l3, 5 passed; P1-BUILD-* `cargo build -p moirai-network --lib` in the
#     default build, with --no-default-features, and with --no-default-features --features zenoh,
#     each exit 0.
# P7  P7-FMT `cargo fmt --all --check` exit 0; P7-CLIPPY clippy on moirai-network and
#     moirai-interp (network), all targets: no warning in a Rust file of the scope or in a Rust
#     file the spec commit added or changed since 69753ce (0 each at the spec commit).
# Exit 0 only when every check passes. Cargo under the shared lock, -j 2, nice 10.
set -uo pipefail
export LC_ALL=C

LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SPEC="${1:?spec commit}"
OUT="${2:?out dir}"
BRANCH=akira/moirai-modelstore
BASE=69753ce5ffc261090e06ec5d616ddc34a944e82a
AR="${NW19_ARACHNE:-$ROOT/../arachne-model-plane}"
AR_REV="${NW19_ARACHNE_REV:-bd7b647}"
SCR="${NW19_SCRATCH:-/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.19}"
[ -e "$OUT" ] && { echo "NW.19 STATIC REFUSED: $OUT exists"; exit 2; }
mkdir -p "$OUT" "$SCR/tmp"
export CARGO_BUILD_JOBS=2 TMPDIR="$SCR/tmp"
CARGO=(flock "$LOCK" nice -n 10 cargo)
FAILS=0
pass() { printf 'PASS %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*"; FAILS=$((FAILS + 1)); }
note() { printf 'RECORD %s\n' "$*"; }
G() { git -C "$ROOT" "$@"; }
for v in $(env | cut -d= -f1 | grep -E '^(ZENOH_|MOIRAI_TRANSPORT$|MOIRAI_E2E_|ROUTER_REGIONS$)'); do
    echo "NW.19 STATIC REFUSED: $v is set"; exit 2
done

SPEC="$(G rev-parse "$SPEC")"
HEAD="$(G rev-parse HEAD)"
note "spec commit $SPEC, HEAD $HEAD"

# ------------------------------------------------------------------ P0
br="$(G branch --show-current)"; dirty="$(G status --porcelain)"
if [ "$br" = "$BRANCH" ] && [ -z "$dirty" ]; then pass "P0-TREE clean on $BRANCH"; else fail "P0-TREE branch $br, changed: $(printf '%s' "$dirty" | head -5 | tr '\n' ' ')"; fi
G show "$SPEC:tests/nw19/scope.txt" > "$OUT/scope.txt" 2>/dev/null
scope_of() { # path -> prints `full`, `doc` or nothing
    local p=$1 line
    while read -r line; do
        [ -n "$line" ] || continue
        case "$line" in
            *' doc') [ "$p" = "${line% doc}" ] && { echo doc; return; } ;;
            *'/**') [[ "$p" == "${line%'/**'}/"* ]] && { echo full; return; } ;;
            *) [ "$p" = "$line" ] && { echo full; return; } ;;
        esac
    done < "$OUT/scope.txt"
}
comments_only() { # from to path -> 0 when every changed line is blank or a // comment
    G diff -U0 "$1" "$2" -- "$3" | grep -E '^[+-]' | grep -vE '^(\+\+\+|---) ' \
        | grep -vqE '^[+-][[:space:]]*(//.*)?$' && return 1
    return 0
}
outside=""; walked=0
merges="$(G rev-list --merges "$SPEC..HEAD" | wc -l)"
judge_paths() { # from to label
    local p s
    for p in $(G diff --name-only "$1" "$2"); do
        s="$(scope_of "$p")"
        case "$s" in
            full) ;;
            doc) comments_only "$1" "$2" "$p" || outside="$outside $3:$p(code)" ;;
            *) outside="$outside $3:$p" ;;
        esac
    done
}
for c in $(G rev-list --first-parent --no-merges "$SPEC..HEAD"); do
    walked=$((walked + 1))
    judge_paths "$c^" "$c" "${c:0:7}"
done
judge_paths "$SPEC" HEAD diff
if [ -s "$OUT/scope.txt" ] && [ -z "$outside" ] && [ "$merges" = 0 ]; then
    pass "P0-SCOPE $walked commit(s) walked, $(G diff --name-only "$SPEC" HEAD | wc -l) path(s) changed, all in scope, no merge"
else fail "P0-SCOPE outside the scope:$outside; merges $merges; scope read: $(tr '\n' ';' < "$OUT/scope.txt")"; fi

default_at() { G show "$1:moirai-network/src/transport_config.rs" 2>/dev/null | grep -oE 'pub const DEFAULT_TRANSPORT: TransportKind = TransportKind::[A-Za-z]+' | sed 's/.*:://'; }
# The stack's support at commit $1, read from a git archive of it: echoes the failing parts.
support_at() {
    local c=$1 d="$OUT/at-${1:0:7}" bad=""
    rm -rf "$d"; mkdir -p "$d/moirai-model-plane"
    G archive "$c" docker | tar -x -C "$d/moirai-model-plane"
    local df="$d/moirai-model-plane/docker/e2e/Dockerfile" st="$d/moirai-model-plane/docker/compose/stack_interpreter.sh"
    python3 -I "$HERE/compose_check.py" dockerfile "$df" > "$d/check.txt" 2>&1 || bad="$bad image-router"
    [ -n "$(G diff --name-only "$SPEC" "$c" -- docker/compose)" ] || bad="$bad compose-unchanged"
    local env=(env MOIRAI_IMAGE=nw19-static-dry-run:none MOIRAI_INFRA_PROJECT=nw19-static MOIRAI_STACK_NETWORK=nw19_static_net
        MOIRAI_ROUTER_NAME=nw19-static-router HTTP_ALLOWED_ORIGINS=)
    "${env[@]}" MOIRAI_TRANSPORT=zenoh bash "$st" config infra > "$d/infra-zenoh.json" 2> "$d/infra-zenoh.err"
    python3 -I "$HERE/compose_check.py" infra "$d/infra-zenoh.json" zenoh nw19-static-router nw19_static_net >> "$d/check.txt" 2>&1 || bad="$bad infra-zenoh"
    "${env[@]}" bash "$st" --tcp config infra > "$d/infra-tcp.json" 2> "$d/infra-tcp.err"
    python3 -I "$HERE/compose_check.py" infra "$d/infra-tcp.json" tcp >> "$d/check.txt" 2>&1 || bad="$bad infra-tcp"
    "${env[@]}" MOIRAI_TRANSPORT=zenoh bash "$st" config up nw19-static-a --port 1 > "$d/up-zenoh.json" 2> "$d/up-zenoh.err"
    python3 -I "$HERE/compose_check.py" replica "$d/up-zenoh.json" zenoh nw19-static-a >> "$d/check.txt" 2>&1 || bad="$bad replica-zenoh"
    "${env[@]}" bash "$st" --tcp config up nw19-static-a --port 1 > "$d/up-tcp.json" 2> "$d/up-tcp.err"
    python3 -I "$HERE/compose_check.py" replica "$d/up-tcp.json" tcp nw19-static-a >> "$d/check.txt" 2>&1 || bad="$bad replica-tcp"
    (cd "$d/moirai-model-plane/docker/compose" && MOIRAI_IMAGE=nw19-static-dry-run:none docker compose -f docker-compose.yml --profile '*' config --format json) > "$d/rig.json" 2> "$d/rig.err"
    python3 -I "$HERE/compose_check.py" rig "$d/rig.json" >> "$d/check.txt" 2>&1 || bad="$bad rig"
    echo "$bad"
}
F=""; S=""; S_WHY=""
for c in $(G rev-list --reverse --first-parent --no-merges "$SPEC..HEAD"); do
    k="$(default_at "$c")"
    if [ -z "$F" ] && [ "$k" = Zenoh ]; then F=$c; fi
    if [ -z "$F" ] && [ "$k" = Tcp ]; then
        why="$(support_at "$c")"
        if [ -z "${why// /}" ]; then S=$c; else S_WHY="$S_WHY ${c:0:7}:[${why# }]"; fi
    fi
done
if [ "$walked" -ge 2 ] && [ -n "$S" ] && [ -n "$F" ]; then pass "P0-ORDER the stack's support ${S:0:7} (TCP still the default; the router built and shipped; the router service, the TCP mode, the replicas told their transport, the rig on TCP, the dry run) before the flip ${F:0:7}"
else fail "P0-ORDER commits $walked; stack commit before the flip '${S:0:7}' (candidates short of it:${S_WHY:- none}); flip '${F:0:7}'"; fi

added=''
while read -r f; do
    now="$(grep -c '#\[cfg(test)\]' "$ROOT/$f" 2>/dev/null || true)"
    was="$(G show "$SPEC:$f" 2>/dev/null | grep -c '#\[cfg(test)\]' || true)"
    [ "${now:-0}" -le "${was:-0}" ] || added="$added $f($was->$now)"
done < <(cd "$ROOT" && git ls-files | grep -E '^[^/]+/(src|examples)/.*\.rs$')
if [ -z "$added" ]; then pass "P0-NO-INLINE-TEST no #[cfg(test)] added in any */src/ or */examples/ file since the spec commit"
else fail "P0-NO-INLINE-TEST #[cfg(test)] added:$added"; fi

# ------------------------------------------------------------------ P1
line="$(grep -n 'pub const DEFAULT_TRANSPORT' "$ROOT/moirai-network/src/transport_config.rs" 2>&1)"
if printf '%s' "$line" | grep -qE 'pub const DEFAULT_TRANSPORT: TransportKind = TransportKind::Zenoh;'; then pass "P1-DEFAULT $line"
else fail "P1-DEFAULT DEFAULT_TRANSPORT is not TransportKind::Zenoh: ${line:-no such line}"; fi

tests_pass() { # label, expected passed, cargo args...
    local label=$1 want=$2 log="$OUT/p1-$1.log" got
    shift 2
    (cd "$ROOT" && "${CARGO[@]}" "$@") > "$log" 2>&1
    got="$(grep -E '^test result' "$log" | awk '{p += $4; f += $6} END {print p + 0 " " f + 0}')"
    if [ "$got" = "$want 0" ]; then pass "$label $want passed, 0 failed ($*)"
    else fail "$label passed and failed: $got, $want passed registered; $(grep -m3 -E 'panicked|^error|NW19 ' "$log" | tr '\n' ' ' | head -c 600)"; fi
}
tests_pass P1-L1 3 test -j 2 --locked -p moirai-network --test nw19_default_l1
tests_pass P1-LITE 6 test -j 2 --locked -p moirai-network --no-default-features --test nw19_default_l1
tests_pass P1-LIB 3 test -j 2 --locked -p moirai-network --test nw19_default_zenoh -- --test-threads=1
tests_pass P1-NW15 11 test -j 2 --locked -p moirai-network --test nw15_default_tcp --test nw15_zenoh_config -- --test-threads=1

(cd "$ROOT" && "${CARGO[@]}" build -j 2 --locked -p moirai-interp --features network --example model_node) > "$OUT/p1-model_node.log" 2>&1
mkdir -p "$OUT/ar/arachne-model-plane"
git -C "$AR" archive "$AR_REV" | tar -x -C "$OUT/ar/arachne-model-plane"
ln -s "$ROOT" "$OUT/ar/moirai-model-plane"
NN=""
if (CARGO_TARGET_DIR="$SCR/target-arachne" "${CARGO[@]}" build -j 2 --offline --locked \
        --manifest-path "$OUT/ar/arachne-model-plane/generated/json_crdt/Cargo.toml" --example network_node) > "$OUT/p1-network_node.log" 2>&1; then
    NN="$OUT/network_node"; cp "$SCR/target-arachne/debug/examples/network_node" "$NN"
    note "network_node from arachne $(git -C "$AR" rev-parse "$AR_REV") beside HEAD, sha256 $(sha256sum "$NN" | cut -c1-64)"
fi
if [ -n "$NN" ]; then
    export MOIRAI_E2E_NODE_BIN="$NN"
    tests_pass P1-NODES 5 test -j 2 --locked -p moirai-interp --features network --test nw19_default_node_l3 -- --test-threads=1 --nocapture
    unset MOIRAI_E2E_NODE_BIN
else fail "P1-NODES network_node did not build: $(grep -m3 '^error' "$OUT/p1-network_node.log" | tr '\n' ' ')"; fi

build() { # label, cargo args...
    local label=$1 log="$OUT/p1-build-$1.log"
    shift
    if (cd "$ROOT" && "${CARGO[@]}" build -j 2 --locked -p moirai-network --lib "$@") > "$log" 2>&1; then pass "P1-BUILD-$label cargo build -p moirai-network --lib $*"
    else fail "P1-BUILD-$label cargo build -p moirai-network --lib $*: $(grep -m3 -E '^error' "$log" | tr '\n' ' ' | head -c 400)"; fi
}
build default
build none --no-default-features
build zenoh --no-default-features --features zenoh

# ------------------------------------------------------------------ P7
if (cd "$ROOT" && cargo fmt --all --check) > "$OUT/p7-fmt.log" 2>&1; then pass "P7-FMT cargo fmt --all --check exit 0"
else fail "P7-FMT $(grep -m5 '^Diff in' "$OUT/p7-fmt.log" | tr '\n' ' ')"; fi
(cd "$ROOT" && "${CARGO[@]}" clippy -j 2 --locked -p moirai-network -p moirai-interp --features moirai-interp/network \
    --all-targets --message-format=short) > "$OUT/p7-clippy.log" 2>&1
crc=$?
watched="$( { grep -vE '(\*\*| doc)$' "$OUT/scope.txt"; sed -n 's/ doc$//p' "$OUT/scope.txt"; G diff --name-only "$BASE" "$SPEC"; } | grep -E '\.rs$' | sort -u)"
hits="$(grep -E '^[^ ]+\.rs:[0-9]+:[0-9]+: (warning|error)' "$OUT/p7-clippy.log" | grep -F -f <(printf '%s\n' $watched | sed 's/$/:/') | head -20)"
if [ "$crc" = 0 ] && [ -z "$hits" ]; then pass "P7-CLIPPY no warning in the scope's or NW.19's Rust files ($(printf '%s\n' $watched | wc -l) files watched)"
else fail "P7-CLIPPY exit $crc; $(printf '%s' "$hits" | tr '\n' ' ' | head -c 800)"; fi

if [ "$FAILS" -eq 0 ]; then echo "NW.19 STATIC: PASS"; exit 0; fi
echo "NW.19 STATIC: FAIL ($FAILS failing checks)"
exit 1
