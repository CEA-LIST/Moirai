#!/usr/bin/env bash
# NW.25, the check of Arachne's generated json_crdt crate (vault spec/items/NW.25.md, section 4):
# the crate, its own workspace root, patches zenoh to the model store's vendored copy and nothing
# else in its manifest changes; its lock gains Zenoh's tree with zenoh from that path and every
# zenoh-* crate at 1.10.1, and no entry of the 7dc783c lock moves. Reads repos/arachne-model-plane
# by git only (HEAD against the baseline 7dc783c); runs no cargo command.
#
#   tests/nw25/arachne.sh <out-dir>          (NW25_ARACHNE_GIT names the Arachne checkout;
#                                              default: the sibling ../arachne-model-plane)
#
# Checks, each printed PASS or FAIL; exit 0 only when every one passes, 1 otherwise, 2 refused:
#   J1 MANIFEST  generated/json_crdt/Cargo.toml at HEAD: [patch.crates-io] is exactly
#                zenoh = { path = "../../../moirai-model-plane/vendor/zenoh-1.10.1" }, and without
#                the patch table it parses equal to 7dc783c's (no dependency, feature or other change)
#   J2 SCOPE     every path changed since 7dc783c is generated/json_crdt/Cargo.toml or
#                generated/json_crdt/Cargo.lock, and the worktree holds no change to either
#   J3 LOCK      generated/json_crdt/Cargo.lock at HEAD against 7dc783c's:
#                (a) the lock version is unchanged and every entry of the base lock is there with
#                    the same name, version, source and checksum (none gone, none moved);
#                (b) exactly one zenoh entry, 1.10.1, with no source and no checksum (a path package);
#                (c) at least one zenoh-* entry, every one at 1.10.1 from the registry;
#                (d) no base entry loses or retargets a dependency (each reference resolved to the
#                    name and version it names in its own lock), and the dependencies base entries
#                    gain are, by name, exactly the registered set GAINED below
#                Information only: the added entries, and those whose name and version the model
#                store's lock at HEAD does not hold.
#   J4 HOOK      the global pre-commit hook's pattern list (core.hooksPath/pre-commit, read at run
#                time, never copied) finds nothing in the files changed since 7dc783c as committed
#                at HEAD, nor in the added lines of any commit since 7dc783c read as the hook reads
#                a staged diff; self-test first on the published zenoh crate, as structural.sh S10.
set -uo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${1:?out dir}"
AR="${NW25_ARACHNE_GIT:-$ROOT/../arachne-model-plane}"
A_BASE=7dc783c6b2a4c2cd3e26be8f8373ed8594df8f02
REV=HEAD
CRATE_DIR=generated/json_crdt
CRATE="$HOME/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/zenoh-1.10.1.crate"
CRATE_SHA=453ba5d28a1197653aae4bb024fd74a9d5c7051d5a19a77f3a43d83f40c22584
[ -e "$OUT" ] && { echo "NW.25 ARACHNE: REFUSED $OUT exists (an output directory is used once)"; exit 2; }
git -C "$AR" cat-file -e "$A_BASE^{commit}" 2>/dev/null || { echo "NW.25 ARACHNE: REFUSED no Arachne checkout holding $A_BASE at $AR"; exit 2; }
mkdir -p "$OUT"
NCHECKS=4
fails=0
check() { # id PASS|FAIL detail
    printf '%s %s %s\n' "$1" "$2" "$3" | tee -a "$OUT/checks.txt"
    [ "$2" = PASS ] || fails=$((fails + 1))
}
echo "NW.25 ARACHNE: tree $AR at $(git -C "$AR" rev-parse HEAD) on $(git -C "$AR" branch --show-current), baseline $A_BASE" | tee "$OUT/checks.txt"

# J1
git -C "$AR" show "$A_BASE:$CRATE_DIR/Cargo.toml" > "$OUT/Cargo.toml.base"
git -C "$AR" show "$REV:$CRATE_DIR/Cargo.toml" > "$OUT/Cargo.toml.rev"
j1="$(python3 -I - "$OUT/Cargo.toml.base" "$OUT/Cargo.toml.rev" <<'PY'
import sys, tomllib
base, rev = (tomllib.loads(open(p).read()) for p in sys.argv[1:3])
why = []
want = {"crates-io": {"zenoh": {"path": "../../../moirai-model-plane/vendor/zenoh-1.10.1"}}}
if rev.get("patch") != want:
    why.append(f"patch table is {rev.get('patch')!r}")
strip = lambda d: {k: v for k, v in d.items() if k != "patch"}
if strip(base) != strip(rev):
    keys = sorted(set(strip(base)) | set(strip(rev)))
    why.append("differs from 7dc783c outside the patch table in "
               + ", ".join(k for k in keys if strip(base).get(k) != strip(rev).get(k)))
print(f"PASS patch {rev['patch']!r}; features {rev.get('features')!r} as at 7dc783c" if not why else "FAIL " + "; ".join(why))
PY
)"
check J1-MANIFEST "${j1%% *}" "${j1#* }"

# J2
changed="$(git -C "$AR" diff --name-only "$A_BASE" "$REV")"
scope_re="^$CRATE_DIR/Cargo\.(toml|lock)$"
outside="$(printf '%s\n' "$changed" | grep -vE "$scope_re" | grep -c .)"
dirty="$(git -C "$AR" status --porcelain --untracked-files=all -- "$CRATE_DIR/Cargo.toml" "$CRATE_DIR/Cargo.lock" | tr '\n' ' ')"
if [ "$outside" = 0 ] && [ -z "$dirty" ]; then
    check J2-SCOPE PASS "$(printf '%s\n' "$changed" | grep -c .) paths changed since 7dc783c: [$(printf '%s\n' "$changed" | tr '\n' ' ')]; worktree clean there"
else check J2-SCOPE FAIL "$outside paths outside the scope: [$(printf '%s\n' "$changed" | grep -vE "$scope_re" | head -10 | tr '\n' ' ')]; worktree changes [$dirty]"; fi

# J3
git -C "$AR" show "$A_BASE:$CRATE_DIR/Cargo.lock" > "$OUT/Cargo.lock.base"
git -C "$AR" show "$REV:$CRATE_DIR/Cargo.lock" > "$OUT/Cargo.lock.rev"
git -C "$ROOT" show "$REV:Cargo.lock" > "$OUT/Cargo.lock.modelstore"
j3="$(python3 -I - "$OUT/Cargo.lock.base" "$OUT/Cargo.lock.rev" "$OUT/Cargo.lock.modelstore" "$OUT/lock-info.txt" <<'PY'
import sys, tomllib
REG = "registry+https://github.com/rust-lang/crates.io-index"
# The dependencies base entries gain when Zenoh's tree joins the lock, by name (registered in the
# spec from a rehearsal on 2026-10-25, cargo 1.97.1, offline): feature unification, never a move.
GAINED = {
    ("bitflags", "2.11.1"): {"serde_core"},
    ("chrono", "0.4.44"): {"serde"},
    ("digest", "0.10.7"): {"subtle"},
    ("futures-util", "0.3.32"): {"futures-channel", "futures-macro"},
    ("jiff", "0.2.24"): {"jiff-tzdb-platform", "windows-sys"},
    ("moirai-network", "0.1.0"): {"zenoh"},
    ("tokio", "1.53.1"): {"bytes", "tokio-macros"},
    ("tracing", "0.1.44"): {"tracing-attributes"},
    ("tracing-core", "0.1.36"): {"valuable"},
}
base, rev, ms = (tomllib.loads(open(p).read()) for p in sys.argv[1:4])
info = open(sys.argv[4], "w")
why = []
if base.get("version") != rev.get("version"):
    why.append(f"lock version {base.get('version')} -> {rev.get('version')}")
key = lambda p: (p["name"], p["version"], p.get("source"))
def by_name(lock):
    out = {}
    for p in lock["package"]:
        out.setdefault(p["name"], []).append(p["version"])
    return out
bn, rn = by_name(base), by_name(rev)
def resolve(ref, names):
    parts = ref.split(" ")
    if len(parts) >= 2:
        return (parts[0], parts[1])
    vs = names.get(parts[0], [])
    return (parts[0], vs[0] if len(vs) == 1 else f"?{len(vs)}")
b = {key(p): p for p in base["package"]}
r = {key(p): p for p in rev["package"]}
# (a)
gone = [k for k in b if k not in r]
moved = [k for k in b if k in r and b[k].get("checksum") != r[k].get("checksum")]
if gone:
    why.append(f"(a) base entries gone or moved: {[' '.join(map(str, k)) for k in gone[:8]]}")
if moved:
    why.append(f"(a) checksums changed: {moved[:8]}")
# (b)
zen = [p for p in rev["package"] if p["name"] == "zenoh"]
if len(zen) != 1 or zen[0]["version"] != "1.10.1" or "source" in zen[0] or "checksum" in zen[0]:
    why.append(f"(b) zenoh entries: {[(p['version'], p.get('source'), p.get('checksum')) for p in zen]}")
# (c)
zs = [p for p in rev["package"] if p["name"].startswith("zenoh-")]
off = [(p["name"], p["version"], p.get("source")) for p in zs if p["version"] != "1.10.1" or p.get("source") != REG]
if not zs or off:
    why.append(f"(c) zenoh-* entries {len(zs)}, not 1.10.1 from the registry: {off[:8]}")
# (d)
lost, gained = [], {}
for k, p in b.items():
    q = r.get(k)
    if q is None:
        continue
    bd = {resolve(x, bn) for x in p.get("dependencies", [])}
    rd = {resolve(x, rn) for x in q.get("dependencies", [])}
    if bd - rd:
        lost.append((k[0], k[1], sorted(bd - rd)))
    names = {n for n, _ in rd - bd} - {n for n, _ in bd}
    if names:
        gained[(k[0], k[1])] = names
if lost:
    why.append(f"(d) dependencies lost or retargeted: {lost[:6]}")
if gained != GAINED:
    extra = {f"{k[0]} {k[1]}": sorted(v - GAINED.get(k, set())) for k, v in gained.items() if v - GAINED.get(k, set())}
    missing = {f"{k[0]} {k[1]}": sorted(v - gained.get(k, set())) for k, v in GAINED.items() if v - gained.get(k, set())}
    why.append(f"(d) gained dependencies off the registered set: unregistered {extra}, registered but not gained {missing}")
added = sorted(set(r) - set(b))
msk = {(p["name"], p["version"]) for p in ms["package"]}
not_in_ms = [k for k in added if (k[0], k[1]) not in msk]
print(f"base {len(b)} entries, HEAD {len(r)}, added {len(added)}; gained {sorted((f'{k[0]} {k[1]}', sorted(v)) for k, v in gained.items())}", file=info)
print(f"added entries whose name and version the model store's lock does not hold: {len(not_in_ms)} {[' '.join(map(str, k[:2])) for k in not_in_ms[:40]]}", file=info)
for k in added:
    print("ADDED", k[0], k[1], k[2] or "(path)", file=info)
if why:
    print("FAIL " + "; ".join(why))
else:
    print(f"PASS {len(b)} base entries kept unmoved, {len(r)} now ({len(added)} added); zenoh 1.10.1 a path package; "
          f"{len(zs)} zenoh-* entries all 1.10.1 from the registry; no dependency lost; gained exactly the registered "
          f"{sum(len(v) for v in GAINED.values())} in {len(GAINED)} entries; {len(not_in_ms)} added name+version pairs absent from the model store's lock")
PY
)"
check J3-LOCK "${j3%% *}" "${j3#* }"

# J4
HOOKS="$(git -C "$AR" config --get core.hooksPath 2>/dev/null)"; HOOKS="${HOOKS/#\~/$HOME}"
HOOK="${HOOKS:+$HOOKS/pre-commit}"
PAT=""
if [ -n "$HOOK" ] && [ -f "$HOOK" ] && [ "$(grep -c "^patterns='.*'\$" "$HOOK")" = 1 ] \
   && grep -qF 'git diff --cached -U0 --no-color' "$HOOK" && grep -qF 'grep -EIn "$patterns"' "$HOOK"; then
    PAT="$(sed -n "s/^patterns='\(.*\)'\$/\1/p" "$HOOK")"
fi
hook_sha="$(sha256sum "$HOOK" 2>/dev/null | cut -c1-64)"
scan_files() { (cd "$1" && grep -rEn -a -e "$PAT" -- . 2>/dev/null | cut -d: -f1,2 | sed 's#^\./##' | sort); }
scan_added() { "$@" 2>/dev/null | grep -nE '^\+' | grep -EIn -e "$PAT" | cut -d: -f1,2; }
if [ -z "$PAT" ]; then
    check J4-HOOK FAIL "cannot read the hook's pattern list from '${HOOK:-core.hooksPath unset}'"
elif [ "$(sha256sum "$CRATE" 2>/dev/null | cut -c1-64)" != "$CRATE_SHA" ]; then
    check J4-HOOK FAIL "self-test impossible: no zenoh-1.10.1.crate with sha256 $CRATE_SHA"
else
    mkdir -p "$OUT/published" "$OUT/empty" "$OUT/changed"
    tar -xzf "$CRATE" -C "$OUT/published"
    st_files="$(scan_files "$OUT/published/zenoh-1.10.1" | grep -c .)"
    st_out="$(scan_files "$OUT/published/zenoh-1.10.1" | grep -vc '^tests/')"
    st_added="$(scan_added diff -ruN "$OUT/empty" "$OUT/published/zenoh-1.10.1" | grep -c .)"
    git -C "$AR" diff --name-only -z --diff-filter=d "$A_BASE" "$REV" > "$OUT/changed-paths.z"
    [ -s "$OUT/changed-paths.z" ] && xargs -0 git -C "$AR" archive "$REV" -- < "$OUT/changed-paths.z" | tar -xi -C "$OUT/changed"
    nchanged="$(tr -cd '\0' < "$OUT/changed-paths.z" | wc -c)"
    hf="$(scan_files "$OUT/changed" | tee "$OUT/hook-changed.txt" | grep -c .)"
    : > "$OUT/hook-commits.txt"; ncommits=0
    for c in $(git -C "$AR" rev-list --reverse "$A_BASE..$REV"); do
        ncommits=$((ncommits + 1))
        scan_added git -C "$AR" diff -U0 --no-color "$c^" "$c" | sed "s/^/$(git -C "$AR" rev-parse --short=12 "$c") /" >> "$OUT/hook-commits.txt"
    done
    hc="$(grep -c . "$OUT/hook-commits.txt")"
    rm -rf "$OUT/published" "$OUT/empty"
    if [ "$st_files" -lt 1 ] || [ "$st_out" -ne 0 ] || [ "$st_added" -lt 1 ]; then
        check J4-HOOK FAIL "self-test: the published crate gave $st_files file hits ($st_out outside tests/) and $st_added added-line hits; want at least 1, all under tests/"
    elif [ "$hf" = 0 ] && [ "$hc" = 0 ]; then
        check J4-HOOK PASS "hook $HOOK (sha256 $hook_sha); self-test $st_files file hits all under tests/, $st_added added-line hits; 0 hits in the $nchanged files changed since 7dc783c, 0 in the added lines of $ncommits commits"
    else check J4-HOOK FAIL "hits: changed files $hf [$(head -5 "$OUT/hook-changed.txt" | tr '\n' ' ')], commit lines $hc [$(head -5 "$OUT/hook-commits.txt" | tr '\n' ' ')] (hook sha256 $hook_sha)"; fi
fi

if [ "$fails" -eq 0 ]; then echo "NW.25 ARACHNE: PASS ($NCHECKS of $NCHECKS)" | tee -a "$OUT/checks.txt"; exit 0
else echo "NW.25 ARACHNE: FAIL ($fails of $NCHECKS failing)" | tee -a "$OUT/checks.txt"; exit 1; fi
