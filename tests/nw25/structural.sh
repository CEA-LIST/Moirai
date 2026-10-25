#!/usr/bin/env bash
# NW.25, the structural check of the model store's workspace (vault spec/items/NW.25.md, section 4):
# the tree carries zenoh 1.10.1 as published, without its tests/ directory (Cam's ruling of
# 2026-10-24 for NW.23, option A), plus exactly upstream commit 646f2d1b730e (eclipse-zenoh/zenoh
# PR #2779); the copy is byte for byte the one repos/moirai-detune carries at 5d3ccd0 (NW.23);
# nothing else in the build moved; nothing committed matches the credential hook's patterns.
# Ported from NW.23's tests/nw23/structural.sh at 57c3f1c, with the baseline changed to 9fbb6fd and
# the twin comparison (S4) added.
#
#   tests/nw25/structural.sh <out-dir>
#
# Ground truth: the zenoh-1.10.1 .crate in cargo's cache, accepted only if its sha256 equals the
# checksum the lock at the baseline 9fbb6fd records for it (crates.io's), extracted afresh; the
# upstream diff kept beside this script as a fixture, accepted only by its sha256; NW.23's vendored
# tree read from repos/moirai-detune by git at 5d3ccd0, accepted only by its registered tree id.
# The reference tree is the extraction with its tests/ directory removed and the fixture applied by
# patch -p2 at zero fuzz; it is compared with the vendored tree as committed at HEAD (git archive),
# not as it sits in the worktree.
#
# Checks, each printed PASS or FAIL; exit 0 only when every one passes, 1 otherwise, 2 refused:
#   S1 FIXTURE   the fixture's sha256, its three files, 42 lines added and 41 removed
#   S2 REFERENCE the .crate's sha256 equals the baseline lock's checksum; its tests/ directory
#                removed; the fixture applies with no offset and no fuzz
#   S3 VENDOR    diff -r of the committed vendor/zenoh-1.10.1 and the reference: no difference
#                (no .cargo-ok, no tests/); the committed Cargo.toml is byte for byte the published
#                one; the worktree holds no change to the vendored tree, Cargo.toml or Cargo.lock
#   S4 TWIN      the committed vendor/zenoh-1.10.1 is git tree 88cf8327..., the tree
#                repos/moirai-detune holds at 5d3ccd0 under the same path (names, modes, bytes),
#                and the two extractions have the same files with the same sha256
#   S5 PATCH     root Cargo.toml: [patch.crates-io] is exactly zenoh = { path = "vendor/zenoh-1.10.1" },
#                the file names issue 2581, PR 2779 and commit 646f2d1b730e, and apart from the
#                patch table and workspace.exclude it parses equal to the baseline's
#   S6 SCOPE     every path changed since the baseline is Cargo.toml, Cargo.lock,
#                vendor/zenoh-1.10.1/** or tests/nw25/**; so every zenoh = "=1.10.1" requirement
#                (moirai-network, moirai-router, moirai-interp) and DEFAULT_TRANSPORT stand
#   S7 MEMBERS   cargo metadata's workspace members equal members-9fbb6fd.txt (no zenoh among them)
#   S8 LOCK      Cargo.lock at HEAD against the baseline's: the only package entry that differs is
#                zenoh 1.10.1, which loses its registry source and checksum and keeps its
#                dependencies; every other entry identical, none added or removed
#   S9 TREE      cargo tree -i zenoh (the workspace with all features; moirai-interp with network;
#                moirai-router; moirai-network on its defaults) roots at
#                zenoh v1.10.1 (<root>/vendor/zenoh-1.10.1), and no zenoh from the registry is
#                anywhere in the workspace's tree
#   S10 HOOK     the pattern list of the global pre-commit hook (core.hooksPath/pre-commit, its one
#                patterns='...' line, read at run time and never copied) finds nothing in: every
#                file of the committed vendored tree; every file changed since the baseline as
#                committed at HEAD; the added lines of every commit since the baseline, read as the
#                hook reads a staged diff; every file under vendor/zenoh-1.10.1 on disk.
#                Self-test first: the same scans must find the published crate's key literals, all
#                under its tests/, or S10 fails. Hits are reported as path:line only.
set -uo pipefail
export LC_ALL=C

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${1:?out dir}"
BASE=9fbb6fdcb770cd3ddcb346aba0f103fc678883e7
REV=HEAD
FIX="$HERE/zenoh-646f2d1b730e.diff"
FIX_SHA=41a4f7881c57d642e1afca7797f5d965ce7e3430cfca85347da1fa17b95bc2d4
CRATE="$HOME/.cargo/registry/cache/index.crates.io-1949cf8c6b5b557f/zenoh-1.10.1.crate"
VENDOR=vendor/zenoh-1.10.1
TWIN_GIT="${NW25_DETUNE_GIT:-$ROOT/../moirai-detune}"
TWIN_COMMIT=5d3ccd02895bd035aecd89ba0662af36d1c46ca0
TWIN_TREE=88cf8327c91b52d3ea416243d5105d5de741ae3e
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
CARGO=(flock "$LOCK" nice -n 10 cargo)
[ -e "$OUT" ] && { echo "NW.25 STRUCTURAL: REFUSED $OUT exists (an output directory is used once)"; exit 2; }
mkdir -p "$OUT"
NCHECKS=10
fails=0
check() { # id PASS|FAIL detail
    printf '%s %s %s\n' "$1" "$2" "$3" | tee -a "$OUT/checks.txt"
    [ "$2" = PASS ] || fails=$((fails + 1))
}
cd "$ROOT" || exit 2
echo "NW.25 STRUCTURAL: tree $ROOT at $(git rev-parse HEAD) on $(git branch --show-current), baseline $BASE" | tee "$OUT/checks.txt"

# S1
sha="$(sha256sum "$FIX" | cut -c1-64)"
files="$(grep -E '^diff --git ' "$FIX" | awk '{print $3}' | sed 's#^a/##' | tr '\n' ' ')"
plus="$(grep -E '^\+' "$FIX" | grep -vc '^+++')"
minus="$(grep -E '^-' "$FIX" | grep -vc '^---')"
if [ "$sha" = "$FIX_SHA" ] && [ "$files" = "zenoh/src/net/protocol/gossip.rs zenoh/src/net/routing/hat/peer/interests.rs zenoh/src/net/runtime/orchestrator.rs " ] \
   && [ "$plus" = 42 ] && [ "$minus" = 41 ]; then
    check S1-FIXTURE PASS "sha256 $sha, files [$files], +$plus -$minus"
else check S1-FIXTURE FAIL "sha256 $sha (want $FIX_SHA), files [$files], +$plus -$minus (want +42 -41)"; fi

# S2
want_ck="$(git show "$BASE:Cargo.lock" | python3 -I -c 'import sys, tomllib
d = tomllib.loads(sys.stdin.read())
print(next(p["checksum"] for p in d["package"] if p["name"] == "zenoh" and p["version"] == "1.10.1"))')"
crate_sha="$(sha256sum "$CRATE" 2>/dev/null | cut -c1-64)"
mkdir -p "$OUT/ref" "$OUT/published"
ref_ok=0
pub_ok=0
if [ -n "$crate_sha" ] && [ "$crate_sha" = "$want_ck" ] && tar -xzf "$CRATE" -C "$OUT/published" && tar -xzf "$CRATE" -C "$OUT/ref"; then
    pub_ok=1
    ntests="$(find "$OUT/ref/zenoh-1.10.1/tests" -type f 2>/dev/null | wc -l)"
    rm -rf "$OUT/ref/zenoh-1.10.1/tests"
    (cd "$OUT/ref/zenoh-1.10.1" && patch -p2 --fuzz=0 --no-backup-if-mismatch < "$FIX") > "$OUT/patch.txt" 2>&1
    prc=$?
    if [ "$prc" -eq 0 ] && [ "$ntests" -gt 0 ] && [ "$(grep -c '^patching file ' "$OUT/patch.txt")" = 3 ] && ! grep -qiE 'offset|fuzz|fail|reject' "$OUT/patch.txt"; then
        ref_ok=1
        check S2-REFERENCE PASS "crate sha256 $crate_sha = the baseline lock's checksum; its tests/ removed ($ntests files); fixture applied with no offset or fuzz ($(tr '\n' ' ' < "$OUT/patch.txt"))"
    else check S2-REFERENCE FAIL "tests/ files $ntests; patch exit $prc: $(tr '\n' ' ' < "$OUT/patch.txt" | head -c 300)"; fi
else check S2-REFERENCE FAIL "crate sha256 ${crate_sha:-absent}, baseline lock checksum $want_ck"; fi

# S3
mkdir -p "$OUT/committed"
if git cat-file -e "$REV:$VENDOR" 2>/dev/null && git archive "$REV" "$VENDOR" | tar -x -C "$OUT/committed"; then
    if [ "$ref_ok" = 1 ]; then
        diff -r "$OUT/ref/zenoh-1.10.1" "$OUT/committed/$VENDOR" > "$OUT/vendor-diff.txt" 2>&1
        drc=$?
    else drc=9; echo "no reference tree" > "$OUT/vendor-diff.txt"; fi
else drc=9; echo "no $VENDOR at $REV" > "$OUT/vendor-diff.txt"; fi
dirty="$(git status --porcelain --untracked-files=all -- "$VENDOR" Cargo.toml Cargo.lock | head -5 | tr '\n' ' ')"
nfiles="$(find "$OUT/committed/$VENDOR" -type f 2>/dev/null | wc -l)"
toml_sha="$(sha256sum "$OUT/committed/$VENDOR/Cargo.toml" 2>/dev/null | cut -c1-64)"
pub_toml_sha="$(sha256sum "$OUT/published/zenoh-1.10.1/Cargo.toml" 2>/dev/null | cut -c1-64)"
extra=""
[ -e "$OUT/committed/$VENDOR/tests" ] && extra="$extra tests/ committed;"
[ -e "$OUT/committed/$VENDOR/.cargo-ok" ] && extra="$extra .cargo-ok committed;"
[ -n "$toml_sha" ] && [ "$toml_sha" = "$pub_toml_sha" ] || extra="$extra Cargo.toml sha256 ${toml_sha:-absent} is not the published ${pub_toml_sha:-absent};"
if [ "$drc" -eq 0 ] && [ -z "$dirty" ] && [ -z "$extra" ]; then
    check S3-VENDOR PASS "committed $VENDOR ($nfiles files) equals the .crate without tests/ plus the fixture; no tests/, no .cargo-ok; Cargo.toml sha256 $toml_sha, the published one; worktree clean there"
else check S3-VENDOR FAIL "diff exit $drc: $(head -c 300 "$OUT/vendor-diff.txt" | tr '\n' ' ');$extra worktree changes [$dirty]"; fi

# S4
sums() { (cd "$1" 2>/dev/null && find . -type f -print0 | sort -z | xargs -0 -r sha256sum); }
twin_tree="$(git -C "$TWIN_GIT" rev-parse --verify -q "$TWIN_COMMIT:$VENDOR" 2>/dev/null)"
head_tree="$(git rev-parse --verify -q "$REV:$VENDOR" 2>/dev/null)"
mkdir -p "$OUT/twin"
git -C "$TWIN_GIT" archive "$TWIN_COMMIT" "$VENDOR" 2>/dev/null | tar -x -C "$OUT/twin" 2>/dev/null
sums "$OUT/twin/$VENDOR" > "$OUT/twin-sha256.txt"
sums "$OUT/committed/$VENDOR" > "$OUT/committed-sha256.txt"
ntwin="$(grep -c . "$OUT/twin-sha256.txt")"
if [ "$twin_tree" = "$TWIN_TREE" ] && [ "$head_tree" = "$TWIN_TREE" ] && [ "$ntwin" -gt 0 ] && cmp -s "$OUT/twin-sha256.txt" "$OUT/committed-sha256.txt"; then
    check S4-TWIN PASS "committed $VENDOR is tree $head_tree, the tree moirai-detune holds at ${TWIN_COMMIT:0:7}; $ntwin files, every sha256 equal"
else
    check S4-TWIN FAIL "moirai-detune ${TWIN_COMMIT:0:7}:$VENDOR is tree '${twin_tree:-absent}' (registered $TWIN_TREE), HEAD's is '${head_tree:-absent}'; per-file sha256 lists $(cmp -s "$OUT/twin-sha256.txt" "$OUT/committed-sha256.txt" && echo equal || echo "differ ($(diff "$OUT/twin-sha256.txt" "$OUT/committed-sha256.txt" | grep -c '^[<>]') lines)")"
fi

# S5
git show "$BASE:Cargo.toml" > "$OUT/Cargo.toml.base"
git show "$REV:Cargo.toml" > "$OUT/Cargo.toml.rev"
s5="$(python3 -I - "$OUT/Cargo.toml.base" "$OUT/Cargo.toml.rev" <<'PY'
import sys, tomllib
base_text = open(sys.argv[1]).read()
rev_text = open(sys.argv[2]).read()
base, rev = tomllib.loads(base_text), tomllib.loads(rev_text)
why = []
patch = rev.get("patch")
if patch != {"crates-io": {"zenoh": {"path": "vendor/zenoh-1.10.1"}}}:
    why.append(f"patch table is {patch!r}")
for word in ("2581", "2779", "646f2d1b730e"):
    if word not in rev_text:
        why.append(f"no mention of {word}")
def strip(d):
    d = {k: v for k, v in d.items() if k != "patch"}
    if "workspace" in d:
        d["workspace"] = {k: v for k, v in d["workspace"].items() if k != "exclude"}
    return d
if strip(base) != strip(rev):
    keys = sorted(set(strip(base)) | set(strip(rev)))
    why.append("differs from the baseline outside the patch table and workspace.exclude in "
               + ", ".join(k for k in keys if strip(base).get(k) != strip(rev).get(k)))
print("PASS exclude=" + repr(rev.get("workspace", {}).get("exclude")) if not why else "FAIL " + "; ".join(why))
PY
)"
check S5-PATCH "${s5%% *}" "${s5#* }"

# S6
changed="$(git diff --name-only "$BASE" "$REV")"
scope_re="^(Cargo\.toml|Cargo\.lock|$VENDOR/.+|tests/nw25/.+)$"
outside="$(printf '%s\n' "$changed" | grep -vE "$scope_re" | grep -c .)"
reqs="$(git grep -n -E '^zenoh = ' "$REV" -- '*/Cargo.toml' | sed "s/^$REV://" | tr '\n' ' ')"
deft="$(git grep -h -E 'pub const DEFAULT_TRANSPORT' "$REV" -- moirai-network/src/transport_config.rs | tr -s ' ')"
if [ "$outside" = 0 ]; then
    check S6-SCOPE PASS "$(printf '%s\n' "$changed" | grep -c .) paths changed since the baseline, none outside the scope; zenoh requirements: $reqs; $deft"
else check S6-SCOPE FAIL "$outside paths outside the scope: $(printf '%s\n' "$changed" | grep -vE "$scope_re" | head -10 | tr '\n' ' ')"; fi

# S7
("${CARGO[@]}" metadata --format-version 1 --no-deps --locked --offline > "$OUT/metadata.json") 2> "$OUT/metadata.err"
python3 -I -c 'import json, sys
m = json.load(open(sys.argv[1])); root = m["workspace_root"]; pk = {p["id"]: p for p in m["packages"]}
for i in sorted(m["workspace_members"], key=lambda i: pk[i]["manifest_path"]):
    print(pk[i]["name"], pk[i]["version"], pk[i]["manifest_path"].replace(root + "/", ""))' "$OUT/metadata.json" > "$OUT/members.txt" 2>> "$OUT/metadata.err"
if [ -s "$OUT/members.txt" ] && cmp -s "$OUT/members.txt" "$HERE/members-9fbb6fd.txt"; then
    check S7-MEMBERS PASS "$(wc -l < "$OUT/members.txt") members, as at the baseline"
else check S7-MEMBERS FAIL "members differ: $(diff "$HERE/members-9fbb6fd.txt" "$OUT/members.txt" | grep '^[<>]' | tr '\n' ' ' | head -c 300) $(head -c 200 "$OUT/metadata.err" | tr '\n' ' ')"; fi

# S8
git show "$BASE:Cargo.lock" > "$OUT/Cargo.lock.base"
git show "$REV:Cargo.lock" > "$OUT/Cargo.lock.rev"
s8="$(python3 -I - "$OUT/Cargo.lock.base" "$OUT/Cargo.lock.rev" <<'PY'
import sys, tomllib
base, rev = (tomllib.loads(open(p).read()) for p in sys.argv[1:3])
why = []
if base.get("version") != rev.get("version"):
    why.append(f"lock version {base.get('version')} -> {rev.get('version')}")
if set(base) != set(rev):
    why.append(f"lock sections {sorted(base)} -> {sorted(rev)}")
key = lambda p: (p["name"], p["version"], p.get("source"))
b = {key(p): p for p in base["package"]}
r = {key(p): p for p in rev["package"]}
reg = "registry+https://github.com/rust-lang/crates.io-index"
gone, new = set(b) - set(r), set(r) - set(b)
if gone != {("zenoh", "1.10.1", reg)}:
    why.append(f"entries gone: {sorted(map(str, gone))}")
if new != {("zenoh", "1.10.1", None)}:
    why.append(f"entries added: {sorted(map(str, new))}")
else:
    z, zb = r[("zenoh", "1.10.1", None)], b.get(("zenoh", "1.10.1", reg), {})
    if set(z) - {"name", "version", "dependencies"}:
        why.append(f"path zenoh entry carries {sorted(set(z) - {'name', 'version', 'dependencies'})}")
    if z.get("dependencies") != zb.get("dependencies"):
        why.append("zenoh's dependencies changed")
changed = sorted(str(k) for k in set(b) & set(r) if b[k] != r[k])
if changed:
    why.append(f"entries changed: {changed[:8]}")
zen = [k for k in r if k[0] == "zenoh"]
if len(zen) != 1:
    why.append(f"{len(zen)} zenoh entries")
print(f"PASS {len(r)} packages, only zenoh 1.10.1 moved (registry source and checksum gone, dependencies kept)" if not why else "FAIL " + "; ".join(why))
PY
)"
check S8-LOCK "${s8%% *}" "${s8#* }"

# S9
want_root="zenoh v1.10.1 ($ROOT/$VENDOR)"
t1="$("${CARGO[@]}" tree -i zenoh --workspace --all-features -e normal,build,dev --locked --offline --prefix none 2>&1)"
t2="$("${CARGO[@]}" tree -i zenoh -p moirai-interp --features network -e normal,build,dev --locked --offline --prefix none 2>&1)"
t3="$("${CARGO[@]}" tree -i zenoh -p moirai-router -e normal,build,dev --locked --offline --prefix none 2>&1)"
t4="$("${CARGO[@]}" tree -i zenoh -p moirai-network -e normal,build --locked --offline --prefix none 2>&1)"
all="$("${CARGO[@]}" tree --workspace --all-features -e normal,build,dev --locked --offline --prefix none 2>&1)"
printf '%s\n--\n%s\n--\n%s\n--\n%s\n' "$t1" "$t2" "$t3" "$t4" > "$OUT/tree-i-zenoh.txt"
registry_zenoh="$(printf '%s\n' "$all" | grep -E '^zenoh v' | grep -vF "$want_root" | sort -u | head -3 | tr '\n' ' ')"
consumers="$(printf '%s\n' "$t1" | sed -n '2,$p' | grep -F "($ROOT/" | awk '{print $1}' | sort -u | tr '\n' ' ')"
first() { printf '%s\n' "$1" | head -1; }
if [ "$(first "$t1")" = "$want_root" ] && [ "$(first "$t2")" = "$want_root" ] && [ "$(first "$t3")" = "$want_root" ] \
   && [ "$(first "$t4")" = "$want_root" ] && [ -z "$registry_zenoh" ] && printf '%s\n' "$all" | grep -qF "$want_root"; then
    check S9-TREE PASS "every inverted tree roots at '$want_root'; workspace packages reaching it: $consumers"
else check S9-TREE FAIL "roots: '$(first "$t1")' / '$(first "$t2")' / '$(first "$t3")' / '$(first "$t4")'; other zenoh in the tree: [$registry_zenoh]"; fi

# S10
# hits_files <dir> <label>: every file under <dir>, binary or not, path:line of each hit (never its text)
hits_files() {
    local d="$1" rc
    [ -d "$d" ] || { : > "$OUT/hook-$2.txt"; return 0; }
    (cd "$d" && grep -rEn -a -e "$PAT" -- . 2> "$OUT/hook-$2.err" | cut -d: -f1,2 | sed 's#^\./##' | sort
     exit "${PIPESTATUS[0]}") > "$OUT/hook-$2.txt"; rc=$?
    [ "$rc" -le 1 ]
}
# hits_added <label> <diff-producing command...>: the hook's own reading, grep -nE '^\+' | grep -EIn
hits_added() {
    local label="$1"; shift
    "$@" 2>> "$OUT/hook-$label.err" | grep -nE '^\+' | grep -EIn -e "$PAT" | cut -d: -f1,2 >> "$OUT/hook-$label.txt"
}
HOOKS="$(git config --get core.hooksPath 2>/dev/null)"; HOOKS="${HOOKS/#\~/$HOME}"
HOOK="${HOOKS:+$HOOKS/pre-commit}"
PAT=""
s10=""
if [ -n "$HOOK" ] && [ -f "$HOOK" ] && [ "$(grep -c "^patterns='.*'\$" "$HOOK")" = 1 ] \
   && grep -qF 'git diff --cached -U0 --no-color' "$HOOK" && grep -qF 'grep -EIn "$patterns"' "$HOOK"; then
    PAT="$(sed -n "s/^patterns='\(.*\)'\$/\1/p" "$HOOK")"
fi
hook_sha="$(sha256sum "$HOOK" 2>/dev/null | cut -c1-64)"
if [ -z "$PAT" ]; then
    s10="FAIL cannot read the hook's pattern list from '${HOOK:-core.hooksPath unset}'"
else
    # self-test: the published crate, tests/ included, must be caught by both readings, only under tests/
    : > "$OUT/hook-selftest-added.txt"
    mkdir -p "$OUT/empty"
    if [ "$pub_ok" = 1 ] && hits_files "$OUT/published/zenoh-1.10.1" selftest-files; then
        hits_added selftest-added diff -ruN "$OUT/empty" "$OUT/published/zenoh-1.10.1"
    fi
    st_files="$(grep -c . "$OUT/hook-selftest-files.txt" 2>/dev/null)"; st_files="${st_files:-0}"
    st_out="$(grep -vc '^tests/' "$OUT/hook-selftest-files.txt" 2>/dev/null)"; st_out="${st_out:-0}"
    st_added="$(grep -c . "$OUT/hook-selftest-added.txt" 2>/dev/null)"; st_added="${st_added:-0}"
    if [ "$st_files" -lt 1 ] || [ "$st_out" -ne 0 ] || [ "$st_added" -lt 1 ]; then
        s10="FAIL self-test: the published crate gave $st_files file hits ($st_out outside tests/) and $st_added added-line hits; want at least 1, all under tests/"
    else
        # (a) the committed vendored tree, extracted by S3
        if [ -d "$OUT/committed/$VENDOR" ]; then hits_files "$OUT/committed/$VENDOR" committed || s10="$s10 scan error (committed);"
        else s10="$s10 no $VENDOR at $REV to scan;"; : > "$OUT/hook-committed.txt"; fi
        # (b) every file changed since the baseline, as committed at HEAD
        mkdir -p "$OUT/changed"
        git diff --name-only -z --diff-filter=d "$BASE" "$REV" > "$OUT/changed-paths.z"
        if [ -s "$OUT/changed-paths.z" ]; then
            xargs -0 git archive "$REV" -- < "$OUT/changed-paths.z" | tar -xi -C "$OUT/changed" || s10="$s10 archive error (changed);"
        fi
        nchanged="$(tr -cd '\0' < "$OUT/changed-paths.z" | wc -c)"
        hits_files "$OUT/changed" changed || s10="$s10 scan error (changed);"
        # (c) the added lines of every commit since the baseline, as the hook reads a staged diff
        : > "$OUT/hook-commits.txt"
        ncommits=0
        for c in $(git rev-list --reverse "$BASE..$REV"); do
            ncommits=$((ncommits + 1))
            before="$(grep -c . "$OUT/hook-commits.txt")"
            hits_added commits git diff -U0 --no-color "$c^" "$c"
            after="$(grep -c . "$OUT/hook-commits.txt")"
            [ "$after" -gt "$before" ] && s10="$s10 commit $(git rev-parse --short=12 "$c") adds $((after - before)) matching lines;"
        done
        # (d) the vendored directory on disk, tracked, untracked or ignored
        hits_files "$ROOT/$VENDOR" worktree || s10="$s10 scan error (worktree);"
        hc="$(grep -c . "$OUT/hook-committed.txt")"; hch="$(grep -c . "$OUT/hook-changed.txt")"
        hco="$(grep -c . "$OUT/hook-commits.txt")"; hw="$(grep -c . "$OUT/hook-worktree.txt")"
        found="$(cat "$OUT/hook-committed.txt" "$OUT/hook-changed.txt" "$OUT/hook-worktree.txt" | sort -u | head -8 | tr '\n' ' ')"
        if [ -z "$s10" ] && [ "$hc" = 0 ] && [ "$hch" = 0 ] && [ "$hco" = 0 ] && [ "$hw" = 0 ]; then
            s10="PASS hook $HOOK (sha256 $hook_sha); self-test caught the published crate at $(tr '\n' ' ' < "$OUT/hook-selftest-files.txt")($st_added added-line hits); 0 hits in the committed $VENDOR ($(find "$OUT/committed/$VENDOR" -type f | wc -l) files), 0 in the $nchanged files changed since the baseline, 0 in the added lines of $ncommits commits, 0 under $VENDOR on disk"
        else
            s10="FAIL hits: committed $hc, changed files $hch, commit lines $hco, on disk $hw [$found];$s10 (hook sha256 $hook_sha; self-test caught the published crate: $st_files file hits, all under tests/, $st_added added-line hits)"
        fi
    fi
fi
rm -rf "$OUT/published" "$OUT/empty" "$OUT/twin"
check S10-HOOK "${s10%% *}" "${s10#* }"

if [ "$fails" -eq 0 ]; then echo "NW.25 STRUCTURAL: PASS ($NCHECKS of $NCHECKS)" | tee -a "$OUT/checks.txt"; exit 0
else echo "NW.25 STRUCTURAL: FAIL ($fails of $NCHECKS failing)" | tee -a "$OUT/checks.txt"; exit 1; fi
