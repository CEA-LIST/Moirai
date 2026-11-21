#!/bin/bash
# EC.03c3a's oracle (spec: cea-cdrt-knowledge/spec/items/EC.03c3.md): 748a185's side
# crates moirai-router, moirai-relay, moirai-bootnode and moirai-dashboard on this
# line, and the tests that build or start them on loopback, by name. Every check runs:
#   C1 the tree: the four crates are 748a185's byte for byte (side.sha256) and
#      workspace members; the workspace manifest is otherwise unchanged; Cargo.lock
#      gains exactly the four packages with 748a185's dependency lists and nothing
#      else moves; moirai-interp/Cargo.toml holds 748a185's thirteen [[test]]
#      entries, dev-dependencies and default features.
#   C2 the seven new cfg(network) moirai-interp targets build under default features.
#   C3 release model_node, router, relay and bootnode; every test binary of the four
#      crates, nw16b_router_pin_l0, the eight moirai-interp targets and
#      nw27_model_replication's three Zenoh names, run under the loopback shim
#      (ec03c2/loopback_bind.c, also forced into every router and node through
#      the wrappers MOIRAI_E2E_ROUTER_BIN and MOIRAI_E2E_INTERP_NODE_BIN name,
#      since the tests drop LD_PRELOAD) with skip.txt's names left out: every name
#      with 748a185's outcome (names.txt, this script run on 748a185 itself), each
#      binary's list equal to names.txt plus skip.txt, no E2E-SKIP, every bind on
#      loopback and none on 7446, 7447 or 7450, no process of the run left.
# Exit 0 when all hold, 1 otherwise, 75 when the node lock, the wait for other
# lanes' nodes, or NW.27's fixed work folder is not free (environment, not run).
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c3a/check.sh
set -uo pipefail
# names.txt is sorted under C; pin the locale so sort and diff ignore the caller's.
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
OUT=$SCRATCH/ec03c3a-check
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
NODES_LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/nodes.lock
# nw27/mod.rs's WORK, 748a185's constant: nw27_model_replication's TMPDIR must be under it.
NW27=/volatile/home/vp242103/.cache/cea-crdt-scratch/NW.27/work
INTERP="nw16b_isolated_l3 nw16b_observations_l3 nw16b_router_l3 nw16d_first_write_l2
nw16d_router_bytes_l3 nw16e_refusals_l2 nw16e_regions_l3 si13_migration_l3"
NW27_NAMES="nw27_g3_zenoh_deep_churn nw27_g4_zenoh_deep_donor nw27_g5_zenoh_ordinary_churn"
REL=$SCRATCH/target/release
NODE=$REL/examples/model_node

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

rm -rf "$OUT" && mkdir -p "$OUT/tmp"
status=0
args=()
dargs=()
for t in $INTERP; do args+=(--test "$t"); [ "$t" = si13_migration_l3 ] || dargs+=(--test "$t"); done

python3 -I - "$REPO" <<'PY' || status=1
import hashlib, json, sys, tomllib
repo = sys.argv[1]
load = lambda p: tomllib.load(open(f"{repo}/{p}", "rb"))
digest = lambda v: hashlib.sha256(json.dumps(v, sort_keys=True).encode()).hexdigest()
side = {"moirai-router": ["zenoh"], "moirai-relay": ["serde", "serde_json", "tiny_http"],
        "moirai-bootnode": ["serde", "serde_json", "tiny_http"],
        "moirai-dashboard": ["moirai-network", "reqwest", "serde", "serde_json", "tiny_http"]}
base = ["moirai-protocol", "moirai-crdt", "moirai-fuzz", "moirai-macros", "moirai-semantics",
        "moirai-interp", "moirai-network"]
ws = load("Cargo.toml")
members = ws["workspace"].pop("members", [])
ws_ok = sorted(members) == sorted(base + list(side)) and \
    digest(ws) == "28a1666db04a79ed1e1a927a71748a920757135dfe2f46a313c99ed8b0b92c81"
print(f"C1 workspace: {'ok' if ws_ok else 'FAIL'}: members {sorted(members)}")
lock = load("Cargo.lock")["package"]
got = sorted((p for p in lock if p["name"] in side), key=lambda p: p["name"])
want = [{"name": n, "version": "0.1.0", "dependencies": side[n]} for n in sorted(side)]
rest = [p for p in lock if p["name"] not in side]
lock_ok = got == want and \
    digest(rest) == "18256e50f766bce550216db89affb1d61d7e6965ddb09dfc7a25b42fb15cd0f5"
print(f"C1 Cargo.lock: {'ok' if lock_ok else 'FAIL'}: side packages {[p['name'] for p in got]}, "
      f"others {'unchanged' if digest(rest) == '18256e50f766bce550216db89affb1d61d7e6965ddb09dfc7a25b42fb15cd0f5' else 'CHANGED'}")
m = load("moirai-interp/Cargo.toml")
entries = ["si12_copy_migration", "si12_copy_migration_l3", "si12_fixtures", "si12_scale_l3",
           "si13_concurrent_migrations", "si13_copy_in_progress", "si13_migration_l3",
           "si13_scale_l3", "si14_fixtures", "si14_rebase", "si14_rebase_l3",
           "si16_rebased_joiner", "si16_rebased_joiner_l3"]
tests = m.get("test", [])
t_ok = sorted(tests, key=lambda t: t.get("name", "")) == \
    [{"name": n, "required-features": ["network"]} for n in entries]
dev = m.get("dev-dependencies", {})
zenoh = {"version": "=1.10.1", "default-features": False, "features": ["transport_tcp", "unstable"]}
dev_ok = sorted(dev) == ["rand_chacha", "reqwest", "zenoh"] and dev.get("zenoh") == zenoh
default_ok = m.get("features", {}).get("default") == ["serde", "test_utils"]
print(f"C1 moirai-interp manifest: {'ok' if t_ok and dev_ok and default_ok else 'FAIL'}: "
      f"[[test]] {sorted(t.get('name') for t in tests)}, dev {sorted(dev)}")
sys.exit(0 if ws_ok and lock_ok and t_ok and dev_ok and default_ok else 1)
PY
# The four crates hold exactly side.sha256's files, each 748a185's.
listed=$(cd "$REPO" && find moirai-bootnode moirai-dashboard moirai-relay moirai-router -type f 2>/dev/null | sort)
if [ "$listed" = "$(awk '{print $2}' "$HERE/side.sha256" | sort)" ] \
    && (cd "$REPO" && sha256sum --quiet -c "$HERE/side.sha256") >"$OUT/side.log" 2>&1; then
    echo "C1 side crates: ok: $(wc -l <"$HERE/side.sha256") files equal to 748a185's"
else
    echo "C1 side crates: FAIL: $(echo "$listed" | grep -c .) files found, $(grep -c FAILED "$OUT/side.log") differ"
    status=1
fi

# si13_migration_l3 is left out here: cargo refuses to name a target whose required
# features are off, and C1 checks its [[test]] entry instead.
cargo_here test -j 2 --offline -p moirai-interp "${dargs[@]}" --no-run >"$OUT/default.log" 2>&1
code=$?
echo "C2 default features build: exit $code"
[ "$code" = 0 ] || { grep -m5 -E '^error' "$OUT/default.log"; status=1; }

SO=$OUT/loopback_bind.so
cc -shared -fPIC -O2 -o "$SO" "$HERE/../ec03c2/loopback_bind.c" -ldl \
    || { echo "C3 FAIL: the shim did not build"; exit 1; }
# The router and the node under the shim whatever LD_PRELOAD the test hands them.
for w in "router:$REL/moirai-router" "node:$NODE"; do
    printf '#!/bin/sh\ncase ":${LD_PRELOAD:-}:" in *":%s:"*) ;; *) LD_PRELOAD="%s${LD_PRELOAD:+:$LD_PRELOAD}"; export LD_PRELOAD ;; esac\nexec %s "$@"\n' \
        "$SO" "$SO" "${w#*:}" >"$OUT/${w%%:*}-wrap"
    chmod +x "$OUT/${w%%:*}-wrap"
done
# model_node in its own invocation, so no side crate's features reach its build.
cargo_here build -j 2 --offline --release -p moirai-interp --features network --example model_node \
    >"$OUT/build.log" 2>&1 \
    && cargo_here test -j 2 --offline --release -p moirai-interp --features network "${args[@]}" \
        --test nw27_model_replication --no-run >>"$OUT/build.log" 2>&1 \
    && cargo_here build -j 2 --offline --release -p moirai-router -p moirai-relay -p moirai-bootnode \
        >>"$OUT/build.log" 2>&1 \
    && cargo_here test -j 2 --offline --release -p moirai-router -p moirai-relay -p moirai-bootnode \
        -p moirai-dashboard --no-run >>"$OUT/build.log" 2>&1 \
    && cargo_here test -j 2 --offline --release -p moirai-network --test nw16b_router_pin_l0 \
        --no-run >>"$OUT/build.log" 2>&1
code=$?
echo "C3 build: exit $code"
if [ "$code" != 0 ]; then
    grep -m5 -E '^error' "$OUT/build.log"
    echo FAIL; exit 1
fi

[ -e "$NW27" ] && { echo "ENV: $NW27 exists (another run's); not run"; exit 75; }

# `<label> <dir> <executable>`: the label is the target, the directory cargo would run it from.
sed -nE 's/^ *Executable (unittests )?([^ ]+) \((.*)\)$/\3/p' "$OUT/build.log" | sort -u | while read -r exe; do
    label=$(basename "$exe" | sed -E 's/-[0-9a-f]+$//')
    case $label in
        moirai_relay) dir=moirai-relay ;; moirai_bootnode) dir=moirai-bootnode ;;
        moirai_dashboard) dir=moirai-dashboard ;;
        moirai_router | nw16b_router_config_l1 | nw16e_router_regions_l1) dir=moirai-router ;;
        nw16b_router_pin_l0) dir=moirai-network ;;
        *) dir=moirai-interp ;;
    esac
    echo "$label $dir $exe"
done >"$OUT/binaries.txt"

# Every test binary runs serially, holding the node lock, once no other node runs.
run_all() {
    for i in $(seq 60); do pgrep -x model_node >/dev/null || break; sleep 30; done
    pgrep -x model_node >/dev/null && { echo "ENV: another lane's node stayed 30 minutes"; return 75; }
    mkdir -p "$NW27/tmp"
    while read -r label dir exe; do
        log=$OUT/run-$(basename "$exe").log
        filters=(--exact)
        tmp=$OUT/tmp
        if [ "$label" = nw27_model_replication ]; then
            filters+=($NW27_NAMES)
            tmp=$NW27/tmp
        else
            for n in $(awk -v t="$label" '$1 == t {print $2}' "$HERE/skip.txt"); do filters+=(--skip "$n"); done
        fi
        "$exe" --list "${filters[@]}" </dev/null >"$OUT/list-$(basename "$exe").txt" 2>&1
        (cd "$REPO/$dir" && env -u MOIRAI_TRANSPORT -u ZENOH_SCOUTING -u ZENOH_ROUTER \
            -u ZENOH_MODE -u ZENOH_CONFIG -u ZENOH_REGION -u ROUTER_REGIONS -u BOOTNODE_URL \
            -u PEERS -u PEERS_FILE -u PORTS_FILE -u LISTEN_PORT -u DASHBOARD_URL -u RUST_LOG \
            -u NW07_SCRATCH -u NW11_SCRATCH -u NW27_CASE -u NW16A_ISOLATED_NETNS \
            -u MOIRAI_E2E_NODE_BIN \
            TMPDIR="$tmp" MOIRAI_E2E_INTERP_NODE_BIN="$OUT/node-wrap" \
            MOIRAI_E2E_ROUTER_BIN="$OUT/router-wrap" MOIRAI_E2E_RELAY_BIN="$REL/moirai-relay" \
            MOIRAI_E2E_BOOTNODE_BIN="$REL/moirai-bootnode" NW27_LOOPBACK_SHIM="$SO" \
            LD_PRELOAD="$SO" EC03C2_BIND_LOG="$OUT/binds.log" \
            "$exe" --test-threads=1 "${filters[@]}" </dev/null) >"$log" 2>&1
        echo "$label ($(basename "$exe")): exit $?"
    done <"$OUT/binaries.txt"
    rm -rf "$NW27"
}
export -f run_all
export OUT NODE REPO HERE NW27 NW27_NAMES REL SO
flock -E 75 -w 3600 "$NODES_LOCK" bash -c run_all
code=$?
[ "$code" = 75 ] && { echo "ENV: not run (node lock)"; exit 75; }

# `<label> :: <name> <ok|FAILED|ignored>`, one line per test a binary ran.
while read -r label dir exe; do
    awk -v t="$label" '/^test .* \.\.\. /{o=$NF; if ($0 ~ /\.\.\. ignored/) o="ignored"; print t" :: "$2" "o}' \
        "$OUT/run-$(basename "$exe").log"
done <"$OUT/binaries.txt" | sort >"$OUT/names.txt"
if diff "$HERE/names.txt" "$OUT/names.txt" >"$OUT/names.diff"; then
    echo "C3 names: $(wc -l <"$OUT/names.txt") equal to names.txt"
else
    echo "C3 names: FAIL, differences (< expected, > observed):"
    cat "$OUT/names.diff"
    status=1
fi
# Each binary's own list is names.txt's names plus skip.txt's: nothing is dropped unnamed.
while read -r label dir exe; do
    "$exe" --list </dev/null | sed -nE "s/^(.*): test$/$label \1/p"
done <"$OUT/binaries.txt" | awk -v keep=" $NW27_NAMES " \
    '$1 != "nw27_model_replication" || index(keep, " "$2" ")' | sort >"$OUT/listed.txt"
{ awk '{print $1, $3}' "$HERE/names.txt"; awk '{print $1, $2}' "$HERE/skip.txt"; } | sort >"$OUT/expected-list.txt"
if cmp -s "$OUT/expected-list.txt" "$OUT/listed.txt"; then
    echo "C3 list: $(wc -l <"$OUT/listed.txt") names, names.txt plus skip.txt"
else
    echo "C3 FAIL: the binaries list other names than names.txt plus skip.txt"
    diff "$OUT/expected-list.txt" "$OUT/listed.txt"
    status=1
fi
grep -l 'E2E-SKIP' "$OUT"/run-*.log && { echo "C3 FAIL: a scenario skipped"; status=1; }
if [ ! -s "$OUT/binds.log" ]; then
    echo "C3 FAIL: the shim logged no bind"; status=1
elif awk '($2 !~ /^127\./ && $2 != "::1") || $3 == 7446 || $3 == 7447 || $3 == 7450' "$OUT/binds.log" | grep -q .; then
    echo "C3 FAIL: a bind beyond loopback or on a scouting port"; status=1
fi
echo "C3 binds: $(wc -l <"$OUT/binds.log" 2>/dev/null || echo 0), $(awk '$4 == "rewritten"' "$OUT/binds.log" 2>/dev/null | wc -l) rewritten from the wildcard"
left=0
for p in $(pgrep -x model_node; pgrep -x moirai-router; pgrep -x moirai-relay; pgrep -x moirai-bootnode); do
    readlink "/proc/$p/exe" | grep -qF "$REL/" && left=$((left + 1))
done
[ "$left" = 0 ] || { echo "C3 FAIL: $left process(es) of this run left running"; status=1; }

[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
