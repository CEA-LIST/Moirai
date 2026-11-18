#!/bin/bash
# EC.03c3b1's oracle (spec: cea-cdrt-knowledge/spec/items/EC.03c3b1.md): 748a185's
# nw19_default_node_l3 (its three model_node names) and nw19_e2e_free_ports on this
# line by name, on loopback, with e2e_convergence built as a target and not run.
#   C1 the three test files are 748a185's byte for byte (files.sha256).
#   C3 release model_node, nw19_default_node_l3, nw19_e2e_free_ports and
#      e2e_convergence build; the first two run under the loopback shim
#      (ec03c2/loopback_bind.c, also forced into model_node through the wrapper
#      MOIRAI_E2E_INTERP_NODE_BIN names) with skip.txt's names left out: every name
#      with 748a185's outcome (names.txt, this script run on 748a185 itself); each
#      binary's list, e2e_convergence's too, equals names.txt plus skip.txt; no
#      E2E-SKIP; every bind on loopback, none on 7446, 7447 or 7450; no model_node
#      of the run left.
# Exit 0 when all hold, 1 otherwise, 75 when the node lock or the wait for other
# lanes' nodes is not free (environment, not run).
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c3b1/check.sh
set -uo pipefail
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
OUT=$SCRATCH/ec03c3b1-check
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
NODES_LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/nodes.lock
REL=$SCRATCH/target/release
NODE=$REL/examples/model_node

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

rm -rf "$OUT" && mkdir -p "$OUT/tmp"
status=0

if (cd "$REPO" && sha256sum --quiet -c "$HERE/files.sha256") >"$OUT/files.log" 2>&1; then
    echo "C1 test files: ok: $(wc -l <"$HERE/files.sha256") equal to 748a185's"
else
    echo "C1 test files: FAIL: $(tr '\n' ' ' <"$OUT/files.log")"
    status=1
fi

SO=$OUT/loopback_bind.so
cc -shared -fPIC -O2 -o "$SO" "$HERE/../ec03c2/loopback_bind.c" -ldl \
    || { echo "C3 FAIL: the shim did not build"; exit 1; }
printf '#!/bin/sh\ncase ":${LD_PRELOAD:-}:" in *":%s:"*) ;; *) LD_PRELOAD="%s${LD_PRELOAD:+:$LD_PRELOAD}"; export LD_PRELOAD ;; esac\nexec %s "$@"\n' \
    "$SO" "$SO" "$NODE" >"$OUT/node-wrap"
chmod +x "$OUT/node-wrap"

cargo_here build -j 2 --offline --release -p moirai-interp --features network --example model_node \
    >"$OUT/build.log" 2>&1 \
    && cargo_here test -j 2 --offline --release -p moirai-interp --features network \
        --test nw19_default_node_l3 --no-run >>"$OUT/build.log" 2>&1 \
    && cargo_here test -j 2 --offline --release -p moirai-network --test nw19_e2e_free_ports \
        --test e2e_convergence --no-run >>"$OUT/build.log" 2>&1
code=$?
echo "C3 build: exit $code"
if [ "$code" != 0 ]; then
    grep -m5 -E '^error' "$OUT/build.log"
    echo FAIL; exit 1
fi

# `<label> <dir> <executable>`: the label is the target, the directory cargo would run it from.
sed -nE 's/^ *Executable (unittests )?([^ ]+) \((.*)\)$/\3/p' "$OUT/build.log" | sort -u | while read -r exe; do
    label=$(basename "$exe" | sed -E 's/-[0-9a-f]+$//')
    case $label in nw19_default_node_l3) dir=moirai-interp ;; *) dir=moirai-network ;; esac
    echo "$label $dir $exe"
done >"$OUT/binaries.txt"

# e2e_convergence is built and listed, never run: its names are skip.txt's.
run_all() {
    for i in $(seq 60); do pgrep -x model_node >/dev/null || break; sleep 30; done
    pgrep -x model_node >/dev/null && { echo "ENV: another lane's node stayed 30 minutes"; return 75; }
    while read -r label dir exe; do
        [ "$label" = e2e_convergence ] && continue
        filters=(--exact)
        for n in $(awk -v t="$label" '$1 == t {print $2}' "$HERE/skip.txt"); do filters+=(--skip "$n"); done
        (cd "$REPO/$dir" && env -u MOIRAI_TRANSPORT -u ZENOH_SCOUTING -u ZENOH_ROUTER \
            -u ZENOH_MODE -u ZENOH_CONFIG -u ZENOH_REGION -u ROUTER_REGIONS -u BOOTNODE_URL \
            -u PEERS -u PEERS_FILE -u PORTS_FILE -u LISTEN_PORT -u DASHBOARD_URL -u RUST_LOG \
            -u MOIRAI_E2E_NODE_BIN -u MOIRAI_E2E_BACKEND -u MOIRAI_E2E_IMAGE \
            TMPDIR="$OUT/tmp" MOIRAI_E2E_INTERP_NODE_BIN="$OUT/node-wrap" \
            LD_PRELOAD="$SO" EC03C2_BIND_LOG="$OUT/binds.log" \
            "$exe" --test-threads=1 "${filters[@]}" </dev/null) >"$OUT/run-$(basename "$exe").log" 2>&1
        echo "$label ($(basename "$exe")): exit $?"
    done <"$OUT/binaries.txt"
}
export -f run_all
export OUT REPO HERE SO
flock -E 75 -w 3600 "$NODES_LOCK" bash -c run_all
code=$?
[ "$code" = 75 ] && { echo "ENV: not run (node lock)"; exit 75; }

# `<label> :: <name> <ok|FAILED|ignored>`, one line per test a binary ran.
while read -r label dir exe; do
    [ "$label" = e2e_convergence ] && continue
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
while read -r label dir exe; do
    "$exe" --list </dev/null | sed -nE "s/^(.*): test$/$label \1/p"
done <"$OUT/binaries.txt" | sort >"$OUT/listed.txt"
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
for p in $(pgrep -x model_node); do
    readlink "/proc/$p/exe" | grep -qF "$REL/" && left=$((left + 1))
done
[ "$left" = 0 ] || { echo "C3 FAIL: $left process(es) of this run left running"; status=1; }

[ "$status" = 0 ] && echo PASS || echo FAIL
exit $status
