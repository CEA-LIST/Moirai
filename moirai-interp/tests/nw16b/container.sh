#!/bin/bash
# NW.16b, L2 and R6 (vault spec/items/NW.16b.md): runs nw16b_isolated_l3
# inside one container that has no network but its own loopback, so that
# nothing a test starts can reach the host's network, and multicast
# scouting (R6's Ledgera-configured peer, its witness router, and the
# router of control arm M1) never leaves the container.
#
#   moirai-interp/tests/nw16b/container.sh <test-binary> <model_node> <moirai-router> <out-dir>
#
# <test-binary> is target/debug/deps/nw16b_isolated_l3-<hash>. The three
# binaries are host builds, mounted read-only, and run on the host's own
# loader and C libraries, mounted read-only too, so the image only has to
# provide `sh` and `ip` (busybox, already present locally: nothing is pulled
# or built). A <moirai-router> that is not a file is not mounted, and the
# tests that need it say so.
#
# Phase 1, `lo` alone with 127.0.0.1 only: the L2 tests, where a Zenoh
# session listening on 0.0.0.0 lists no locator. Phase 2, after NET_ADMIN
# gives `lo` the address 10.77.0.1, the MULTICAST flag and a route for
# 224.0.0.0/4 (as NW.16a's p7_container.sh and DT.25b's harness do): R6.
# The container is started detached, waited for, its log kept, and removed;
# its id is printed. Exit 0 only when both phases passed.
set -u
U="usage: container.sh <test-binary> <model_node> <moirai-router> <out-dir>"
TB=${1:?$U}
NODE=${2:?$U}
ROUTER=${3:?$U}
O=${4:?$U}
TB=$(realpath "$TB"); NODE=$(realpath "$NODE")
LIB=/usr/lib/x86_64-linux-gnu
IMAGE=${NW16B_IMAGE:-busybox:latest}
mkdir -p "$O"
docker image inspect "$IMAGE" >/dev/null 2>&1 \
    || { echo "no local image $IMAGE; this script pulls and builds nothing" >&2; exit 2; }
ROUTER_MOUNT=()
if [ -f "$ROUTER" ]; then
    ROUTER=$(realpath "$ROUTER")
    ROUTER_MOUNT=(-v "$ROUTER":/nw16b/moirai-router:ro)
else
    echo "no router binary at $ROUTER; not mounted"
fi
cid=$(docker run -d --network none --cap-add NET_ADMIN \
    -v "$TB":/nw16b/test:ro -v "$NODE":/nw16b/model_node:ro "${ROUTER_MOUNT[@]}" \
    -v "$LIB/ld-linux-x86-64.so.2":/lib64/ld-linux-x86-64.so.2:ro \
    -v "$LIB/libc.so.6":/nw16b/lib/libc.so.6:ro \
    -v "$LIB/libm.so.6":/nw16b/lib/libm.so.6:ro \
    -v "$LIB/libgcc_s.so.1":/nw16b/lib/libgcc_s.so.1:ro \
    -e LD_LIBRARY_PATH=/nw16b/lib -e MOIRAI_E2E_INTERP_NODE_BIN=/nw16b/model_node \
    -e MOIRAI_E2E_ROUTER_BIN=/nw16b/moirai-router \
    -e NW16A_ISOLATED_NETNS=1 -e TMPDIR=/tmp \
    "$IMAGE" sh -c 'echo "== phase 1: lo alone, 127.0.0.1 only"; ls /sys/class/net; ip addr show lo
        /nw16b/test --include-ignored --test-threads=1 --nocapture l2_; p1=$?
        echo "== phase 1 exit $p1"
        ip addr add 10.77.0.1/32 dev lo && ip link set lo multicast on \
            && ip route add 224.0.0.0/4 dev lo && ip addr show lo && ip route || exit 3
        echo "== phase 2: 10.77.0.1 and multicast on lo"
        /nw16b/test --include-ignored --test-threads=1 --nocapture r6_; p2=$?
        echo "== phase 2 exit $p2"
        [ "$p1" -eq 0 ] && [ "$p2" -eq 0 ]') \
    || { echo "docker run failed" >&2; exit 2; }
echo "container $cid started"
rc=$(docker wait "$cid")
docker logs "$cid" > "$O/nw16b_container.log" 2>&1
docker rm "$cid" >/dev/null
echo "container $cid exited $rc and was removed; log in $O/nw16b_container.log"
exit "$rc"
