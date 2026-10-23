#!/bin/bash
# NW.16a, P7 (vault spec/items/NW.16a.md): runs nw16a_scouting_l3, both of
# its tests, inside one container that has no network but its own loopback,
# so that multicast scouting cannot leave the machine.
#
#   moirai-interp/tests/nw16a/p7_container.sh <test-binary> <model_node> <out-dir>
#
# <test-binary> is target/debug/deps/nw16a_scouting_l3-<hash>; both binaries
# are host builds, mounted read-only, and run on the host's own loader and C
# libraries, mounted read-only too, so the image only has to provide `sh`
# and `ip` (busybox, already present locally: nothing is pulled or built).
# Inside: `--network none` leaves `lo` alone; NET_ADMIN lets the script give
# it the address 10.77.0.1 (so a session listening on 0.0.0.0 reports a
# locator, as DT.25b's harness does), the MULTICAST flag and a route for
# 224.0.0.0/4. The container is started detached, waited for, its log kept,
# and removed; its id is printed.
set -u
TB=${1:?usage: p7_container.sh <test-binary> <model_node> <out-dir>}
NODE=${2:?usage: p7_container.sh <test-binary> <model_node> <out-dir>}
O=${3:?usage: p7_container.sh <test-binary> <model_node> <out-dir>}
TB=$(realpath "$TB"); NODE=$(realpath "$NODE")
LIB=/usr/lib/x86_64-linux-gnu
IMAGE=${NW16A_P7_IMAGE:-busybox:latest}
mkdir -p "$O"
docker image inspect "$IMAGE" >/dev/null 2>&1 \
    || { echo "no local image $IMAGE; this script pulls and builds nothing" >&2; exit 2; }
cid=$(docker run -d --network none --cap-add NET_ADMIN \
    -v "$TB":/nw16a/test:ro -v "$NODE":/nw16a/model_node:ro \
    -v "$LIB/ld-linux-x86-64.so.2":/lib64/ld-linux-x86-64.so.2:ro \
    -v "$LIB/libc.so.6":/nw16a/lib/libc.so.6:ro \
    -v "$LIB/libm.so.6":/nw16a/lib/libm.so.6:ro \
    -v "$LIB/libgcc_s.so.1":/nw16a/lib/libgcc_s.so.1:ro \
    -e LD_LIBRARY_PATH=/nw16a/lib -e MOIRAI_E2E_INTERP_NODE_BIN=/nw16a/model_node \
    -e NW16A_ISOLATED_NETNS=1 -e TMPDIR=/tmp \
    "$IMAGE" sh -c 'ip addr add 10.77.0.1/32 dev lo && ip link set lo multicast on \
        && ip route add 224.0.0.0/4 dev lo && ls /sys/class/net && ip addr show lo && ip route \
        && /nw16a/test --include-ignored --test-threads=1 --nocapture') \
    || { echo "docker run failed" >&2; exit 2; }
echo "container $cid started"
rc=$(docker wait "$cid")
docker logs "$cid" > "$O/p7_container.log" 2>&1
docker rm "$cid" >/dev/null
echo "container $cid exited $rc and was removed; log in $O/p7_container.log"
exit "$rc"
