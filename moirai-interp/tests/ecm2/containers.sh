#!/bin/bash
# EC.M2 piece 3a (spec: cea-cdrt-knowledge/spec/items/EC.M2c.md): the two Docker and
# testcontainers names that need no image, on this tree, once, by label:
#   nw11s  nw11_http_origin_policy_l2 :: nw11_s_the_stack_sends_json_and_can_set_the_list
#          (reads docker/compose, no container)
#   nw16b  nw16b_isolated_l3's three ignored names through nw16b/container.sh (busybox,
#          --network none, host-built model_node and moirai-router mounted read only)
# Release builds into $ECM2_SCRATCH/target under cargo.lock; the container under heavy.lock
# and cargo.lock together.
# The verdict lines (build exits, names run, failures, libtest summaries, the container's
# interfaces, container.sh's digest, exits) are compared with containers-expect.txt, which
# is 748a185's run of this script. Exit 0 equal, 1 a difference (printed), 75 environment
# (a lock not taken in time, no local busybox, docker run refused).
#
#   ECM2_SCRATCH=<dir> moirai-interp/tests/ecm2/containers.sh [nw11s] [nw16b]
set -uo pipefail
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
S=${ECM2_SCRATCH:?ECM2_SCRATCH names the work folder}
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
HEAVY=/volatile/home/vp242103/.cache/cea-crdt-scratch/orchestrator/heavy.lock
LABELS=${*:-nw11s nw16b}
OUT=$S/ecm2-containers
rm -rf "$OUT" && mkdir -p "$OUT/bin"
: >"$OUT/observed.txt"

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$S/target CARGO_BUILD_JOBS=2 \
        flock -E 75 -w 3600 "$LOCK" nice -n 10 cargo "$@")
}
# The test binary cargo built for target $1, from build log $2.
exe() { sed -nE "s/^ *Executable [^(]*\((.*\/deps\/$1-[0-9a-f]+)\)$/\1/p" "$2" | tail -n 1; }
summary() { grep -E '^test result:' "$1" | sed -E 's/; finished in [0-9.]+s$//'; }

worst=0
for l in $LABELS; do
    {
    case $l in
    nw11s)
        name=nw11_s_the_stack_sends_json_and_can_set_the_list
        cargo_here test -j 2 --offline --release -p moirai-interp --features network \
            --test nw11_http_origin_policy_l2 --no-run >"$OUT/nw11s-build.log" 2>&1
        code=$?
        echo "build exit $code"
        if [ "$code" = 0 ]; then
            (cd "$REPO/moirai-interp" && "$(exe nw11_http_origin_policy_l2 "$OUT/nw11s-build.log")" \
                --exact "$name") >"$OUT/nw11s.log" 2>&1
            code=$?
            sed -nE 's/^test (nw11_[a-z0-9_]+) \.\.\. ([a-zA-Z]+)$/nw11_http_origin_policy_l2 :: \1 \2/p' \
                "$OUT/nw11s.log"
            summary "$OUT/nw11s.log"
        fi
        ;;
    nw16b)
        log=$OUT/nw16b-build.log
        # model_node in its own invocation, so no other crate's features reach its build.
        cargo_here build -j 2 --offline --release -p moirai-interp --features network \
            --example model_node >"$log" 2>&1 \
            && cargo_here build -j 2 --offline --release -p moirai-router >>"$log" 2>&1 \
            && cargo_here test -j 2 --offline --release -p moirai-interp --features network \
                --test nw16b_isolated_l3 --no-run >>"$log" 2>&1
        code=$?
        echo "build exit $code"
        echo "container.sh sha256 $(sha256sum "$HERE/../nw16b/container.sh" | cut -c1-16)"
        if [ "$code" = 0 ]; then
            # Copied now, so a later build into the shared target cannot swap them under the wait.
            cp "$(exe nw16b_isolated_l3 "$log")" "$OUT/bin/nw16b_isolated_l3" \
                && cp "$S/target/release/examples/model_node" "$S/target/release/moirai-router" "$OUT/bin/"
            # heavy.lock first, then cargo.lock, so the container never runs beside a cargo build.
            (cd "$REPO/moirai-interp" && flock -E 75 -w 10800 "$HEAVY" \
                flock -E 75 -w 3600 "$LOCK" "$HERE/../nw16b/container.sh" \
                "$OUT/bin/nw16b_isolated_l3" "$OUT/bin/model_node" "$OUT/bin/moirai-router" \
                "$OUT/nw16b") >"$OUT/nw16b.log" 2>&1
            code=$?
            [ "$code" = 2 ] && code=75 # container.sh: no local image or docker run refused
            c=$OUT/nw16b/nw16b_container.log
            if [ -f "$c" ]; then
                echo "interfaces in phase 1: $(sed -n '/^== phase 1:/{n;p;q}' "$c")"
                grep -E '^== phase [12] exit' "$c"
                grep -oE '^test (l2_|r6_)[a-z0-9_]+ \.\.\. ' "$c" \
                    | sed -E 's/^test ([a-z0-9_]+) .*/nw16b_isolated_l3 :: \1 ran/'
                awk '/^failures:$/ { f = 1; next } /^test result:/ { f = 0 }
                     f && /^    [a-z0-9_]+$/ { print "nw16b_isolated_l3 :: " $1 " FAILED" }' "$c" | sort -u
                summary "$c"
            fi
        fi
        ;;
    *) echo "unknown label $l"; exit 1 ;;
    esac
    echo "exit $code"
    } 2>&1 | sed "s/^/$l /" >>"$OUT/observed.txt"
    grep "^$l exit " "$OUT/observed.txt"
    grep -q "^$l exit 75$" "$OUT/observed.txt" && worst=75
done

pattern=$(echo "$LABELS" | tr ' ' '\n' | sed 's/.*/^& /' | paste -sd'|')
grep -E "$pattern" "$HERE/containers-expect.txt" | sort >"$OUT/expected.txt"
sort -o "$OUT/observed.txt" "$OUT/observed.txt"
if diff "$OUT/expected.txt" "$OUT/observed.txt" >"$OUT/containers.diff"; then
    echo "PASS: $(wc -l <"$OUT/observed.txt") verdict lines equal to containers-expect.txt ($LABELS)"
    exit 0
fi
echo "FAIL: verdict lines differ (< expected, > observed):"
cat "$OUT/containers.diff"
[ "$worst" != 0 ] && exit $worst
exit 1
