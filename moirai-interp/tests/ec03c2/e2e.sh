#!/bin/bash
# EC.03c2's O6 (spec: cea-cdrt-knowledge/spec/items/EC.03c2.md): the
# candidate's model_node builds under the `network` feature, and 748a185's
# e2e_interp passes by name on it, on Zenoh (MOIRAI_TRANSPORT unset) and on
# TCP (MOIRAI_TRANSPORT=tcp), every bind on loopback (loopback_bind.c).
#
# Exit 0: the build passed and both runs passed the seven names, no skip, no
# bind beyond loopback. Non-zero otherwise.
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c2/e2e.sh
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
OUT=$SCRATCH/ec03c2-e2e
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
NAMES="ip18_one_binary_hosts_a_behaviour_tree_and_a_uml_model_at_once
ip19_a_descriptor_posted_to_running_nodes_is_usable_without_a_restart
ip23_a_replica_without_the_descriptor_drops_the_frames_and_counts_them
ip24_a_create_is_refused_where_a_join_is_hosted_with_no_table
ip25_two_descriptors_sharing_an_ns_uri_are_two_metamodels
ip26_an_edited_metamodel_leaves_the_old_models_converging
ip27_two_replicas_holding_no_descriptor_join_a_model_and_get_it_from_the_log"

cargo_here() {
    (cd "$REPO" && CARGO_TARGET_DIR=$SCRATCH/target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

rm -rf "$OUT" && mkdir -p "$OUT/tmp"
cc -shared -fPIC -O2 -o "$OUT/loopback_bind.so" "$HERE/loopback_bind.c" -ldl || exit 2
cargo_here build -j 2 --offline --release -p moirai-interp --features network --example model_node \
    || { echo "FAIL: model_node did not build under --features network"; exit 1; }
cargo_here test -j 2 --offline --release -p moirai-interp --features network --test e2e_interp --no-run \
    || { echo "FAIL: e2e_interp did not build"; exit 1; }

status=0
for transport in zenoh tcp; do
    log=$OUT/e2e-$transport.log
    (if [ "$transport" = tcp ]; then export MOIRAI_TRANSPORT=tcp; else unset MOIRAI_TRANSPORT; fi
     TMPDIR=$OUT/tmp MOIRAI_E2E_INTERP_NODE_BIN=$SCRATCH/target/release/examples/model_node \
     LD_PRELOAD=$OUT/loopback_bind.so EC03C2_BIND_LOG=$OUT/binds-$transport.log \
     cargo_here test -j 2 --offline --release -p moirai-interp --features network --test e2e_interp \
        -- --test-threads=1) >"$log" 2>&1
    passed=$(grep -c '^test .* \.\.\. ok$' "$log")
    for name in $NAMES; do
        grep -q "^test $name \.\.\. ok$" "$log" || { echo "FAIL ($transport): $name did not pass"; status=1; }
    done
    grep -q 'E2E-SKIP' "$log" && { echo "FAIL ($transport): a scenario skipped"; status=1; }
    [ -s "$OUT/binds-$transport.log" ] || { echo "FAIL ($transport): the shim logged no bind"; status=1; }
    if awk '$2 !~ /^127\./ && $2 != "::1"' "$OUT/binds-$transport.log" 2>/dev/null | grep -q .; then
        echo "FAIL ($transport): a bind beyond loopback"; status=1
    fi
    echo "e2e_interp ($transport): $passed passed; $(grep -m1 '^test result' "$log")"
done
exit $status
