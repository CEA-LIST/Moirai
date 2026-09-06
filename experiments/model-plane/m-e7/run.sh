#!/usr/bin/env bash
#
# M-E7 — per-operation cost of the structural check, and what it would cost
# on the receive path.
#
# In process, `examples/conformance_cost.rs` of the Arachne-generated crate
# (the check lives beside the node binary, so the example lives beside it
# too), at N in {1, 4, 16, 64} hosted models and for both descriptors, `bt`
# and `uml`, over 100,000 well-formed operations from the seeded generator
# mp32 uses:
#
#   check alone  `check_structure` per operation, timed one by one with
#                `Instant`; p50 and p99.
#   receive      the same operations delivered to the real node through
#                `handle_transport_message`, timed as one run with the check
#                off (the node as shipped) and again on a fresh node with the
#                check on, which is the serialization plus the check before
#                every delivery. The check is off on receive by design; the
#                on arm is an experiment arm and not a mode the node has.
#
# Run in release, the profile a deployment ships, and again in debug, the
# profile the rig image is built with, as M-E2 does.
#
# Thresholds, proposed in the plan and for decision after the first run: p50
# at most 10 us and p99 at most 50 us per operation at the largest N on either
# descriptor, and the receive run with the check on at most 1.10 x the run
# with it off. The example prints each verdict; a crossed one is recorded in
# the manifest and the run goes on, so the CSV is complete either way.
#
# Usage, from this directory:
#
#     ./run.sh
#
# Knobs: POINTS (default 1,4,16,64), OPS (100000), SEED (20260903),
# PROFILES (release,debug).

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
. "$HERE/../common.sh"

POINTS=${POINTS:-1,4,16,64}
OPS=${OPS:-10000}
SEED=${SEED:-20260903}
PROFILES=${PROFILES:-release,debug}
CRATE="$ARACHNE_ROOT/generated/json_crdt"
DESCRIPTORS="$ARACHNE_ROOT/examples"

[ -f "$CRATE/examples/conformance_cost.rs" ] \
    || mp_die "no conformance_cost example at $CRATE/examples; is ARACHNE_ROOT right?"

stamp=$(mp_stamp)
results="$HERE/results.csv"
crossed=()
verdicts=()

# The example prints one `met:` or `CROSSED:` line per threshold on stderr;
# they are kept, per profile, for the manifest.
run_profile() {
    local profile="$1" out="$2" flag=()
    local log="$HERE/.$profile.log"
    [ "$profile" = release ] && flag=(--release)
    mp_say "building and running the example in $profile (load $(mp_load_average))"
    if ! ( cd "$CRATE" && nice -n 10 cargo run -q "${flag[@]}" -j 2 --example conformance_cost -- \
            --descriptors "$DESCRIPTORS" --out "$out" --points "$POINTS" --ops "$OPS" --seed "$SEED" ) 2>&1 \
            | grep -vE '^warning|^\s+-->|^\s+\||^\s*$|^\s+=|^[0-9]+ \|' | tee "$log" >&2; then
        crossed+=("$profile: $(grep '^CROSSED:' "$log" | sed 's/^CROSSED: //' | paste -sd ';' -)")
    fi
    verdicts+=("$profile: $(grep -E '^(met|CROSSED):' "$log" | paste -sd ';' -)")
    rm -f "$log"
}

first=1
for profile in ${PROFILES//,/ }; do
    part="$HERE/.$profile.csv"
    run_profile "$profile" "$part"
    if [ "$first" -eq 1 ]; then cat "$part" > "$results"; first=0; else tail -n +2 "$part" >> "$results"; fi
    rm -f "$part"
done
mp_say "wrote $results"

{
    mp_manifest_common "$stamp" "POINTS=$POINTS OPS=$OPS SEED=$SEED PROFILES=$PROFILES ./run.sh"
    printf '%-14s%s\n' points "$POINTS (hosted logs per node; the operations are spread over them round-robin)"
    printf '%-14s%s\n' ops "$OPS well-formed operations per descriptor from the seeded generator (tests/support/conformance_workload.rs), seed $SEED, every position 0 so each applies"
    printf '%-14s%s\n' descriptors "bt and uml, $DESCRIPTORS, the schema parsed once per descriptor"
    printf '%-14s%s\n' check "check_structure over the serialized operation, each call timed on its own with Instant; the serialization timed beside it; timer_p50_ns is Instant around nothing"
    printf '%-14s%s\n' receive "writer replicas (one per log, rotated every 1,000 operations so their own documents stay small) send the frames; the node delivers them through handle_transport_message; check off is the node as shipped, check on serializes and checks before every delivery"
    printf '%-14s%s\n' thresholds "proposed, FOR-DECISION: check p50 <= 10 us and p99 <= 50 us at the largest N on either descriptor; receive with the check on <= 1.10 x off"
    local_verdict=""
    for local_verdict in "${verdicts[@]}"; do printf '%-14s%s\n' verdicts "$local_verdict"; done
    if [ "${#crossed[@]}" -gt 0 ]; then
        printf '%-14s%s\n' crossed "${crossed[*]}"
    else
        printf '%-14s%s\n' crossed "none"
    fi
} > "$HERE/manifest.txt"
cat "$HERE/manifest.txt"
[ "${#crossed[@]}" -eq 0 ]
