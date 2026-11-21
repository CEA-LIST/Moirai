#!/bin/bash
# EC.03c2's node differential (spec: cea-cdrt-knowledge/spec/items/EC.03c2.md).
#
# EC.03's 24 schedules, as ec03/run.sh recorded them (run it first: this
# script reads $EC03_SCRATCH/ec03-run/schedules and checks their sha256),
# driven by nodes.py through four model_node processes on loopback, on
# 748a185's model_node and on the candidate's, and the traces compared line by
# line: on Zenoh (MOIRAI_TRANSPORT unset) for all 24, on TCP
# (MOIRAI_TRANSPORT=tcp) for the six named below. Every process runs under
# loopback_bind.so.
#
# Exit 0: every read and answer equal, the candidate's binds on loopback and
# its nodes on the transport asked for. 1: a line differs (the first is
# printed) or a check fails. 2: the instrument failed (S1 to S5) on the
# reference, so nothing was compared.
#
#   EC03_SCRATCH=<dir> moirai-interp/tests/ec03c2/run.sh
set -uo pipefail
# The schedules are hashed in sort order; pin the locale so the order is the bytes', not the caller's.
export LC_ALL=C

HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
SCRATCH=${EC03_SCRATCH:?EC03_SCRATCH names the work folder}
SCHEDULES=$SCRATCH/ec03-run/schedules
REF=$SCRATCH/ref-c2
OUT=$SCRATCH/ec03c2-run
LOCK=/volatile/home/vp242103/.cache/cea-crdt-scratch/cargo.lock
TCP_ONLY=ip12-bt-0,bench-0,json-0,uml-0,rebase-d2-0,rebase-drop-0

cargo_in() { # dir target-dir args...
    local dir=$1 target=$2
    shift 2
    (cd "$dir" && CARGO_TARGET_DIR=$target CARGO_BUILD_JOBS=2 \
        flock -w 3000 "$LOCK" nice -n 10 cargo "$@")
}

# S1: the schedules are EC.03's registered ones.
sha=$(cd "$SCHEDULES" 2>/dev/null && cat $(ls *.jsonl | sort) | sha256sum | cut -d' ' -f1)
[ "$sha" = "$(cat "$REPO/moirai-interp/tests/ec03/schedules.sha256")" ] \
    || { echo "S1 FAIL: no registered schedules in $SCHEDULES; run ec03/run.sh first"; exit 2; }

rm -rf "$OUT" && mkdir -p "$OUT"
cc -shared -fPIC -O2 -o "$OUT/loopback_bind.so" "$HERE/loopback_bind.c" -ldl \
    || { echo "S2: the shim did not build"; exit 2; }

if [ ! -f "$REF/.ec03c2-748a185" ]; then
    rm -rf "$REF" && mkdir -p "$REF"
    git -C "$REPO" archive 748a185 | tar -x -C "$REF" || exit 2
    touch "$REF/.ec03c2-748a185"
fi
echo "== build model_node: 748a185, then the candidate ($(git -C "$REPO" rev-parse --short HEAD))"
cargo_in "$REF" "$SCRATCH/target-ref" build -j 2 --offline --release -p moirai-interp \
    --features network --example model_node || { echo "S2: the reference did not build"; exit 2; }
cargo_in "$REPO" "$SCRATCH/target" build -j 2 --offline --release -p moirai-interp \
    --features network --example model_node || { echo "FAIL: the candidate's model_node did not build"; exit 1; }

drive() { # label binary transport [only]
    local label=$1 bin=$2 transport=$3 only=${4:-}
    echo "== $label"
    (if [ "$transport" = tcp ]; then export MOIRAI_TRANSPORT=tcp; else unset MOIRAI_TRANSPORT; fi
     EC03C2_BIND_LOG=$OUT/binds-$label.log python3 -I "$HERE/nodes.py" --bin "$bin" \
        --schedules "$SCHEDULES" --out "$OUT/trace-$label.txt" --shim "$OUT/loopback_bind.so" \
        --scratch "$OUT/nodes-$label" --only "$only")
}

# S4: every bind of the run was on loopback, and the shim saw some.
binds_ok() { # label
    local log=$OUT/binds-$1.log
    [ -s "$log" ] || { echo "S4 FAIL: $1: the shim logged no bind"; return 1; }
    if awk '$2 !~ /^127\./ && $2 != "::1"' "$log" | grep -q .; then
        echo "S4 FAIL: $1 bound beyond loopback:"; awk '$2 !~ /^127\./ && $2 != "::1"' "$log" | head -3
        return 1
    fi
}

# S5 on the reference, a check on the candidate: every node of a run without
# MOIRAI_TRANSPORT opened a Zenoh session, and every node of a TCP run
# listened on TCP and opened none.
transport_ok() { # label transport
    local logs=("$OUT/nodes-$1"/*/[abcd].log) bad=0 log
    [ -e "${logs[0]}" ] || { echo "S5 FAIL: $1: no node log"; return 1; }
    for log in "${logs[@]}"; do
        if [ "$2" = zenoh ]; then
            grep -q 'Zenoh transport in session' "$log" || bad=$((bad + 1))
        else
            { grep -q 'TCP transport listening' "$log" && ! grep -q 'Zenoh transport in session' "$log"; } \
                || bad=$((bad + 1))
        fi
    done
    [ $bad = 0 ] || { echo "S5 FAIL: $1: $bad of ${#logs[@]} nodes not on $2"; return 1; }
}

diffs=0
for transport in zenoh tcp; do
    only=""; [ "$transport" = tcp ] && only=$TCP_ONLY
    drive "ref-$transport" "$SCRATCH/target-ref/release/examples/model_node" $transport "$only" \
        || { echo "S3: the reference run broke ($transport)"; grep broken "$OUT/trace-ref-$transport.txt"; exit 2; }
    drive "ref2-$transport" "$SCRATCH/target-ref/release/examples/model_node" $transport "$only" \
        || { echo "S3: the second reference run broke ($transport)"; exit 2; }
    # S3: the reference is deterministic under this harness.
    cmp -s "$OUT/trace-ref-$transport.txt" "$OUT/trace-ref2-$transport.txt" \
        || { echo "S3 FAIL: two reference runs differ ($transport):"; diff "$OUT/trace-ref-$transport.txt" "$OUT/trace-ref2-$transport.txt" | head -4; exit 2; }
    binds_ok "ref-$transport" && binds_ok "ref2-$transport" || exit 2
    transport_ok "ref-$transport" $transport || exit 2
    echo "reference ($transport): $(wc -l < "$OUT/trace-ref-$transport.txt") lines, $(grep -c ' answer-200 ' "$OUT/trace-ref-$transport.txt") answers 200, S3 S4 S5 ok"
    drive "cand-$transport" "$SCRATCH/target/release/examples/model_node" $transport "$only"
    binds_ok "cand-$transport" || diffs=1
    transport_ok "cand-$transport" $transport || diffs=1
    if cmp -s "$OUT/trace-ref-$transport.txt" "$OUT/trace-cand-$transport.txt"; then
        echo "PASS ($transport): $(wc -l < "$OUT/trace-ref-$transport.txt") lines equal"
    else
        diffs=1
        echo "FAIL ($transport): first difference: $(diff "$OUT/trace-ref-$transport.txt" "$OUT/trace-cand-$transport.txt" | grep -m1 '^[<>]')"
        diff "$OUT/trace-ref-$transport.txt" "$OUT/trace-cand-$transport.txt" | grep -c '^>' | sed 's/^/candidate lines that differ: /'
    fi
done
exit $diffs
