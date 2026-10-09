//! ST.14, parts (2) and (3) on M5's own operation stream: the 107-instance
//! SysML build, 4,240 operations in 107 batches, built in process.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/ST.14.md`, section 3, test M1.
//!
//! The stream (`tests/st14/m5-107.ops.jsonl`, sha256 `8b6deb63...806e`) is the
//! one M5 and M5c posted, written by the vault's
//! `experiments/m5c-node-cost/stream.gen.ts` from M5's own plan and encoder.
//! The descriptor is `tests/fixtures/sysml.ar02.metamodel.json`, byte for
//! byte the `demo/sysml.json` M5 served (sha256 `ae01acb7...daea`). The served
//! state the build must end on (`tests/st14/m5-107.served.json`, 38,133 bytes)
//! is the body M5's own node served over HTTP after the same operations
//! (`2d61802`, debug image), which `f14f969` reproduces in process byte for
//! byte (spec section 5).
//!
//! The writer `a` sends every operation of a batch, then the receiver `b`
//! takes the whole batch, as a peer takes one `Batch` frame. Measured at the
//! writer only: the receiver's causal stability advances on every delivery
//! and its stabilize walk, which is linear in the model and out of this
//! item's scope, allocates on every delivery.
//!
//! Run in release: at `f14f969` the build replays every list on every
//! operation's path three times and takes about ten minutes. The registered
//! command runs it with `st14_sequence_order_cache.rs`:
//!
//! ```bash
//! nice -n 10 cargo test -j 2 --offline --release -p moirai-interp \
//!     --test st14_sequence_order_cache --test st14_m5_build --no-fail-fast -- --nocapture
//! ```

mod st14;

use std::time::Instant;

use st14::*;

use moirai_interp::testing::install;
use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::replica::IsReplica;
use serde::Deserialize;
use serde_json::Value;

/// The windows the writer's means are compared over, by the number of
/// operations already in the model's history when the operation is written.
const EARLY: std::ops::Range<usize> = 400..1_200;
const LATE: std::ops::Range<usize> = 3_440..4_240;
/// The registered bound on the late window's mean allocations per operation
/// over the early window's.
const FLAT: f64 = 1.5;
/// The resource check's bound on the late window's mean wall-clock time per
/// operation at the writer, in a release build. Not a performance claim.
const LATE_MS_PER_OP: f64 = 20.0;

#[derive(Deserialize)]
struct Batch {
    ops: Vec<ModelOp>,
}

fn stream() -> Vec<Vec<ModelOp>> {
    let text = std::fs::read_to_string(fixture_path("m5-107.ops.jsonl"))
        .expect("the M5 stream is in tests/st14");
    text.lines()
        .map(|line| {
            serde_json::from_str::<Batch>(line)
                .expect("one batch per line")
                .ops
        })
        .collect()
}

fn sysml() -> Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("sysml.ar02.metamodel.json");
    serde_json::from_str(&std::fs::read_to_string(path).expect("the SysML descriptor"))
        .expect("the descriptor is JSON")
}

#[test]
fn st14_m1_the_m5_build_stays_flat_at_the_writer_and_serves_what_m5_served() {
    let batches = stream();
    let mut failures = Vec::new();
    let total: usize = batches.iter().map(Vec::len).sum();
    if batches.len() != 107 || total != 4_240 {
        failures.push(format!(
            "set-up: the stream holds {} batches and {total} operations, not 107 and 4,240",
            batches.len()
        ));
    }

    let (mut a, mut b) = moirai_crdt::utils::membership::twins_log::<ModelLog>();
    let opening = send(&mut a, install("m5c", &sysml()));
    b.receive(opening);

    // Per operation: history before it, allocations and nanoseconds at the writer.
    let mut per_op: Vec<(usize, u64, u128)> = Vec::with_capacity(total);
    let started = Instant::now();
    let mut history = 0usize;
    for (index, ops) in batches.into_iter().enumerate() {
        let mut events = Vec::with_capacity(ops.len());
        for op in ops {
            let clock = Instant::now();
            let (event, allocations) = counted(|| send(&mut a, op));
            per_op.push((history, allocations, clock.elapsed().as_nanos()));
            events.push(event);
            history += 1;
        }
        for event in events {
            b.receive(event);
        }
        if index % 10 == 0 {
            eprintln!(
                "st14_m1 batch {index}, history {history}, {:.1} s",
                started.elapsed().as_secs_f64()
            );
        }
    }

    // Part (3): the bytes GET /api/state serves, on the writer and the receiver.
    let expected = std::fs::read_to_string(fixture_path("m5-107.served.json"))
        .expect("M5's served body is in tests/st14");
    for (who, replica) in [("a", &a), ("b", &b)] {
        let got = served(replica);
        if got != expected {
            let path = observed_path(&format!("m5-107.served.{who}.json"));
            std::fs::write(&path, &got).expect("write the observed state");
            failures.push(format!(
                "{who} serves {} bytes after the build, not the {} bytes M5's node served for \
                 the same operations; what it serves is in {}",
                got.len(),
                expected.len(),
                path.display()
            ));
        }
    }
    if a.state().unresolved() != 0 || b.state().unresolved() != 0 {
        failures.push(format!(
            "an operation did not route: unresolved {} at a, {} at b",
            a.state().unresolved(),
            b.state().unresolved()
        ));
    }

    // Part (2): the writer's allocations per operation do not grow with history.
    let window = |range: &std::ops::Range<usize>| -> (Vec<u64>, Vec<u128>) {
        per_op
            .iter()
            .filter(|(history, _, _)| range.contains(history))
            .map(|(_, allocations, nanos)| (*allocations, *nanos))
            .unzip()
    };
    let (early, early_ns) = window(&EARLY);
    let (late, late_ns) = window(&LATE);
    let early_mean = mean(&early);
    let late_mean = mean(&late);
    let ratio = late_mean / early_mean;
    let ms = |nanos: &[u128]| nanos.iter().sum::<u128>() as f64 / nanos.len().max(1) as f64 / 1e6;
    let (early_ms, late_ms) = (ms(&early_ns), ms(&late_ns));
    eprintln!(
        "st14_m1 writer: mean allocations per operation {early_mean:.1} at history {EARLY:?} \
         ({} operations), {late_mean:.1} at {LATE:?} ({} operations), ratio {ratio:.3} (bound \
         {FLAT}); resource check: {early_ms:.3} and {late_ms:.3} ms per operation (bound \
         {LATE_MS_PER_OP} ms late, release builds only); build {:.1} s",
        early.len(),
        late.len(),
        started.elapsed().as_secs_f64()
    );
    if ratio > FLAT {
        failures.push(format!(
            "the writer's mean allocations per operation at history {LATE:?} ({late_mean:.1}) \
             are {ratio:.2} times those at {EARLY:?} ({early_mean:.1}), over the bound {FLAT}: \
             the cost of an operation grows with the model's history"
        ));
    }
    if !cfg!(debug_assertions) && late_ms > LATE_MS_PER_OP {
        failures.push(format!(
            "resource check: the writer took {late_ms:.3} ms per operation at history {LATE:?}, \
             over {LATE_MS_PER_OP} ms"
        ));
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
