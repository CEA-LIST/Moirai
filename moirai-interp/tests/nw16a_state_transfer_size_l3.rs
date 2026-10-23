//! NW.16a, C1's measurement (vault `spec/items/NW.16a.md`): the size on the
//! wire of the largest state transfer `model_node` sends at this cut, under
//! Zenoh on loopback. A record, not a pass gate: it backs the value of
//! `wait_before_close` the spec registers, and is ignored by default.
//!
//! A donor refuses a state transfer whose exported log passes 768 KiB
//! (`MAX_STATE_TRANSFER_BYTES`, `generic.rs`); the frame it sends is that log
//! deflated and in base64 when that is shorter (`LogPayload::encode`), plus a
//! snapshot whose suffix, the events above the donor's stable frontier, is
//! not counted against the ceiling. So three cases are measured:
//!
//! - `acked`: the log filled to just under the ceiling, by stereotypes of
//!   2000 letters each, while the other member writes one operation per
//!   ten, so the stable frontier keeps up and the suffix stays short;
//! - `silent`: the same fill while the other member never writes, so
//!   nothing becomes stable and the suffix carries every event.
//!
//! Each prints `NW16A C1 <case>: ...` with the donor's reported log bytes and
//! the bytes of every frame a raw observer saw put to the joiner.
//!
//! ```text
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test nw16a_state_transfer_size_l3 -- --ignored --test-threads=1 --nocapture
//! ```
#![cfg(feature = "network")]

mod nw16a;

use std::time::{Duration, Instant};

use nw16a::{Cluster, Observer, Transport, si01};

/// `MAX_STATE_TRANSFER_BYTES` at d996427 and d28b6d7.
const CEILING: usize = 768 * 1024;

/// The exported log bytes a donor reported serving to `joiner`, read from
/// its own log line.
fn served_bytes(cluster: &Cluster, donors: &[usize], joiner: &str, model: &str) -> Option<usize> {
    donors.iter().find_map(|&d| {
        cluster.nodes[d].log_text().lines().find_map(|line| {
            let tail = line
                .split(&format!(
                    "serving a state transfer to {joiner} for log {model}: "
                ))
                .nth(1)?;
            tail.split(' ').next()?.parse().ok()
        })
    })
}

/// The length of every stereotype `a` adds.
const VALUE_LEN: usize = 2000;

/// `VALUE_LEN` letters from a small linear congruential generator seeded by
/// `seed`: text deflate shrinks by about a third, not the run of one letter
/// it would fold away.
fn value(seed: usize) -> String {
    let mut state = (seed as u64)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1);
    (0..VALUE_LEN)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (b'a' + ((state >> 33) % 26) as u8) as char
        })
        .collect()
}

/// Add `count` stereotypes of `VALUE_LEN` letters to `a`'s class `x`, at
/// the head of `Model.ownedElements`, numbered from `from`; every `every`
/// operations the node at `acker`, if any, inserts one class `k` after it,
/// at position 1, so `x` stays at the head and `a` hears from it.
fn fill(
    cluster: &Cluster,
    model: &str,
    from: usize,
    count: usize,
    acker: Option<(usize, usize)>,
    acked: &mut usize,
) {
    let uml = si01::Uml::new();
    for i in 0..count {
        cluster
            .submit(0, model, &uml.stereotype(0, &value(from + i)))
            .unwrap_or_else(|e| panic!("fill: {e}"));
        if let Some((node, every)) = acker
            && (i + 1) % every == 0
        {
            for op in uml.named_class(1, "k") {
                cluster
                    .submit(node, model, &op)
                    .unwrap_or_else(|e| panic!("ack: {e}"));
            }
            *acked += 1;
        }
    }
}

/// Start `joiner`, told the donors, join the model, and report what it got.
fn measure(cluster: &mut Cluster, case: &str, joiner: &str, donors: &[usize], model: &str) {
    let ports: Vec<u16> = donors.iter().map(|&d| cluster.nodes[d].listen).collect();
    let observer = Observer::start(&ports, &format!("moirai/*/to/{joiner}/*"))
        .unwrap_or_else(|e| panic!("{case}: {e}"));
    let index = cluster
        .add(joiner, donors)
        .unwrap_or_else(|e| panic!("{case}: {e}"));
    let started = Instant::now();
    cluster
        .join_model(index, model)
        .unwrap_or_else(|e| panic!("{case}: {e}"));
    cluster
        .await_table(index, model)
        .unwrap_or_else(|e| panic!("{case}: {e}"));
    // Let the delta sync after adoption, or the catch-up, finish.
    std::thread::sleep(Duration::from_secs(5));
    let served = served_bytes(cluster, donors, joiner, model);
    let frames = observer.frames();
    let largest = frames.iter().map(|f| (f.bytes, f.kind.clone())).max();
    let state_responses: Vec<usize> = frames
        .iter()
        .filter(|f| f.kind == "StateResponse")
        .map(|f| f.bytes)
        .collect();
    let batches: Vec<usize> = frames
        .iter()
        .filter(|f| f.kind == "Batch")
        .map(|f| f.bytes)
        .collect();
    println!(
        "NW16A C1 {case}: donor reported {served:?} exported log bytes (ceiling {CEILING}); \
         StateResponse frames {state_responses:?} bytes; Batch frames {batches:?} bytes; \
         largest frame {largest:?}; joiner adopted: {}; {} frames to {joiner} in {:?}",
        cluster.nodes[index].adopted(model),
        frames.len(),
        started.elapsed()
    );
}

/// Exported log bytes per stereotype, from a joiner after `count`, on a
/// cluster of its own: the joiner becomes a member that never writes, which
/// would hold the stable frontier of the cluster it joined.
fn calibrate(count: usize) -> f64 {
    let (mut cluster, model) = base("c1c");
    let mut acked = 0;
    fill(&cluster, &model, 0, count, Some((1, 10)), &mut acked);
    let index = cluster.add("j0", &[0, 1]).expect("calibration joiner");
    cluster.join_model(index, &model).expect("calibration join");
    cluster
        .await_table(index, &model)
        .expect("calibration table");
    let served = served_bytes(&cluster, &[0, 1], "j0", &model).expect("calibration served");
    println!("NW16A C1 calibration: {served} exported log bytes after {count} stereotypes");
    served as f64 / count as f64
}

fn base(tag: &str) -> (Cluster, String) {
    let cluster = Cluster::start(Transport::Zenoh, tag, &["a", "b"]).expect("cluster");
    let model = cluster.create_model(0).expect("model");
    cluster.join_model(1, &model).expect("join b");
    cluster.await_table(1, &model).expect("table b");
    cluster.write_class(0, &model, "x").expect("class x");
    cluster
        .await_names(1, &model, &nw16a::set(&["x"]))
        .expect("class x at b");
    (cluster, model)
}

#[test]
#[ignore = "NW.16a C1 measurement, a record: run with --ignored"]
fn c1_the_largest_state_transfer_under_zenoh() {
    // acked: b inserts a class every ten stereotypes, so a's stable
    // frontier keeps up.
    // The counts may be given (`NW16A_C1_ACKED`, `NW16A_C1_SILENT`) once a
    // first run has shown where the ceiling falls; by default a calibration
    // run estimates them.
    let count = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
    };
    let target = match count("NW16A_C1_ACKED") {
        Some(n) => n,
        None => (CEILING as f64 * 0.97 / calibrate(40)) as usize,
    };
    let silent_target = count("NW16A_C1_SILENT").unwrap_or(target);
    println!("NW16A C1: filling to {target} stereotypes acked, {silent_target} silent");
    let (mut cluster, model) = base("c1a");
    let mut acked = 0;
    fill(&cluster, &model, 0, target, Some((1, 10)), &mut acked);
    measure(&mut cluster, "acked", "d", &[0, 1], &model);
    drop(cluster);

    // silent: the same stereotypes, b never writes, nothing becomes stable.
    let (mut cluster, model) = base("c1s");
    let mut none = 0;
    fill(&cluster, &model, 0, silent_target, None, &mut none);
    measure(&mut cluster, "silent", "e", &[0, 1], &model);
}
