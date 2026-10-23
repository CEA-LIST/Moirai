//! NW.17, claim point 3b (vault `spec/items/NW.17.md`): once
//! akira/zenoh-transport is merged into akira/moirai-modelstore, a state
//! transfer whose message is above 1 MiB crosses Zenoh whole between two
//! `model_node` processes and is adopted, and nothing either donor puts on
//! the wire is a relay chunk. The TCP twin runs the same scenario on the
//! default transport.
//!
//! Why. Modelstore (SI.13 D2) lifted the ceiling on one state transfer from
//! 768 KiB of log to 128 MiB, and made a relay session carry a message above
//! the relay's 1 MiB frame bound as `TransportMessage::Chunk` frames that
//! the receiving session joins before the node sees them; a direct TCP link
//! reads a frame of any length. Zenoh fragments a large message itself, so
//! the Zenoh path needs no chunk and must not send one. A merge that keeps
//! both sides keeps the ceiling from modelstore and the Zenoh path from
//! zenoh-transport: at 3855e4a the node ignores `MOIRAI_TRANSPORT` and runs
//! TCP, at 9d1485f the donor refuses any log above 768 KiB. SI.13's scale
//! suites cannot show this under Zenoh (a relay and a bootnode, and
//! ignored), nor SI.12's (ignored, and below 5,000 objects no message
//! reaches 1 MiB on the wire).
//!
//! The scenario, on the transport each test names:
//!
//! 1. `a` and `b` start by NW.02's launcher through `nw16a::Cluster` (on
//!    Zenoh each is witnessed by its liveliness token at the port it
//!    reported); on Zenoh a raw observer dials both and records every frame
//!    they put on `moirai/**`.
//! 2. `a` creates a SimpleUML model `m`, `b` joins it, `a` writes class `x`
//!    and then adds [`COUNT`] stereotypes of [`LEN`] pseudo-random letters
//!    to `x`, which `b` receives. Each stereotype is in the log, so the log
//!    holds at least `COUNT * LEN` = 2,000,000 bytes of letters, which
//!    deflate shrinks by about two fifths and base64 grows by a third: the
//!    state transfer's message is above 1 MiB however the donor encodes it.
//! 3. `j` starts, told `a` and `b`, and joins `m` by its id; holding no
//!    history of `m`, it asks a peer for a state transfer.
//!
//! The claim's checks, collected (each that fails is kept and the test
//! fails at its end with all of them):
//!
//! - B0 (Zenoh): every node started with `MOIRAI_TRANSPORT=zenoh` shows its
//!   token at the port it reported (`NW16A WITNESS` from the cluster);
//!   failing it is a claim failure, the node does not run Zenoh.
//! - B1: `j`'s log says it adopted a transferred state for `m`
//!   (`GenericNode::adopt_state`'s line, read as ST.01's harness reads it).
//! - B2: within [`BOUND`], `j` serves `m` with class `x` holding exactly
//!   the `COUNT` stereotypes written, and the very document `a` serves.
//! - B3 (Zenoh): the observer saw at least one `StateResponse` of more than
//!   1 MiB put to `j` on a key `moirai/<session>/to/j/<donor>`, as one
//!   sample, and no frame of type `Chunk` on any key.
//!
//! A message starting `set-up:` is a failure of the instrument or of the
//! host (a process, the model, a write a member must accept, `b` never
//! receiving the stereotypes): the point inconclusive, never a pass.
//! Records, printed as `NW17 RECORD`: the donors' own state-transfer lines,
//! every `StateResponse`'s size and key, the largest frame, the frames
//! seen, the time `j` took.
//!
//! ```text
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network --no-fail-fast \
//!     --test nw17_large_message_l3 -- --test-threads=1 --nocapture
//! ```
//!
//! The binary is `MOIRAI_E2E_INTERP_NODE_BIN` or
//! `target/debug/examples/model_node` (`nw16a::node_binary`).
#![cfg(feature = "network")]

mod nw16a;

use std::collections::BTreeSet;
use std::thread;
use std::time::{Duration, Instant};

use nw16a::{Cluster, Observer, Transport, si01};
use serde_json::Value;

/// Stereotypes `a` adds to `x`.
const COUNT: usize = 100;
/// Letters in each stereotype.
const LEN: usize = 20_000;
/// The relay's frame bound, `MAX_FRAME_BYTES` in `moirai-relay`: 1 MiB.
const MIB: usize = 1024 * 1024;
/// How long `b` may take to hold every stereotype, and `j` to serve `m`
/// after it asked to join: a liveness bound, not a timing claim.
const BOUND: Duration = Duration::from_secs(180);
const POLL: Duration = Duration::from_millis(500);

// Premise: the log holds at least 1.5 MiB of letters.
const _: () = assert!(COUNT * LEN >= MIB + MIB / 2);

/// `LEN` letters from a linear congruential generator seeded by `seed`:
/// text that deflate shrinks by about two fifths, not the run of one letter
/// it would fold away. Distinct seeds give distinct texts.
fn value(seed: usize) -> String {
    let mut state = (seed as u64 + 0x5117)
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1);
    (0..LEN)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (b'a' + ((state >> 33) % 26) as u8) as char
        })
        .collect()
}

/// The stereotypes of the one class named `x` in a served SimpleUML
/// document; `None` when there is no such class, or more than one.
fn stereotypes_of_x(doc: &Value) -> Option<BTreeSet<String>> {
    let owned = doc["ownedElements"].as_array()?;
    let hits: Vec<&Value> = owned
        .iter()
        .filter(|element| si01::name_of(element) == "x")
        .collect();
    if hits.len() != 1 {
        return None;
    }
    hits[0]["stereotype"].as_array().map(|items| {
        items
            .iter()
            .filter_map(|s| s.as_str().map(str::to_string))
            .collect()
    })
}

/// The document `node` serves for `model`.
fn served(cluster: &Cluster, node: usize, model: &str) -> Result<Value, String> {
    match cluster.get(node, &format!("/api/model/{model}/state"))? {
        (200, doc) => Ok(doc),
        (status, body) => Err(format!(
            "{} answered /api/model/{model}/state with {status}: {}",
            cluster.nodes[node].id,
            short(&body.to_string())
        )),
    }
}

fn short(text: &str) -> String {
    if text.len() <= 300 {
        text.to_string()
    } else {
        format!("{}... ({} bytes)", &text[..300], text.len())
    }
}

/// A cluster error read as the claim's (a node on Zenoh that shows no
/// token, B0) or the instrument's (anything else, `set-up:`).
fn started(transport: Transport, result: Result<Cluster, String>) -> Cluster {
    result.unwrap_or_else(|e| panic!("{}", classify(transport, &e)))
}

fn classify(transport: Transport, error: &str) -> String {
    if transport == Transport::Zenoh && error.contains("NW16A WITNESS") {
        format!("B0: a node started with MOIRAI_TRANSPORT=zenoh does not run Zenoh: {error}")
    } else {
        format!("set-up: {error}")
    }
}

/// The lines in which `a` or `b` reports serving, or refusing, a state
/// transfer to `j`.
fn donor_lines(cluster: &Cluster, joiner: &str) -> Vec<String> {
    cluster.nodes[..2]
        .iter()
        .flat_map(|node| {
            node.log_text()
                .lines()
                .filter(|line| {
                    line.contains("state transfer")
                        && (line.contains(&format!(" to {joiner} "))
                            || line.contains(&format!(" to {joiner}:")))
                })
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
}

fn scenario(transport: Transport) {
    let tag = format!("nw17-b-{}", transport.var());
    let mut cluster = started(transport, Cluster::start(transport, &tag, &["a", "b"]));
    let observer = (transport == Transport::Zenoh).then(|| {
        let ports = [cluster.nodes[0].listen, cluster.nodes[1].listen];
        Observer::start(&ports, "moirai/**").unwrap_or_else(|e| panic!("set-up: observer: {e}"))
    });
    let setup = |what: &str, result: Result<(), String>| {
        result.unwrap_or_else(|e| panic!("set-up: {what}: {e}"));
    };

    let model = cluster
        .create_model(0)
        .unwrap_or_else(|e| panic!("set-up: create: {e}"));
    setup("b joins", cluster.join_model(1, &model));
    setup("b's table", cluster.await_table(1, &model));
    setup("class x", cluster.write_class(0, &model, "x"));
    setup(
        "x at b",
        cluster.await_names(1, &model, &nw16a::set(&["x"])),
    );

    let expected: BTreeSet<String> = (0..COUNT).map(value).collect();
    assert_eq!(
        expected.len(),
        COUNT,
        "premise: the stereotypes are distinct"
    );
    let uml = si01::Uml::new();
    let wrote = Instant::now();
    for i in 0..COUNT {
        setup(
            &format!("stereotype {i} on a"),
            cluster.submit(0, &model, &uml.stereotype(0, &value(i))),
        );
    }
    let deadline = Instant::now() + BOUND;
    loop {
        let held = served(&cluster, 1, &model)
            .ok()
            .and_then(|doc| stereotypes_of_x(&doc));
        if held.as_ref() == Some(&expected) {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "set-up: b never held the {COUNT} stereotypes within {BOUND:?} (holds {:?}):\n{}",
                held.map(|h| h.len()),
                cluster.nodes[1].tail()
            );
        }
        thread::sleep(POLL);
    }
    let wrote = wrote.elapsed();

    // B0 for the joiner: `add` witnesses it on Zenoh.
    let joiner = cluster
        .add("j", &[0, 1])
        .unwrap_or_else(|e| panic!("{}", classify(transport, &e)));
    let asked = Instant::now();
    setup("j joins m by its id", cluster.join_model(joiner, &model));

    let mut failures: Vec<String> = Vec::new();

    // B2: j serves m, x holding every stereotype, and a's very document.
    let deadline = Instant::now() + BOUND;
    let mut last = String::new();
    let converged = loop {
        match (
            served(&cluster, joiner, &model),
            served(&cluster, 0, &model),
        ) {
            (Ok(on_j), Ok(on_a)) => {
                let held = stereotypes_of_x(&on_j);
                if held.as_ref() == Some(&expected) && on_j == on_a {
                    break Some(asked.elapsed());
                }
                last = format!(
                    "j holds {:?} of the {COUNT} stereotypes on x; j's document {} a's",
                    held.map(|h| h.intersection(&expected).count()),
                    if on_j == on_a {
                        "equals"
                    } else {
                        "differs from"
                    }
                );
            }
            (j, a) => last = format!("j: {:?}; a: {:?}", j.err(), a.err()),
        }
        if Instant::now() > deadline {
            break None;
        }
        thread::sleep(POLL);
    };
    if converged.is_none() {
        failures.push(format!(
            "B2: within {BOUND:?} of joining, j never served m with x holding the {COUNT} \
             stereotypes a wrote and a's document ({last}); j's last log lines:\n{}",
            cluster.nodes[joiner].tail()
        ));
    }

    // B1: j adopted a transferred state for m.
    let adopted = cluster.nodes[joiner].adopted(&model);
    if !adopted {
        failures.push(format!(
            "B1: j never adopted a state transfer for {model}; the donors said {:?}; j's last \
             log lines:\n{}",
            donor_lines(&cluster, "j"),
            cluster.nodes[joiner].tail()
        ));
    }

    // B3: on the wire, one StateResponse above 1 MiB to j, and no chunk.
    let mut record = String::new();
    if let Some(observer) = &observer {
        thread::sleep(Duration::from_secs(1));
        let frames = observer.frames();
        let to_j: Vec<(String, usize)> = frames
            .iter()
            .filter(|f| f.kind == "StateResponse" && f.key.contains("/to/j/"))
            .map(|f| (f.key.clone(), f.bytes))
            .collect();
        let chunks: Vec<(String, usize)> = frames
            .iter()
            .filter(|f| f.kind == "Chunk")
            .map(|f| (f.key.clone(), f.bytes))
            .collect();
        let largest = frames
            .iter()
            .max_by_key(|f| f.bytes)
            .map(|f| (f.bytes, f.kind.clone(), f.key.clone()));
        if !to_j.iter().any(|(_, bytes)| *bytes > MIB) {
            failures.push(format!(
                "B3: no StateResponse of more than {MIB} bytes was put to j on Zenoh: \
                 StateResponse frames to j {to_j:?}; largest frame seen {largest:?}"
            ));
        }
        if !chunks.is_empty() {
            failures.push(format!(
                "B3: a donor put {} frame(s) of type Chunk on Zenoh: {chunks:?}",
                chunks.len()
            ));
        }
        record = format!(
            " StateResponse frames to j {to_j:?}; Chunk frames {}; largest frame {largest:?}; \
             {} frames seen;",
            chunks.len(),
            frames.len()
        );
    }
    println!(
        "NW17 RECORD {}: {COUNT} stereotypes of {LEN} letters written and held by b in \
         {wrote:?}; j converged after {converged:?}; j adopted: {adopted};{record} donors said \
         {:?}",
        transport.var(),
        donor_lines(&cluster, "j")
    );
    drop(observer);
    if !failures.is_empty() {
        panic!(
            "NW.17 3b on {}: {} failing check(s):\n{}",
            transport.var(),
            failures.len(),
            failures.join("\n")
        );
    }
}

#[test]
fn b_a_state_transfer_above_1_mib_crosses_whole_and_is_adopted_zenoh() {
    scenario(Transport::Zenoh);
}

#[test]
fn b_a_state_transfer_above_1_mib_crosses_whole_and_is_adopted_tcp() {
    scenario(Transport::Tcp);
}
