//! ST.01 over the wire: O4 of `cea-cdrt-knowledge/spec/items/ST.01.md`.
//!
//! Eight `model_node` processes on loopback. `alice` and `bob` host six
//! models of `st01.ecore`, one per place of the claim (the flat optional, the
//! inner slot unset, the outer optional unset) and per member W reaches (the
//! `A` alice created, the `B` bob created). Partitioned (`/api/pause-all`),
//! alice writes an `A` and bob a `B`; healed, each model holds the conflict.
//! Partitioned again, alice unsets the place (U) and bob increments the
//! member's `n` by 10 (W); healed, alice has delivered U then W and bob W
//! then U. Each node is read once it has delivered all six operations of the
//! model (its `Install` among them), so a difference then is a divergence
//! and not a lag. Both must serve one default read and one identified read,
//! V1's document, under the identities of C3. Then one joiner per model
//! (`carol` and five more) registers it, adopts a donor's state (its log says
//! so), and serves the same. The joiner then makes O2's later writes: the
//! reached member's class +1, after which all three serve it holding 11 under
//! its identity; then the place removed and the emptied member's class +5,
//! after which all three serve that class alone, under an identity nothing
//! carried before.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st01_optional_conflict_race_l3 -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: the test prints `E2E-SKIP st01-l3: <why>` and returns. A skip
//! is not a pass (prediction P4 counts them).
#![cfg(feature = "network")]

#[macro_use]
mod si01;
#[path = "st01/cluster.rs"]
mod cluster;
mod st01;

use std::collections::BTreeSet;
use std::path::PathBuf;

use cluster::{Cluster, digest, node_binary};
use si01::{OPAQUE, all_ids, strip};
use st01::*;

const ALICE: usize = 0;
const BOB: usize = 1;
const JOINERS: [&str; 6] = ["carol", "dave", "erin", "frank", "grace", "heidi"];

/// One model of the run: its place, the member W reaches, its id, and the
/// joiner that registers it last.
struct Case {
    place: Place,
    member: &'static str,
    model: String,
    joiner: usize,
}

impl Case {
    fn schedule(&self) -> Schedule {
        Schedule {
            place: self.place,
            xy: (ALICE, BOB),
            x_first: true,
            uw: (ALICE, BOB),
            u_first: true,
            member: self.member,
        }
    }

    fn label(&self) -> String {
        format!(
            "{:?}, W reaches the {} (model {})",
            self.place, self.member, self.model
        )
    }
}

#[test]
fn st01_l3_two_nodes_partitioned_and_a_joiner_serve_the_reached_member_alone() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP st01-l3: {why}");
            return;
        }
    };
    match l3(&binary) {
        Ok(failures) if failures.is_empty() => {}
        Ok(failures) => panic!(
            "ST.01 O4: {} of the checks over the wire fail:\n  {}",
            failures.len(),
            failures.join("\n  ")
        ),
        Err(why) => panic!("st01-l3: {why}"),
    }
}

/// The run; `Err` for a harness failure (a node that never answers, a
/// refused operation), `Ok` with the claim's failures, one line each.
fn l3(binary: &PathBuf) -> Result<Vec<String>, String> {
    let mut ids = vec!["alice", "bob"];
    ids.extend(JOINERS);
    let cluster = Cluster::start(binary, &ids, &[("st01", ST01)])?;
    let key = digest(ST01);
    let tray = Tray::new();
    let mut failures = Vec::new();

    let mut cases = Vec::new();
    for place in Place::OPTIONAL {
        for member in ["A", "B"] {
            let model = cluster.register(ALICE, None, &key)?;
            cluster.register(BOB, Some(&model), &key)?;
            cases.push(Case {
                place,
                member,
                model,
                joiner: 2 + cases.len(),
            });
        }
    }
    for case in &cases {
        cluster.until_delivered(BOB, &case.model, 1)?;
        cluster.submit(ALICE, &case.model, &tray.tick())?;
    }
    for case in &cases {
        for node in [ALICE, BOB] {
            cluster.until_delivered(node, &case.model, 2)?;
        }
    }

    // ---- partition 1: an `A` on alice, a `B` on bob ----
    cluster.pause_all(ALICE)?;
    cluster.pause_all(BOB)?;
    for case in &cases {
        cluster.submit(ALICE, &case.model, &tray.write(case.place, "A", 1))?;
        cluster.submit(BOB, &case.model, &tray.write(case.place, "B", 2))?;
    }
    cluster.resume_all(ALICE)?;
    cluster.resume_all(BOB)?;
    let mut formed = Vec::new();
    for case in &cases {
        for node in [ALICE, BOB] {
            cluster.until_delivered(node, &case.model, 4)?;
        }
        let on_alice = cluster.identified(ALICE, &case.model)?;
        let on_bob = cluster.identified(BOB, &case.model)?;
        if on_alice != on_bob {
            return Err(format!(
                "set-up: {}: alice and bob part once the conflict formed: {on_alice} | {on_bob}",
                case.label()
            ));
        }
        formed.push(Formed::read(case.place, &on_alice, &case.label()));
    }

    // ---- partition 2: U on alice, W on bob ----
    cluster.pause_all(ALICE)?;
    cluster.pause_all(BOB)?;
    for case in &cases {
        cluster.submit(ALICE, &case.model, &tray.removal(case.place))?;
        cluster.submit(BOB, &case.model, &tray.write(case.place, case.member, 10))?;
    }
    cluster.resume_all(ALICE)?;
    cluster.resume_all(BOB)?;
    for (case, formed) in cases.iter().zip(&formed) {
        for node in [ALICE, BOB] {
            cluster.until_delivered(node, &case.model, 6)?;
        }
        let schedule = case.schedule();
        let expected = v1(&schedule);
        let mut docs = Vec::new();
        for node in [ALICE, BOB] {
            let who = format!("{}: {}", case.label(), cluster.nodes[node].id);
            let doc = cluster.identified(node, &case.model)?;
            let plain = cluster.default_state(node, &case.model)?;
            if strip(&doc, OPAQUE) != plain {
                failures.push(format!(
                    "{who}: the default read {plain} is not the identified read stripped {doc}"
                ));
            }
            if plain != expected {
                failures.push(format!("{who}: serves {plain}, not V1's {expected}"));
            }
            if let Err(why) = v1_identities(&schedule, formed, &doc, &who) {
                failures.push(why);
            }
            docs.push(doc);
        }
        if docs[0] != docs[1] {
            failures.push(format!(
                "{}: alice and bob, both having delivered every operation, part: {} | {}",
                case.label(),
                docs[0],
                docs[1]
            ));
        }
    }

    // ---- the joiners, by state transfer ----
    for case in &cases {
        cluster.register(case.joiner, Some(&case.model), &key)?;
    }
    for (case, formed) in cases.iter().zip(&formed) {
        let joiner = &cluster.nodes[case.joiner];
        let who = format!("{}: {}", case.label(), joiner.id);
        cluster.until_delivered(case.joiner, &case.model, 6)?;
        if !joiner.adopted(&case.model) {
            failures.push(format!(
                "{who}: did not join by state transfer; its log:\n{}",
                joiner.tail()
            ));
        }
        let schedule = case.schedule();
        let doc = cluster.identified(case.joiner, &case.model)?;
        let plain = cluster.default_state(case.joiner, &case.model)?;
        let on_alice = cluster.identified(ALICE, &case.model)?;
        if doc != on_alice {
            failures.push(format!("{who}: serves {doc} where alice serves {on_alice}"));
        }
        if plain != v1(&schedule) || strip(&doc, OPAQUE) != plain {
            failures.push(format!("{who}: serves {plain}, identified {doc}"));
        }
        if let Err(why) = v1_identities(&schedule, formed, &doc, &who) {
            failures.push(why);
        }
    }

    // ---- each joiner writes: the reached class +1, then the place removed
    // and the emptied class +5 (O2's later writes) ----
    let mut seen: Vec<BTreeSet<String>> = Vec::new();
    for (case, formed) in cases.iter().zip(&formed) {
        let mut ids = formed.all();
        for node in [ALICE, BOB, case.joiner] {
            ids.extend(all_ids(&cluster.identified(node, &case.model)?));
        }
        cluster.submit(
            case.joiner,
            &case.model,
            &reached_write(&tray, &case.schedule()),
        )?;
        seen.push(ids);
    }
    for (case, formed) in cases.iter().zip(&formed) {
        let reads = settled(&cluster, case, 7)?;
        for line in check_reached_write(&case.schedule(), formed, &reads) {
            failures.push(format!("{}, after the joiner's +1: {line}", case.label()));
        }
    }
    for (case, seen) in cases.iter().zip(seen.iter_mut()) {
        for node in [ALICE, BOB, case.joiner] {
            seen.extend(all_ids(&cluster.identified(node, &case.model)?));
        }
        for op in written_again(&tray, &case.schedule()) {
            cluster.submit(case.joiner, &case.model, &op)?;
        }
    }
    for ((case, formed), seen) in cases.iter().zip(&formed).zip(&seen) {
        let reads = settled(&cluster, case, 9)?;
        for line in check_written_again(&case.schedule(), formed, seen, &reads) {
            failures.push(format!(
                "{}, after the joiner wrote the emptied class again: {line}",
                case.label()
            ));
        }
    }
    for line in &failures {
        println!("ST01-O4 {line}");
    }
    println!(
        "ST01-O4 models {}: failing checks {}",
        cases.len(),
        failures.len()
    );
    Ok(failures)
}

/// Both reads of alice, bob and the case's joiner, once each has delivered
/// `count` operations of the model.
fn settled(cluster: &Cluster, case: &Case, count: u64) -> Result<Vec<Reads>, String> {
    let mut reads = Vec::new();
    for node in [ALICE, BOB, case.joiner] {
        cluster.until_delivered(node, &case.model, count)?;
        reads.push(Reads {
            who: cluster.nodes[node].id.clone(),
            identified: cluster.identified(node, &case.model)?,
            default: cluster.default_state(node, &case.model)?,
        });
    }
    Ok(reads)
}
