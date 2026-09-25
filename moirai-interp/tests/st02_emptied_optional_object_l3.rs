//! ST.02 over the wire: A4 of `cea-cdrt-knowledge/spec/items/ST.02.md`.
//!
//! Nine `model_node` processes on loopback. `alice` and `bob` host seven
//! models of `st02.ecore`, one per place of the claim and target of W
//! (`Flat` and `Keyed` with W on `Box.k` or on the `Cell`; `Deep` with W on
//! `Box.k`, `Mid.k` or the `Cell`). Partitioned (`/api/pause-all`), alice
//! writes the `Cell` (+1) and bob the owner's `k` (+2); healed, each model
//! holds the owner and its `Cell`. Partitioned again, alice removes
//! (`Holder.inner` unset, or key `k` of `Holder.bag` removed; U)
//! and bob increments W's target by 10 (W); healed, alice has delivered U
//! then W and bob W then U. Each node is read once it has delivered all six
//! operations of the model (its `Install` among them), so a difference then
//! is a divergence and not a lag. Both must serve one default read and one
//! identified read, the rule's document, under C3's identities. Then one
//! joiner per model registers it, adopts a donor's state (its log says so),
//! and serves the same. The joiner then makes A2's later writes: W's target
//! +1, after which all three serve it holding 11 and nothing moved; then,
//! where the race dropped the `Cell`, the `Cell` +5, after which all three
//! serve it under an identity nothing carried before.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st02_emptied_optional_object_l3 -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: the test prints `E2E-SKIP st02-l3: <why>` and returns. A skip
//! is not a pass (the spec's prediction counts them).
#![cfg(feature = "network")]

#[macro_use]
mod si01;
#[path = "st01/cluster.rs"]
mod cluster;
mod st02;

use std::collections::BTreeSet;
use std::path::PathBuf;

use cluster::{Cluster, digest, node_binary};
use si01::{OPAQUE, all_ids, strip};
use st02::*;

const ALICE: usize = 0;
const BOB: usize = 1;
const JOINERS: [&str; 7] = ["carol", "dave", "erin", "frank", "grace", "heidi", "ivan"];

/// One model of the run: its place, W's target, its id, and the joiner that
/// registers it last.
struct Case {
    place: Place,
    target: Target,
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
            target: self.target,
        }
    }

    fn label(&self) -> String {
        format!(
            "{:?}, W on {:?} (model {})",
            self.place, self.target, self.model
        )
    }
}

#[test]
fn st02_l3_two_nodes_partitioned_and_a_joiner_drop_the_emptied_object() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP st02-l3: {why}");
            return;
        }
    };
    match l3(&binary) {
        Ok(failures) if failures.is_empty() => {}
        Ok(failures) => panic!(
            "ST.02 A4: {} of the checks over the wire fail:\n  {}",
            failures.len(),
            failures.join("\n  ")
        ),
        Err(why) => panic!("st02-l3: {why}"),
    }
}

/// The run; `Err` for a harness failure (a node that never answers, a
/// refused operation), `Ok` with the claim's failures, one line each.
fn l3(binary: &PathBuf) -> Result<Vec<String>, String> {
    let mut ids = vec!["alice", "bob"];
    ids.extend(JOINERS);
    let cluster = Cluster::start(binary, &ids, &[("st02", ST02)])?;
    let key = digest(ST02);
    let tray = Tray::new();
    let mut failures = Vec::new();

    let mut cases = Vec::new();
    for place in Place::CLAIM {
        for &target in place.targets() {
            let model = cluster.register(ALICE, None, &key)?;
            cluster.register(BOB, Some(&model), &key)?;
            cases.push(Case {
                place,
                target,
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

    // ---- partition 1: the Cell on alice, the owner's k on bob ----
    cluster.pause_all(ALICE)?;
    cluster.pause_all(BOB)?;
    for case in &cases {
        cluster.submit(
            ALICE,
            &case.model,
            &tray.write(case.place, Target::Object, 1),
        )?;
        cluster.submit(
            BOB,
            &case.model,
            &tray.write(case.place, Target::Sibling, 2),
        )?;
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
                "set-up: {}: alice and bob part once formed: {on_alice} | {on_bob}",
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
        cluster.submit(BOB, &case.model, &tray.write(case.place, case.target, 10))?;
    }
    cluster.resume_all(ALICE)?;
    cluster.resume_all(BOB)?;
    for (case, formed) in cases.iter().zip(&formed) {
        for node in [ALICE, BOB] {
            cluster.until_delivered(node, &case.model, 6)?;
        }
        let schedule = case.schedule();
        let want = expected(&schedule);
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
            if plain != want {
                failures.push(format!("{who}: serves {plain}, not {want}"));
            }
            if let Err(why) = identities(&schedule, formed, &doc, &who) {
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
        if plain != expected(&schedule) || strip(&doc, OPAQUE) != plain {
            failures.push(format!("{who}: serves {plain}, identified {doc}"));
        }
        if let Err(why) = identities(&schedule, formed, &doc, &who) {
            failures.push(why);
        }
    }

    // ---- each joiner writes W's target again (+1) ----
    let mut seen: Vec<BTreeSet<String>> = Vec::new();
    for (case, formed) in cases.iter().zip(&formed) {
        let mut ids = formed.all();
        for node in [ALICE, BOB, case.joiner] {
            ids.extend(all_ids(&cluster.identified(node, &case.model)?));
        }
        cluster.submit(case.joiner, &case.model, &again_op(&tray, &case.schedule()))?;
        seen.push(ids);
    }
    for (case, formed) in cases.iter().zip(&formed) {
        let reads = settled(&cluster, case, 7)?;
        for line in check_again(&case.schedule(), formed, &reads) {
            failures.push(format!("{}, after the joiner's +1: {line}", case.label()));
        }
    }

    // ---- and, where the race dropped the Cell, writes the Cell again ----
    for (case, seen) in cases.iter().zip(seen.iter_mut()) {
        for node in [ALICE, BOB, case.joiner] {
            seen.extend(all_ids(&cluster.identified(node, &case.model)?));
        }
        if let Some(op) = rebuild_op(&tray, &case.schedule()) {
            cluster.submit(case.joiner, &case.model, &op)?;
        }
    }
    for ((case, formed), seen) in cases.iter().zip(&formed).zip(&seen) {
        let schedule = case.schedule();
        if !schedule.drops_cell() {
            continue;
        }
        let reads = settled(&cluster, case, 8)?;
        for line in check_rebuilt(&schedule, formed, seen, &reads) {
            failures.push(format!(
                "{}, after the joiner wrote the Cell again: {line}",
                case.label()
            ));
        }
    }
    for line in &failures {
        println!("ST02-A4 {line}");
    }
    println!(
        "ST02-A4 models {}: failing checks {}",
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
