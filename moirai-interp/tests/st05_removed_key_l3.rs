//! ST.05 over the wire: K5 of `cea-cdrt-knowledge/spec/items/ST.05.md`.
//!
//! Nine `model_node` processes on loopback. `alice` and `bob` host eighteen
//! models of `st05.ecore`: one per place and target of W for the race
//! (eleven), and one per place for the removal alone (seven). Partitioned
//! (`/api/pause-all`), alice makes the formation's first write (X) and bob
//! its second (Y); healed, each model holds the removed key's object.
//! Partitioned again, alice removes the key (U) and, in a race model, bob
//! writes W's target +10 (W); healed, alice has delivered U then W and bob W
//! then U. Each node is read once it has delivered every operation of the
//! model (its `Install` among them), so a difference then is a divergence
//! and not a lag. Both must serve one default read and one identified read,
//! the rule's document, under the identities. Then one joiner per place
//! (`carol` to `ivan`) registers the place's models, adopts a donor's state
//! for each (its log says so), and serves the same. The joiner then makes
//! K3's later writes: in a race model W's target +1, after which all three
//! serve it and nothing moved, then, where the race left out the `Mid` at
//! key `j`, that `Mid` +5, after which all three serve it under an identity
//! nothing carried before; in a removal-alone model the removed key's
//! object +5, served under an identity nothing carried before, then alice's
//! +1, which moves nothing.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st05_removed_key_l3 -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: the test prints `E2E-SKIP st05-l3: <why>` and returns. A skip
//! is not a pass (the spec's prediction counts them).
#![cfg(feature = "network")]

#[macro_use]
mod si01;
#[path = "st01/cluster.rs"]
mod cluster;
mod st05;

use std::collections::BTreeSet;
use std::path::PathBuf;

use cluster::{Cluster, digest, node_binary};
use si01::{OPAQUE, all_ids, strip};
use st05::*;

const ALICE: usize = 0;
const BOB: usize = 1;
const JOINERS: [&str; 7] = ["carol", "dave", "erin", "frank", "grace", "heidi", "ivan"];

/// One model of the run: its place, W's target (none for a removal alone),
/// its id, and the joiner that registers it last.
struct Case {
    place: Place,
    target: Option<Target>,
    model: String,
    joiner: usize,
}

impl Case {
    fn schedule(&self) -> Option<Schedule> {
        self.target.map(|target| Schedule {
            place: self.place,
            xy: (ALICE, BOB),
            x_first: true,
            uw: (ALICE, BOB),
            u_first: true,
            target,
        })
    }

    fn label(&self) -> String {
        match self.target {
            Some(target) => format!("{:?}, W on {:?} (model {})", self.place, target, self.model),
            None => format!("{:?}, removal alone (model {})", self.place, self.model),
        }
    }

    /// Operations every node holds once the removal and W are delivered,
    /// the `Install` among them.
    fn after_removal(&self) -> u64 {
        if self.target.is_some() { 6 } else { 5 }
    }

    fn want(&self) -> serde_json::Value {
        match self.schedule() {
            Some(schedule) => expected(&schedule),
            None => alone(self.place),
        }
    }

    fn roles(&self) -> (&'static [Role], &'static [Role]) {
        match self.schedule() {
            Some(schedule) => race_roles(&schedule),
            None => alone_roles(self.place),
        }
    }
}

#[test]
fn st05_l3_two_nodes_partitioned_and_a_joiner_serve_the_removed_key_by_the_rule() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP st05-l3: {why}");
            return;
        }
    };
    match l3(&binary) {
        Ok(failures) if failures.is_empty() => {}
        Ok(failures) => panic!(
            "ST.05 K5: {} of the checks over the wire fail; first:\n  {}",
            failures.len(),
            failures
                .iter()
                .take(8)
                .cloned()
                .collect::<Vec<_>>()
                .join("\n  ")
        ),
        Err(why) => panic!("st05-l3: {why}"),
    }
}

/// The run; `Err` for a harness failure (a node that never answers, a
/// refused operation), `Ok` with the claim's failures, one line each.
fn l3(binary: &PathBuf) -> Result<Vec<String>, String> {
    let mut ids = vec!["alice", "bob"];
    ids.extend(JOINERS);
    let cluster = Cluster::start(binary, &ids, &[("st05", ST05)])?;
    let key = digest(ST05);
    let tray = Tray::new();
    let mut failures = Vec::new();

    let mut cases = Vec::new();
    for (index, place) in Place::ALL.into_iter().enumerate() {
        let targets = place.targets().iter().copied().map(Some);
        for target in targets.chain([None]) {
            let model = cluster.register(ALICE, None, &key)?;
            cluster.register(BOB, Some(&model), &key)?;
            cases.push(Case {
                place,
                target,
                model,
                joiner: 2 + index,
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

    // ---- partition 1: X on alice, Y on bob ----
    cluster.pause_all(ALICE)?;
    cluster.pause_all(BOB)?;
    for case in &cases {
        let (x, y) = case.place.formation();
        cluster.submit(ALICE, &case.model, &tray.write(case.place, x, 1))?;
        cluster.submit(BOB, &case.model, &tray.write(case.place, y, 2))?;
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
        let plain = cluster.default_state(ALICE, &case.model)?;
        if plain != formed_doc(case.place) {
            return Err(format!(
                "set-up: {}: alice serves {plain} once formed, not {}",
                case.label(),
                formed_doc(case.place)
            ));
        }
        formed.push(Formed::read(case.place, &on_alice, &case.label()));
    }

    // ---- partition 2: U on alice, W on bob ----
    cluster.pause_all(ALICE)?;
    cluster.pause_all(BOB)?;
    for case in &cases {
        cluster.submit(ALICE, &case.model, &tray.removal(case.place))?;
        if let Some(target) = case.target {
            cluster.submit(BOB, &case.model, &tray.write(case.place, target, 10))?;
        }
    }
    cluster.resume_all(ALICE)?;
    cluster.resume_all(BOB)?;
    for (case, formed) in cases.iter().zip(&formed) {
        for node in [ALICE, BOB] {
            cluster.until_delivered(node, &case.model, case.after_removal())?;
        }
        let want = case.want();
        let (kept, left_out) = case.roles();
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
            if let Err(why) = identities(case.place, formed, kept, left_out, &doc, &who) {
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
        cluster.until_delivered(case.joiner, &case.model, case.after_removal())?;
        if !joiner.adopted(&case.model) {
            failures.push(format!(
                "{who}: did not join by state transfer; its log:\n{}",
                joiner.tail()
            ));
        }
        let (kept, left_out) = case.roles();
        let doc = cluster.identified(case.joiner, &case.model)?;
        let plain = cluster.default_state(case.joiner, &case.model)?;
        let on_alice = cluster.identified(ALICE, &case.model)?;
        if doc != on_alice {
            failures.push(format!("{who}: serves {doc} where alice serves {on_alice}"));
        }
        if plain != case.want() || strip(&doc, OPAQUE) != plain {
            failures.push(format!("{who}: serves {plain}, identified {doc}"));
        }
        if let Err(why) = identities(case.place, formed, kept, left_out, &doc, &who) {
            failures.push(why);
        }
    }

    // ---- each joiner writes again: W's target +1, or the removed object +5 ----
    let mut seen: Vec<BTreeSet<String>> = Vec::new();
    for (case, formed) in cases.iter().zip(&formed) {
        let mut ids = formed.all();
        for node in [ALICE, BOB, case.joiner] {
            ids.extend(all_ids(&cluster.identified(node, &case.model)?));
        }
        let op = match case.schedule() {
            Some(schedule) => again_op(&tray, &schedule),
            None => again_alone_op(&tray, case.place),
        };
        cluster.submit(case.joiner, &case.model, &op)?;
        seen.push(ids);
    }
    let mut given = Vec::new();
    for ((case, formed), seen) in cases.iter().zip(&formed).zip(&seen) {
        let reads = settled(&cluster, case, case.after_removal() + 1)?;
        let lines = match case.schedule() {
            Some(schedule) => {
                given.push(None);
                check_again(&schedule, formed, &reads)
            }
            None => {
                let (lines, id) = check_again_alone(case.place, formed, seen, &reads);
                given.push(id);
                lines
            }
        };
        for line in lines {
            failures.push(format!(
                "{}, after the joiner's write: {line}",
                case.label()
            ));
        }
    }

    // ---- then: the left-out Mid written again by the joiner, or alice's +1 ----
    for (case, seen) in cases.iter().zip(seen.iter_mut()) {
        for node in [ALICE, BOB, case.joiner] {
            seen.extend(all_ids(&cluster.identified(node, &case.model)?));
        }
        match case.schedule() {
            Some(schedule) => {
                if let Some(op) = rebuild_op(&tray, &schedule) {
                    cluster.submit(case.joiner, &case.model, &op)?;
                }
            }
            None => cluster.submit(ALICE, &case.model, &once_more_op(&tray, case.place))?,
        }
    }
    for (((case, formed), seen), given) in cases.iter().zip(&formed).zip(&seen).zip(&given) {
        let lines = match case.schedule() {
            Some(schedule) if schedule.drops_sub() => {
                let reads = settled(&cluster, case, case.after_removal() + 2)?;
                check_rebuilt(&schedule, formed, seen, &reads)
            }
            Some(_) => continue,
            None => {
                let reads = settled(&cluster, case, case.after_removal() + 2)?;
                check_once_more(case.place, given.as_deref(), &reads)
            }
        };
        for line in lines {
            failures.push(format!("{}, after the second write: {line}", case.label()));
        }
    }
    for line in &failures {
        println!("ST05-K5 {line}");
    }
    println!(
        "ST05-K5 models {}: failing checks {}",
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
