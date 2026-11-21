//! EC.03b's differential with joiners by state transfer.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03b.md`. It replays EC.03a's
//! recorded schedules (`ec03/record.rs`, unchanged) on `a`, `b` and `c` as
//! `ec03_differential.rs` does, and adds three joiners: at a quarter, half and
//! three quarters of each schedule's steps, `j1`, `j2` and `j3` adopt the
//! snapshot and the log of `a`, `b` and `c` in turn, after a serde round
//! trip, from a member table `[a, b, c, jN]` that puts them at index 3. The
//! joiners write nothing and nobody admits them; after the last step each one
//! delivers every message `a`, `b` and `c` sent, by sender then number, so
//! duplicates, stale events and out-of-order events all reach it. The trace
//! has one line per joiner event (the join, each late delivery, the end),
//! written as the joiner's four reads (`ec03/reads.rs`), and the end lines of
//! `a`, `b` and `c`. `ec03/run.sh` compares the reference's trace with the
//! candidate's line by line. A join line ends in `held` when the joiner reads
//! a rebase still waiting for someone. The reference decides which joins
//! exist: where `748a185` cannot write its donor's snapshot or log as JSON
//! (a map keyed by something other than a string), it writes `unserializable`
//! and makes no joiner, and the candidate, given the reference's trace
//! (`EC03_JOINS`), writes the same line without trying.
//!
//! ```bash
//! EC03_SCHEDULES=<dir> EC03_OUT=<file> cargo test -p moirai-interp \
//!     --test ec03b_joiners -- --ignored --exact ec03b_replay_joiners
//! ```

#[path = "ec03/reads.rs"]
mod reads;
#[path = "ec03/side.rs"]
mod side;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{
    broadcast::{message::EventMessage, tcsb::StateSnapshot},
    replica::IsReplica,
};
use reads::{Rep, digest, reads};
use serde_json::Value;

const JOINERS: [(&str, &str); 3] = [("j1", "a"), ("j2", "b"), ("j3", "c")];

#[test]
#[ignore = "EC.03b's differential with joiners; run by moirai-interp/tests/ec03/run.sh"]
fn ec03b_replay_joiners() {
    let dir = std::env::var("EC03_SCHEDULES").expect("EC03_SCHEDULES names the schedules");
    let out = std::env::var("EC03_OUT").expect("EC03_OUT names the trace to write");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("the schedules directory")
        .map(|entry| entry.expect("an entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no schedule in {dir}");
    let made = reference_joins();
    let mut trace = String::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("a schedule");
        replay(&text, &mut trace, &made);
    }
    std::fs::write(&out, trace).expect("the trace");
}

/// A joiner by state transfer from `donor`, as `md03_g4` makes one; `None`
/// when the donor's snapshot or log cannot be written as JSON.
fn join(donor: &Rep, id: &str) -> Option<Rep> {
    let snapshot = serde_json::to_string(&donor.snapshot()).ok()?;
    let log = serde_json::to_string(donor.state()).ok()?;
    let snapshot: StateSnapshot<ModelOp> =
        serde_json::from_str(&snapshot).expect("a snapshot comes back");
    let log: ModelLog = serde_json::from_str(&log).expect("a log comes back");
    let mut joiner = side::bootstrap_in_order::<ModelLog>(id, &["a", "b", "c", id]);
    joiner.adopt(snapshot, log);
    Some(joiner)
}

/// The joins the reference made, from its trace (`EC03_JOINS`), or `None`
/// on the reference itself, which tries every join.
fn reference_joins() -> Option<BTreeSet<String>> {
    let path = std::env::var("EC03_JOINS").ok()?;
    let text = std::fs::read_to_string(&path).expect("the reference joiner trace");
    Some(
        text.lines()
            .filter(|line| line.split(' ').nth(4) == Some("join"))
            .map(|line| line.split(' ').take(3).collect::<Vec<_>>().join(" "))
            .collect(),
    )
}

fn replay(text: &str, trace: &mut String, made: &Option<BTreeSet<String>>) {
    let mut lines = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("a schedule line is JSON"));
    let head = lines.next().expect("a header");
    let name = head["schedule"]
        .as_str()
        .expect("a schedule name")
        .to_string();
    let ids = ["a", "b", "c"];
    let steps: Vec<Value> = lines.take_while(|line| line.get("end").is_none()).collect();
    let mut reps: BTreeMap<&str, Rep> = ids
        .iter()
        .map(|id| (*id, side::bootstrap::<ModelLog>(id, &ids)))
        .collect();
    let mut sent: BTreeMap<String, Vec<Option<EventMessage<ModelOp>>>> = BTreeMap::new();
    let mut joiners: Vec<(&str, Rep)> = Vec::new();
    for (step, line) in steps.iter().enumerate() {
        for (k, (id, donor)) in JOINERS.iter().enumerate() {
            if step == steps.len() * (k + 1) / 4 {
                let tried = made
                    .as_ref()
                    .is_none_or(|made| made.contains(&format!("{name} {step} {id}")));
                let Some(joiner) = tried.then(|| join(&reps[donor], id)).flatten() else {
                    writeln!(trace, "{name} {step} {id} - unserializable {donor}").expect("a line");
                    continue;
                };
                let doc = reads(&joiner);
                let held = doc["migration"]["rebase"]["d1_held"] == Value::Bool(true);
                let mark = if held { " held" } else { "" };
                writeln!(
                    trace,
                    "{name} {step} {id} {} join {donor}{mark}",
                    digest(&doc)
                )
                .expect("a line");
                joiners.push((*id, joiner));
            }
        }
        if let Some(id) = line["send"].as_str() {
            let op: ModelOp =
                serde_json::from_value(line["op"].clone()).expect("an operation in its JSON form");
            let outcome = reps.get_mut(id).expect("a member sends").send(op);
            sent.entry(id.to_string()).or_default().push(outcome.ok());
        } else if let Some(to) = line["deliver"].as_str() {
            let from = line["from"].as_str().expect("a sender");
            let n = line["n"].as_u64().expect("a message number") as usize;
            if let Some(message) = sent
                .get(from)
                .and_then(|list| list.get(n))
                .cloned()
                .flatten()
            {
                reps.get_mut(to)
                    .expect("a member receives")
                    .receive(message);
            }
        } else {
            panic!("{name}: an unknown step {line}");
        }
    }
    for (id, joiner) in &mut joiners {
        for message in sent.values().flatten().flatten() {
            joiner.receive(message.clone());
            writeln!(trace, "{name} late {id} {}", digest(&reads(joiner))).expect("a line");
        }
    }
    for (id, rep) in &reps {
        writeln!(trace, "{name} end {id} {}", digest(&reads(rep))).expect("a line");
    }
    for (id, joiner) in &joiners {
        writeln!(trace, "{name} end {id} {}", digest(&reads(joiner))).expect("a line");
    }
}
