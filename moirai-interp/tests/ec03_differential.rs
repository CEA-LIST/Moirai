//! EC.03's differential: replay recorded schedules and write what every
//! replica reads after every step.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/EC.03.md`. The schedules are
//! recorded on the reference interpreter (`748a185`, `ec03/record.rs`); this
//! file replays them, unchanged, on the reference and on the candidate, and
//! `ec03/run.sh` compares the two traces line by line. A schedule is a list
//! of steps on three replicas `a`, `b`, `c`: a replica sends an operation
//! (`ModelOp`, as JSON), or a replica receives the `n`-th message another one
//! sent. After each step the replica that moved is read (`ec03/reads.rs`); a
//! refused send is written with its refusal; at the end every replica is read.
//!
//! Run by `ec03/run.sh` only:
//!
//! ```bash
//! EC03_SCHEDULES=<dir> EC03_OUT=<file> cargo test -p moirai-interp \
//!     --test ec03_differential -- --ignored --exact ec03_replay
//! ```
//!
//! `EC03_DUMP=<schedule>:<step>` also prints that step's four reads in full.

#[path = "ec03/reads.rs"]
mod reads;
#[path = "ec03/side.rs"]
mod side;

use std::collections::BTreeMap;
use std::fmt::Write as _;

use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{broadcast::message::EventMessage, replica::IsReplica};
use reads::{Rep, digest, reads};
use serde_json::Value;

#[test]
#[ignore = "EC.03's differential; run by moirai-interp/tests/ec03/run.sh"]
fn ec03_replay() {
    let dir = std::env::var("EC03_SCHEDULES").expect("EC03_SCHEDULES names the schedules");
    let out = std::env::var("EC03_OUT").expect("EC03_OUT names the trace to write");
    let dump = std::env::var("EC03_DUMP").ok();
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("the schedules directory")
        .map(|entry| entry.expect("an entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no schedule in {dir}");
    let mut trace = String::new();
    for file in &files {
        let text = std::fs::read_to_string(file).expect("a schedule");
        replay(&text, &mut trace, dump.as_deref());
    }
    std::fs::write(&out, trace).expect("the trace");
}

fn replay(text: &str, trace: &mut String, dump: Option<&str>) {
    let mut lines = text
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("a schedule line is JSON"));
    let head = lines.next().expect("a header");
    let name = head["schedule"]
        .as_str()
        .expect("a schedule name")
        .to_string();
    let members: Vec<String> = head["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|m| m.as_str().expect("a member").to_string())
        .collect();
    let ids: Vec<&str> = members.iter().map(String::as_str).collect();
    let mut reps: BTreeMap<String, Rep> = ids
        .iter()
        .map(|id| (id.to_string(), side::bootstrap::<ModelLog>(id, &ids)))
        .collect();
    let mut sent: BTreeMap<String, Vec<Option<EventMessage<ModelOp>>>> = BTreeMap::new();
    for (step, line) in lines.enumerate() {
        let (moved, what) = if let Some(id) = line["send"].as_str() {
            let op: ModelOp =
                serde_json::from_value(line["op"].clone()).expect("an operation in its JSON form");
            let rep = reps.get_mut(id).expect("a member sends");
            let outcome = rep.send(op);
            let what = match &outcome {
                Ok(_) => None,
                Err(why) => Some(format!("refused {why}")),
            };
            sent.entry(id.to_string()).or_default().push(outcome.ok());
            (id.to_string(), what)
        } else if let Some(to) = line["deliver"].as_str() {
            let from = line["from"].as_str().expect("a sender");
            let n = line["n"].as_u64().expect("a message number") as usize;
            let message = sent
                .get(from)
                .and_then(|list| list.get(n))
                .cloned()
                .flatten();
            let what = match message {
                Some(message) => {
                    reps.get_mut(to)
                        .expect("a member receives")
                        .receive(message);
                    None
                }
                None => Some(format!("missing {from}#{n}")),
            };
            (to.to_string(), what)
        } else if line.get("end").is_some() {
            break;
        } else {
            panic!("{name}: an unknown step {line}");
        };
        let doc = reads(&reps[&moved]);
        if dump == Some(&format!("{name}:{step}")) {
            eprintln!(
                "EC03-DUMP {name}:{step} {moved}\n{}",
                serde_json::to_string_pretty(&doc).expect("a read serializes")
            );
        }
        let line = match what {
            Some(what) => format!("{name} {step} {moved} {} {what}", digest(&doc)),
            None => format!("{name} {step} {moved} {}", digest(&doc)),
        };
        writeln!(trace, "{line}").expect("a string takes a line");
    }
    for (id, rep) in &reps {
        writeln!(trace, "{name} end {id} {}", digest(&reads(rep))).expect("a line");
    }
}
