//! EC.03: record the differential's schedules on the reference (`748a185`).
//!
//! Compiled only in the reference tree, where `ec03/run.sh` adds a test
//! target `ec03_record.rs` that includes this file, under `--features fuzz`:
//! the operations are drawn by the reference interpreter's own generator
//! (`src/fuzz.rs`, `CommandGenerator for ModelLog`), against the state of the
//! replica that sends them, with ChaCha8 seeds. Deliveries are drawn by this
//! file. Every schedule has three replicas `a`, `b`, `c` and four phases by
//! the share of operations written: all online (to 25 %), `c` silent (to
//! 60 %: `c` writes, and nothing reaches it or leaves it), `c` back (to
//! 85 %: the backlog both ways, in random order), all online; then every
//! message is delivered, and twice each replica writes once more and every
//! message is delivered, so every operation is stable everywhere. A step
//! writes (45 %) or delivers one to four pending messages to one replica,
//! often out of causal order. A rebase schedule sends, from `a`, at the first
//! write of `a` past half the operations, the rebase `migrate::rebase` plans.
//!
//! The last line of a schedule holds the digest of each replica's reads at
//! the end of the recording run, which the replay on the reference must
//! reproduce (`ec03/run.sh`, check R2).

use std::panic::{AssertUnwindSafe, catch_unwind};

use moirai_fuzz::op_generator::CommandGenerator;
use moirai_interp::fuzz::set_descriptor;
use moirai_interp::migrate::rebase;
use moirai_interp::testing::{BT_DESCRIPTOR, JSON_DESCRIPTOR, bench_descriptor, install};
use moirai_interp::{ModelLog, ModelOp};
use moirai_protocol::{broadcast::message::EventMessage, replica::IsReplica};
use rand::{RngExt, SeedableRng};
use rand_chacha::ChaCha8Rng;
use serde_json::{Value, json};

use crate::reads::{Rep, digest, reads};
use crate::side;

const UML: &str = include_str!("../fixtures/uml.metamodel.json");
const D1: &str = include_str!("../fixtures/si12/simpleuml.d1.metamodel.json");
const D2: &str = include_str!("../fixtures/si12/simpleuml.d2.metamodel.json");
const D2_DROP: &str = include_str!("../fixtures/si14/simpleuml.d2-drop.metamodel.json");
const DROPS_TAGGED: [&str; 2] = ["ModelElement.taggedValue", "TaggedValue"];
const IDS: [&str; 3] = ["a", "b", "c"];
const C: usize = 2;

/// `ip12`'s seed form: the run's number in the first byte and its complement
/// in the last (`tests/st11_fuzz_generator.rs`).
fn seed(run: u8) -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = run;
    seed[31] = !run;
    seed
}

struct Schedule {
    name: String,
    descriptor: String,
    seed: [u8; 32],
    writes: usize,
    rebase: Option<(&'static str, &'static [&'static str])>,
}

/// Every schedule, in the order of their file names.
fn schedules() -> Vec<Schedule> {
    let bench = serde_json::to_string(&bench_descriptor()).expect("a descriptor");
    let mut all = Vec::new();
    let mut add = |name: String, descriptor: &str, seed, writes, rebase| {
        all.push(Schedule {
            name,
            descriptor: descriptor.to_string(),
            seed,
            writes,
            rebase,
        })
    };
    // `ip12`'s table, seeds and size: three replicas, a thousand operations.
    for run in 0..10 {
        add(
            format!("ip12-bt-{run}"),
            BT_DESCRIPTOR,
            seed(run),
            1_000,
            None,
        );
    }
    for run in 0..4 {
        add(format!("bench-{run}"), &bench, seed(20 + run), 600, None);
        add(
            format!("json-{run}"),
            JSON_DESCRIPTOR,
            seed(30 + run),
            600,
            None,
        );
    }
    for run in 0..2 {
        add(format!("uml-{run}"), UML, seed(40 + run), 600, None);
        add(
            format!("rebase-d2-{run}"),
            D1,
            seed(50 + run),
            600,
            Some((D2, &[])),
        );
        add(
            format!("rebase-drop-{run}"),
            D1,
            seed(60 + run),
            600,
            Some((D2_DROP, &DROPS_TAGGED)),
        );
    }
    all
}

struct Run {
    reps: Vec<Rep>,
    sent: Vec<Vec<EventMessage<ModelOp>>>,
    /// Per receiver, per sender, the message numbers delivered.
    delivered: Vec<Vec<Vec<bool>>>,
    lines: Vec<Value>,
}

impl Run {
    fn send(&mut self, from: usize, op: ModelOp) {
        self.lines.push(json!({"send": IDS[from], "op": op}));
        if let Ok(message) = self.reps[from].send(op) {
            self.sent[from].push(message);
        } else {
            // A refused send holds a number in the trace, so the replay's
            // numbering stays the reference's.
            panic!("the reference refused an operation its own generator drew");
        }
    }

    fn pending(&self, to: usize, silent: bool) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for from in 0..3 {
            if from == to || (silent && (from == C || to == C)) {
                continue;
            }
            for n in 0..self.sent[from].len() {
                if !self.delivered[to][from].get(n).copied().unwrap_or(false) {
                    out.push((from, n));
                }
            }
        }
        out
    }

    fn deliver(&mut self, to: usize, from: usize, n: usize) {
        self.lines
            .push(json!({"deliver": IDS[to], "from": IDS[from], "n": n}));
        let row = &mut self.delivered[to][from];
        if row.len() <= n {
            row.resize(n + 1, false);
        }
        row[n] = true;
        self.reps[to].receive(self.sent[from][n].clone());
    }

    fn drain(&mut self, rng: &mut ChaCha8Rng) {
        loop {
            let all: Vec<(usize, usize, usize)> = (0..3)
                .flat_map(|to| {
                    self.pending(to, false)
                        .into_iter()
                        .map(move |(from, n)| (to, from, n))
                })
                .collect();
            if all.is_empty() {
                return;
            }
            let (to, from, n) = all[rng.random_range(0..all.len())];
            self.deliver(to, from, n);
        }
    }

    /// One operation drawn by the reference's generator, or `None` when the
    /// generator's own debug check stops it (an operation the log refuses).
    fn draw(&self, who: usize, rng: &mut ChaCha8Rng) -> Option<ModelOp> {
        catch_unwind(AssertUnwindSafe(|| {
            self.reps[who].state().generate_command(rng)
        }))
        .ok()
    }
}

fn record(schedule: &Schedule) -> String {
    set_descriptor(&schedule.descriptor);
    let mut rng = ChaCha8Rng::from_seed(schedule.seed);
    let descriptor: Value = serde_json::from_str(&schedule.descriptor).expect("JSON");
    let mut run = Run {
        reps: IDS
            .iter()
            .map(|id| side::bootstrap::<ModelLog>(id, &IDS))
            .collect(),
        sent: vec![Vec::new(); 3],
        delivered: vec![vec![Vec::new(); 3]; 3],
        lines: vec![json!({"schedule": schedule.name, "members": IDS})],
    };
    run.send(0, install("ec03", &descriptor));
    run.deliver(1, 0, 0);
    run.deliver(2, 0, 0);
    let mut written = 0;
    let mut rebased = schedule.rebase.is_none();
    while written < schedule.writes {
        let silent =
            (written * 100 / schedule.writes) >= 25 && (written * 100 / schedule.writes) < 60;
        if rng.random_range(0..100) < 45 {
            let who = rng.random_range(0..3);
            if !rebased && who == 0 && written >= schedule.writes / 2 {
                let (text, drops) = schedule.rebase.expect("a rebase schedule");
                let drops: Vec<String> = drops.iter().map(|d| d.to_string()).collect();
                if let Ok(op) = rebase(run.reps[0].state(), text, &drops) {
                    run.send(0, op);
                    rebased = true;
                    written += 1;
                    continue;
                }
            }
            if let Some(op) = run.draw(who, &mut rng) {
                if run.reps[who].state().refusal(&op).is_ok() {
                    run.send(who, op);
                    written += 1;
                }
            }
        } else {
            let to = rng.random_range(0..3);
            let mut pending = run.pending(to, silent);
            for _ in 0..rng.random_range(1..=4usize) {
                if pending.is_empty() {
                    break;
                }
                let (from, n) = pending.swap_remove(rng.random_range(0..pending.len()));
                run.deliver(to, from, n);
            }
        }
    }
    assert!(rebased, "{}: the rebase was never sent", schedule.name);
    run.drain(&mut rng);
    for _ in 0..2 {
        for who in 0..3 {
            if let Some(op) = run.draw(who, &mut rng)
                && run.reps[who].state().refusal(&op).is_ok()
            {
                run.send(who, op);
            }
        }
        run.drain(&mut rng);
    }
    let end: serde_json::Map<String, Value> = (0..3)
        .map(|i| {
            (
                IDS[i].to_string(),
                Value::String(digest(&reads(&run.reps[i]))),
            )
        })
        .collect();
    run.lines.push(json!({"end": end}));
    let mut text = String::new();
    for line in &run.lines {
        text.push_str(&serde_json::to_string(line).expect("a line"));
        text.push('\n');
    }
    text
}

#[test]
fn ec03_record() {
    let dir = std::env::var("EC03_SCHEDULES").expect("EC03_SCHEDULES names where to write");
    std::fs::create_dir_all(&dir).expect("the schedules directory");
    // The generator's debug check panics on a draw the log would refuse;
    // `draw` takes that as no draw, and the hook keeps it quiet.
    std::panic::set_hook(Box::new(|_| {}));
    for schedule in schedules() {
        let text = record(&schedule);
        std::fs::write(format!("{dir}/{}.jsonl", schedule.name), text).expect("a schedule");
    }
    let _ = std::panic::take_hook();
}
