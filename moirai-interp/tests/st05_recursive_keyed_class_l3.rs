//! ST.05 round 4 over the wire: K12 of
//! `cea-cdrt-knowledge/spec/items/ST.05.md` (its section "Round 4, written
//! before any rerun").
//!
//! Round 3's K7 to K9 read a record class that reaches itself through keyed
//! collections in process only. Here five `model_node` processes on the
//! NW.02 harness (port 0, `PORTS_FILE`, `PEERS_FILE`, through
//! `st01/cluster.rs`) serve the probe's and the cycles' metamodels, K7 to
//! K9's, as descriptors: `alice`, `bob` and `carol` host 25 models and run
//! K7's schedule, K8's removal alone at each place from each node, K9's race
//! (U on one node and W on another in a partition, `carol` healed first to
//! the writer on `alice` so that it delivers `alice`'s operation first),
//! and K10's `Clear`, alone from each node and raced the same way. Every
//! checkpoint reads `GET /api/model/{id}/state` and
//! `GET /api/model/{id}/state/identified` on every node hosting the model,
//! once each has delivered every operation, and judges them as K7 to K10
//! judge replicas in process. `dave` joins five of the models by state
//! transfer once they are formed, and `erin` five others once their removal
//! or race is delivered; each must say it adopted a state, serve what the
//! others serve, and then write (the first later write of
//! `Scenario::later`), which every node must serve, before `alice` writes
//! the second.
//!
//! Round 1's implementation (`d813a55`) recursed without end on the first
//! read of such a model: a node built from it dies on its first
//! `GET /state`. A read a node does not answer is a failure of the claim
//! (every read returns), with the node's own log quoted, and the run stops
//! there.
//!
//! ```bash
//! nice -n 10 cargo build -j 2 -p moirai-interp --features network --example model_node
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test st05_recursive_keyed_class_l3 -- --test-threads=1 --nocapture
//! ```
//!
//! No binary: the test prints `E2E-SKIP st05-k12: <why>` and returns. A skip
//! is not a pass (the spec's prediction counts them).
#![cfg(feature = "network")]

#[path = "st01/cluster.rs"]
mod cluster;
mod si01;
mod st05recursive;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use cluster::{Cluster, digest, node_binary};
use serde_json::Value;
use st05recursive::{
    Ending, Expect, Meta, Ops, Place, Reads, Role, Scenario, Tally, Target, judge, report,
};

const ALICE: usize = 0;
const BOB: usize = 1;
const CAROL: usize = 2;
const DAVE: usize = 3;
const ERIN: usize = 4;
const NAMES: [&str; 5] = ["alice", "bob", "carol", "dave", "erin"];

/// Why a run stopped: the harness could not go on (a node that never
/// delivers, a refused operation), or a node did not answer a read, which
/// is a failure of the claim and already counted.
enum Halt {
    Harness(String),
    Claim,
}

impl From<String> for Halt {
    fn from(why: String) -> Self {
        Halt::Harness(why)
    }
}

/// The models `dave` joins once they are formed.
const DAVE_TAKES: [Scenario; 5] = [
    Scenario::Probe,
    Scenario::Alone {
        place: Place::Indirect,
        u: CAROL,
    },
    Scenario::Race {
        target: Target::Joint,
        uw: (ALICE, BOB),
        u_first: true,
    },
    Scenario::ClearAlone { u: BOB },
    Scenario::ClearRace {
        target: Target::Fresh,
        uw: (BOB, ALICE),
        u_first: false,
    },
];

/// The models `erin` joins once their removal or race is delivered.
const ERIN_TAKES: [Scenario; 5] = [
    Scenario::Alone {
        place: Place::Top,
        u: BOB,
    },
    Scenario::Alone {
        place: Place::Direct,
        u: ALICE,
    },
    Scenario::Race {
        target: Target::Back,
        uw: (BOB, ALICE),
        u_first: false,
    },
    Scenario::ClearAlone { u: CAROL },
    Scenario::ClearRace {
        target: Target::Inner,
        uw: (ALICE, BOB),
        u_first: true,
    },
];

/// One model of the run.
struct Case {
    scenario: Scenario,
    model: String,
    /// The node that joins it later, if one does.
    joiner: Option<usize>,
    /// The nodes hosting it now.
    hosts: Vec<usize>,
    /// Operations every host has delivered, its `Install` included.
    count: u64,
    /// The identity each role was last served under.
    known: BTreeMap<Role, String>,
    /// Every identity served at a checkpoint of this model.
    seen: BTreeSet<String>,
}

impl Case {
    fn label(&self) -> String {
        format!("{:?} (model {})", self.scenario, self.model)
    }
}

#[test]
fn st05_k12_nodes_and_joiners_over_the_wire_serve_a_recursive_keyed_model_by_the_rule() {
    let binary = match node_binary() {
        Ok(binary) => binary,
        Err(why) => {
            eprintln!("\nE2E-SKIP st05-k12: {why}");
            return;
        }
    };
    let mut tally = Tally::default();
    let mut models = 0;
    let mut joined = 0;
    match l3(&binary, &mut tally, &mut models, &mut joined) {
        Ok(()) | Err(Halt::Claim) => {}
        Err(Halt::Harness(why)) => panic!("st05-k12: {why}"),
    }
    let line = format!(
        "ST05-K12 models {models}, joined {joined}: checkpoints {}, node reads {}, parted {}, \
         value other than the rule's {}, identity {}, r' {}, reads that did not answer {}, \
         joins not by state transfer {}",
        tally.checkpoints,
        tally.reads,
        tally.parted,
        tally.value,
        tally.identity,
        tally.r_prime,
        tally.unanswered,
        tally.not_adopted
    );
    report(&line, &tally);
}

/// The run.
fn l3(
    binary: &PathBuf,
    tally: &mut Tally,
    models: &mut usize,
    joined: &mut usize,
) -> Result<(), Halt> {
    let probe_text = serde_json::to_string(&Meta::Probe.descriptor())
        .expect("set-up: the probe's descriptor serializes");
    let cycles_text = serde_json::to_string(&Meta::Cycles.descriptor())
        .expect("set-up: the cycles' descriptor serializes");
    let cluster = Cluster::start(
        binary,
        &NAMES,
        &[
            ("st05probe", probe_text.as_str()),
            ("st05cycles", cycles_text.as_str()),
        ],
    )?;
    let key_of = |meta: Meta| match meta {
        Meta::Probe => digest(&probe_text),
        Meta::Cycles => digest(&cycles_text),
    };
    let probe_ops = Ops::new(Meta::Probe);
    let cycles_ops = Ops::new(Meta::Cycles);
    let ops_of = |scenario: Scenario| match scenario.meta() {
        Meta::Probe => &probe_ops,
        Meta::Cycles => &cycles_ops,
    };

    // ---- 25 models on alice, bob and carol ----
    let mut scenarios = vec![Scenario::Probe];
    for place in [Place::Top, Place::Direct, Place::Indirect] {
        for u in [ALICE, BOB, CAROL] {
            scenarios.push(Scenario::Alone { place, u });
        }
    }
    for target in [Target::Joint, Target::Inner, Target::Back] {
        scenarios.push(Scenario::Race {
            target,
            uw: (ALICE, BOB),
            u_first: true,
        });
        scenarios.push(Scenario::Race {
            target,
            uw: (BOB, ALICE),
            u_first: false,
        });
    }
    for u in [ALICE, BOB, CAROL] {
        scenarios.push(Scenario::ClearAlone { u });
    }
    for target in [Target::Inner, Target::Back, Target::Fresh] {
        scenarios.push(Scenario::ClearRace {
            target,
            uw: (ALICE, BOB),
            u_first: true,
        });
        scenarios.push(Scenario::ClearRace {
            target,
            uw: (BOB, ALICE),
            u_first: false,
        });
    }
    let mut cases = Vec::new();
    for scenario in scenarios {
        let key = key_of(scenario.meta());
        let model = cluster.register(ALICE, None, &key)?;
        cluster.register(BOB, Some(&model), &key)?;
        cluster.register(CAROL, Some(&model), &key)?;
        let joiner = if DAVE_TAKES.contains(&scenario) {
            Some(DAVE)
        } else if ERIN_TAKES.contains(&scenario) {
            Some(ERIN)
        } else {
            None
        };
        cases.push(Case {
            scenario,
            model,
            joiner,
            hosts: vec![ALICE, BOB, CAROL],
            count: 1,
            known: BTreeMap::new(),
            seen: BTreeSet::new(),
        });
    }
    *models = cases.len();
    for case in &cases {
        for node in [BOB, CAROL] {
            cluster.until_delivered(node, &case.model, 1)?;
        }
    }

    // ---- the formation, each write on alice, a checkpoint after each ----
    for step in 0..5 {
        let mut written = Vec::new();
        for (index, case) in cases.iter().enumerate() {
            if let Some((_, op, _)) = case
                .scenario
                .formation(ops_of(case.scenario))
                .into_iter()
                .nth(step)
            {
                cluster.submit(ALICE, &case.model, &op)?;
                written.push(index);
            }
        }
        for index in written {
            let case = &mut cases[index];
            let (at, _, expect) = case
                .scenario
                .formation(ops_of(case.scenario))
                .into_iter()
                .nth(step)
                .expect("set-up: written above");
            case.count += 1;
            checkpoint(&cluster, case, at, &expect, tally)?;
        }
    }

    // ---- dave joins five formed models by state transfer ----
    for case in cases.iter_mut().filter(|case| case.joiner == Some(DAVE)) {
        cluster.register(DAVE, Some(&case.model), &key_of(case.scenario.meta()))?;
        case.hosts.push(DAVE);
    }
    for case in cases.iter_mut().filter(|case| case.joiner == Some(DAVE)) {
        let (_, _, formed) = case
            .scenario
            .formation(ops_of(case.scenario))
            .pop()
            .expect("set-up: a formation writes");
        join(&cluster, case, DAVE, &formed.settled(), tally)?;
        *joined += 1;
    }

    // ---- the removals alone, and the `Clear`s alone ----
    let mut alone = Vec::new();
    for (index, case) in cases.iter().enumerate() {
        if let Ending::Alone { seat, op } = case.scenario.ending(ops_of(case.scenario)) {
            cluster.submit(seat, &case.model, &op)?;
            alone.push(index);
        }
    }
    for index in alone {
        let case = &mut cases[index];
        case.count += 1;
        let ended = case.scenario.ended();
        checkpoint(&cluster, case, "after the ending", &ended, tally)?;
    }

    // ---- the races: U and W in a partition, carol healed to alice first ----
    let racing: Vec<usize> = (0..cases.len())
        .filter(|&index| cases[index].scenario.is_race())
        .collect();
    for node in [ALICE, BOB, CAROL, DAVE] {
        cluster.pause_all(node)?;
    }
    for &index in &racing {
        let case = &cases[index];
        if let Ending::Race { uw, u, w, .. } = case.scenario.ending(ops_of(case.scenario)) {
            cluster.submit(uw.0, &case.model, &u)?;
            cluster.submit(uw.1, &case.model, &w)?;
        }
    }
    cluster.resume_all(ALICE)?;
    cluster.resume_all(CAROL)?;
    for &index in &racing {
        let case = &cases[index];
        cluster.until_delivered(CAROL, &case.model, case.count + 1)?;
    }
    for &index in &racing {
        let case = &cases[index];
        let held = cluster.delivered(CAROL, &case.model);
        if held != case.count + 1 {
            return Err(Halt::Harness(format!(
                "set-up: {}: carol delivered {held} operations before bob was healed, not {}",
                case.label(),
                case.count + 1
            )));
        }
    }
    cluster.resume_all(BOB)?;
    cluster.resume_all(DAVE)?;
    for &index in &racing {
        let case = &mut cases[index];
        case.count += 2;
        let ended = case.scenario.ended();
        checkpoint(&cluster, case, "after U and W", &ended, tally)?;
    }

    // ---- erin joins five models once their ending is delivered ----
    for case in cases.iter_mut().filter(|case| case.joiner == Some(ERIN)) {
        cluster.register(ERIN, Some(&case.model), &key_of(case.scenario.meta()))?;
        case.hosts.push(ERIN);
    }
    for case in cases.iter_mut().filter(|case| case.joiner == Some(ERIN)) {
        let ended = case.scenario.ended().settled();
        join(&cluster, case, ERIN, &ended, tally)?;
        *joined += 1;
    }

    // ---- each joiner writes, then alice ----
    for (step, by_joiner) in [(0, true), (1, false)] {
        let mut written = Vec::new();
        for (index, case) in cases.iter().enumerate() {
            let Some(joiner) = case.joiner else {
                continue;
            };
            let writer = if by_joiner { joiner } else { ALICE };
            let (op, _) = case.scenario.later(ops_of(case.scenario))[step].clone();
            cluster.submit(writer, &case.model, &op)?;
            written.push(index);
        }
        for index in written {
            let case = &mut cases[index];
            let (_, expect) = case.scenario.later(ops_of(case.scenario))[step].clone();
            case.count += 1;
            let at = if by_joiner {
                "after the joiner's write"
            } else {
                "after alice's write"
            };
            checkpoint(&cluster, case, at, &expect, tally)?;
        }
    }
    Ok(())
}

/// `joiner` has registered `case`'s model: once it has delivered what the
/// others hold, its log must say it adopted a transferred state, and every
/// host, the joiner among them, must serve `expect`.
fn join(
    cluster: &Cluster,
    case: &mut Case,
    joiner: usize,
    expect: &Expect,
    tally: &mut Tally,
) -> Result<(), Halt> {
    cluster.until_delivered(joiner, &case.model, case.count)?;
    if !cluster.nodes[joiner].adopted(&case.model) {
        tally.not_adopted += 1;
        tally.lines.push(format!(
            "{}: {} did not join by state transfer; its log:\n{}",
            case.label(),
            NAMES[joiner],
            cluster.nodes[joiner].tail()
        ));
    }
    let at = format!("{} joined", NAMES[joiner]);
    checkpoint(cluster, case, &at, expect, tally)
}

/// A checkpoint: every host has delivered `case.count` operations; both
/// reads of each, judged as in process.
fn checkpoint(
    cluster: &Cluster,
    case: &mut Case,
    at: &str,
    expect: &Expect,
    tally: &mut Tally,
) -> Result<(), Halt> {
    for &node in &case.hosts {
        cluster.until_delivered(node, &case.model, case.count)?;
    }
    let label = format!("{}, {at}", case.label());
    let mut reads = Vec::new();
    for &node in &case.hosts {
        let identified = answered(cluster, node, &label, tally, |c| {
            c.identified(node, &case.model)
        })?;
        let default = answered(cluster, node, &label, tally, |c| {
            c.default_state(node, &case.model)
        })?;
        tally.reads += 1;
        reads.push(Reads {
            who: NAMES[node].to_string(),
            default,
            identified,
        });
    }
    let mut verdict = judge(&label, &reads, expect, &case.known, &case.seen);
    case.known.extend(std::mem::take(&mut verdict.ids));
    case.seen.extend(std::mem::take(&mut verdict.served));
    tally.add(verdict);
    Ok(())
}

/// One read of one node, or the claim's failure: a node that does not
/// answer, with what its log says.
fn answered(
    cluster: &Cluster,
    node: usize,
    label: &str,
    tally: &mut Tally,
    read: impl Fn(&Cluster) -> Result<Value, String>,
) -> Result<Value, Halt> {
    match read(cluster) {
        Ok(value) => Ok(value),
        Err(why) => {
            tally.unanswered += 1;
            tally.lines.push(format!(
                "{label}: {} did not answer a read ({why}); its log: {}",
                NAMES[node],
                alarms(&cluster.nodes[node])
            ));
            Err(Halt::Claim)
        }
    }
}

/// The lines of a node's log that say it died, waiting up to two seconds for
/// them; else its last lines.
fn alarms(node: &cluster::Node) -> String {
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let text = node.log_text();
        let lines: Vec<&str> = text
            .lines()
            .filter(|line| {
                line.contains("overflowed its stack")
                    || line.contains("fatal runtime error")
                    || line.contains("panicked")
            })
            .take(4)
            .collect();
        if !lines.is_empty() {
            return lines.join(" | ");
        }
        if Instant::now() > deadline {
            return format!("no alarm; last lines: {}", node.tail().replace('\n', " | "));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
