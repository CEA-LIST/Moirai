//! SI.14 in process: rebase migration on three real `ModelLog` replicas of
//! one model, in every delivery order of each concurrent schedule, with a
//! check after every step on the replica that takes it.
//!
//! The spec is `cea-cdrt-knowledge/spec/items/SI.14.md`; the contract (M15
//! to M23) is restated in `si14/mod.rs`. Every read is the one a node
//! serves: `QueryableLog::query_identified_state_json` and
//! `QueryableLog::query_state_json`; the status is `ModelLog::migration()`.
//!
//! What each step expects is computed from the schedule and the contract,
//! never read off the implementation. The oracle knows every event it sent:
//! its origin, its sequence number, its Lamport stamp (the events of the
//! model in its causal past, itself included: the running example's 70
//! events by `a`, then each replica's own events, since every replica writes
//! its part of a schedule before it delivers anyone else's), and the
//! migration operations in its causal past. From those it knows, at every
//! step, which migration the rule M22 keeps, which events a removal drops,
//! which are discarded, which members a replica still waits for, and the
//! document: the running example under the kept revision, mapped by hand in
//! `si14/mod.rs` and checked against a direct build in `si14_fixtures.rs`,
//! with the effect of each delivered edit written in by hand.
//!
//! The replicas are `a`, `b` and `c` of `triplet_log`, every one with the
//! members a, b, c (ST.16 parked: the member tables hold the same members).
//! A schedule's delivery orders are run per replica: for the replica under
//! test, every interleaving of the other replicas' streams after its own,
//! each on a fresh model. Every replica's result is held to the same
//! expected document and status, which is the convergence claim.
//!
//! ```bash
//! nice -n 10 cargo test -j 2 -p moirai-interp --features network \
//!     --test si14_rebase -- --test-threads=1 --nocapture
//! ```

mod si14;

use std::collections::{BTreeMap, BTreeSet};

use moirai_crdt::utils::membership::triplet_log;
use moirai_interp::leaf::{LeafOp, Scalar};
use moirai_interp::migrate::{migrate, rebase};
use moirai_interp::{ModelLog, ModelOp};
use moirai_network::query::QueryableLog;
use moirai_protocol::broadcast::message::EventMessage;
use moirai_protocol::broadcast::tcsb::Tcsb;
use moirai_protocol::replica::{IsReplica, Replica};
use serde_json::{Value, json};

use si14::si12::{
    At, CAR, CAR_WHEELS, D1, D2, D2_RENAME, DOOR, HAS_DOORS, SEDAN, Uml, ids_by_label, install,
    interleavings, labels, project_with, strip_ids,
};
use si14::*;

type Rep = Replica<ModelLog, Tcsb<ModelOp>>;
type Msg = EventMessage<ModelOp>;

const A: usize = 0;
const B: usize = 1;
const C: usize = 2;
const IDS: [&str; 3] = ["a", "b", "c"];
/// The successor a cut of these tests names.
const COPY: &str = "si14-copy";

fn identified(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_identified_state_json(replica)
}

fn plain(replica: &Rep) -> Value {
    <ModelLog as QueryableLog>::query_state_json(replica)
}

fn send(replica: &mut Rep, op: ModelOp, what: &str) -> Msg {
    replica
        .send(op)
        .unwrap_or_else(|why| panic!("{what}: the local intake refused it: {why}"))
}

/// M16: the rebase `replica` plans to `rev`, dropping `names`.
fn rebase_op(replica: &Rep, rev: Rev, names: &[&str]) -> ModelOp {
    rebase(replica.state(), rev.text(), &drops(names)).unwrap_or_else(|why| {
        panic!(
            "SI.14 (M16): a rebase to {} with drops {names:?} was refused: {why}",
            rev.digest()
        )
    })
}

/// The running example on three replicas, written on `a` and delivered to
/// `b` and `c`, with what the oracle reads off it before any migration.
struct Base {
    reps: [Rep; 3],
    /// Identity to label, from `a`'s identified read.
    labels: BTreeMap<String, String>,
    /// Label to identity.
    ids: BTreeMap<String, String>,
    /// `a`'s two reads: what a cut written now freezes.
    identified: Value,
    plain: Value,
}

fn base() -> Base {
    let uml = Uml::new(D1);
    let (a, b, c) = triplet_log::<ModelLog>();
    let mut reps = [a, b, c];
    let mut sent = 0u64;
    let mut write = |reps: &mut [Rep; 3], op: ModelOp| {
        let message = send(&mut reps[A], op, "building the running example");
        reps[B].receive(message.clone());
        reps[C].receive(message);
        sent += 1;
    };
    write(&mut reps, install(MODEL, D1));
    for op in uml.structure() {
        write(&mut reps, op);
    }
    let ids = ids_by_label(&identified(&reps[A]));
    for op in uml.links(&ids) {
        write(&mut reps, op);
    }
    assert_eq!(
        sent, 70,
        "premise: the running example is a@1 to a@70 (RC's tie at 71 rests on it)"
    );
    let doc = identified(&reps[A]);
    for (index, replica) in reps.iter().enumerate() {
        assert_eq!(
            si14::si12::project(&identified(replica)),
            si14::si12::expected_d1(),
            "premise: replica {} serves the running example under D1",
            IDS[index]
        );
        assert_eq!(status14(&replica.state().migration()), quiet(), "premise");
    }
    Base {
        labels: labels(&doc),
        ids: ids_by_label(&doc),
        identified: doc,
        plain: plain(&reps[A]),
        reps,
    }
}

/// What one replica must read at one step.
enum Want {
    /// The model under `rev`, serving `doc` (projected through the base's
    /// labels, [`project_keep`]), its status reading `status`.
    Live {
        rev: Rev,
        docs: Vec<Value>,
        status: Value,
    },
    /// The model frozen by a cut: `a`'s two reads before it, under D1.
    Frozen { status: Value },
}

/// Check one replica against `want`: the table, the descriptor, both reads,
/// nothing unrouted, the status.
fn check(replica: &Rep, base: &Base, want: &Want, at: &str) {
    let log = replica.state();
    let status = status14(&log.migration());
    let (rev, wanted_status) = match want {
        Want::Live { rev, status, .. } => (*rev, status),
        Want::Frozen { status } => (Rev::D1, status),
    };
    assert_eq!(
        &status, wanted_status,
        "{at}: the migration status (M17, M20, M22, M23) reads {status}, the schedule gives \
         {wanted_status}"
    );
    let header = log.header().unwrap_or_else(|| panic!("{at}: no header"));
    assert_eq!(header.model_id, MODEL, "{at} (M17): the model's id moved");
    assert_eq!(
        header.metamodel_id,
        rev.digest(),
        "{at} (M17, M22): the model is listed under another metamodel than the kept revision's"
    );
    assert_eq!(
        log.semantics().map(|sem| sem.digest.clone()),
        Some(rev.digest()),
        "{at} (M17, M22): the table in force is not the kept revision's"
    );
    assert_eq!(
        log.descriptor(),
        Some(rev.text()),
        "{at} (M17, M21): the descriptor in force is not the kept revision's text"
    );
    assert_eq!(
        log.unresolved(),
        0,
        "{at} (M18, H4): an operation went unrouted and was counted instead of translated, \
         dropped or discarded"
    );
    let doc = identified(replica);
    match want {
        Want::Live { docs, .. } => {
            let projected = project_keep(&doc, &base.labels);
            assert!(
                docs.contains(&projected),
                "{at} (M17, M18, M20): the model reads {projected}; the schedule gives {}",
                docs.iter()
                    .map(Value::to_string)
                    .collect::<Vec<_>>()
                    .join(" or ")
            );
            let merged = labels_keep(&doc, &base.labels);
            assert_eq!(
                project_with(&plain(replica), &merged),
                strip_ids(&projected),
                "{at} (M17): the default read is not the identified read without identities"
            );
        }
        Want::Frozen { .. } => {
            assert_eq!(
                doc, base.identified,
                "{at} (M22, SI.12 M4): a kept cut freezes the identified read at its writer's"
            );
            assert_eq!(
                plain(replica),
                base.plain,
                "{at} (M22, SI.12 M4): a kept cut freezes the default read at its writer's"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// The schedules, and the rule computed from them
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Kind {
    /// An instance operation. `dropped_by` names, per revision, the removed
    /// feature or class its path reaches first (M20).
    Edit {
        dropped_by: Vec<(Rev, &'static str)>,
    },
    Rebase {
        rev: Rev,
    },
    Cut,
}

/// One event of a schedule, before it is sent.
struct Planned {
    name: &'static str,
    origin: usize,
    kind: Kind,
    make: Box<dyn Fn(&Rep) -> ModelOp>,
}

/// One event of a schedule, as the oracle knows it once sent.
#[derive(Clone, Debug)]
struct Sent {
    name: &'static str,
    origin: usize,
    seq: u64,
    stamp: u64,
    kind: Kind,
    /// The migration operations in its causal past, itself excluded.
    past: Vec<&'static str>,
}

impl Sent {
    fn id(&self) -> (String, u64) {
        ev(IDS[self.origin], self.seq)
    }

    fn is_migration(&self) -> bool {
        !matches!(self.kind, Kind::Edit { .. })
    }
}

fn edit(
    name: &'static str,
    origin: usize,
    dropped_by: Vec<(Rev, &'static str)>,
    make: impl Fn(&Rep) -> ModelOp + 'static,
) -> Planned {
    Planned {
        name,
        origin,
        kind: Kind::Edit { dropped_by },
        make: Box::new(make),
    }
}

fn plain_edit(name: &'static str, origin: usize, op: ModelOp) -> Planned {
    edit(name, origin, Vec::new(), move |_| op.clone())
}

fn rebase_to(
    name: &'static str,
    origin: usize,
    rev: Rev,
    names: &'static [&'static str],
) -> Planned {
    Planned {
        name,
        origin,
        kind: Kind::Rebase { rev },
        make: Box::new(move |replica| rebase_op(replica, rev, names)),
    }
}

fn cut(name: &'static str, origin: usize) -> Planned {
    Planned {
        name,
        origin,
        kind: Kind::Cut,
        make: Box::new(|replica| {
            migrate(replica.state(), COPY, D2)
                .unwrap_or_else(|why| panic!("set-up: the copy-migration was refused: {why}"))
                .cut
        }),
    }
}

/// The documents a model may read under a revision once a set of events is
/// delivered (one, but where D1's text leaves two orders of characters).
type Docs = Box<dyn Fn(Rev, &BTreeSet<&'static str>) -> Vec<Value>>;

/// An operation the replica under test writes under the kept revision at
/// the end of a run, and how the document reads after it.
type Last = Box<dyn Fn(&Rep, &Base) -> (ModelOp, Value)>;

/// A schedule: each replica's stream, written before any exchange, and the
/// document the model reads under a revision once a given set of events is
/// delivered.
struct Schedule {
    name: &'static str,
    plan: Vec<Planned>,
    doc: Docs,
    last: Option<Last>,
}

/// M22, computed from the schedule: the greatest stamp, then the least
/// origin id by bytes, then the least sequence number.
fn kept<'a>(events: &'a [Sent], delivered: &BTreeSet<&'static str>) -> Option<&'a Sent> {
    events
        .iter()
        .filter(|event| event.is_migration() && delivered.contains(event.name))
        .max_by(|x, y| {
            x.stamp
                .cmp(&y.stamp)
                .then_with(|| IDS[y.origin].as_bytes().cmp(IDS[x.origin].as_bytes()))
                .then_with(|| y.seq.cmp(&x.seq))
        })
}

/// What replica `me` must read once it has delivered `delivered`.
fn expected(
    schedule: &Schedule,
    events: &[Sent],
    delivered: &BTreeSet<&'static str>,
    me: usize,
    base: &Base,
) -> Want {
    let held: Vec<&Sent> = events
        .iter()
        .filter(|event| delivered.contains(event.name))
        .collect();
    let rebases = |except: Option<&str>| -> Vec<(String, u64)> {
        held.iter()
            .filter(|event| matches!(event.kind, Kind::Rebase { .. }) && Some(event.name) != except)
            .map(|event| event.id())
            .collect()
    };
    let cuts = |except: Option<&str>| -> Vec<String> {
        held.iter()
            .filter(|event| matches!(event.kind, Kind::Cut) && Some(event.name) != except)
            .map(|_| COPY.to_string())
            .collect()
    };
    match kept(events, delivered) {
        None => Want::Live {
            rev: Rev::D1,
            docs: (schedule.doc)(Rev::D1, delivered),
            status: quiet(),
        },
        Some(cut) if matches!(cut.kind, Kind::Cut) => {
            let seen: BTreeSet<&str> = events
                .iter()
                .filter(|event| event.origin == cut.origin && event.seq <= cut.seq)
                .map(|event| event.name)
                .collect();
            let refused: Vec<(String, u64)> = held
                .iter()
                .filter(|event| !seen.contains(event.name))
                .map(|event| event.id())
                .collect();
            Want::Frozen {
                status: cut_status(COPY, &refused, &cuts(Some(cut.name)), &rebases(None)),
            }
        }
        Some(kept) => {
            let Kind::Rebase { rev, .. } = &kept.kind else {
                unreachable!()
            };
            let rev = *rev;
            let hidden = match rev {
                Rev::Drop => vec![base.ids["wheels"].clone()],
                Rev::Noassoc => vec![base.ids["hasDoors"].clone()],
                _ => Vec::new(),
            };
            let mut dropped = Vec::new();
            let mut discarded = Vec::new();
            for event in &held {
                let Kind::Edit { dropped_by } = &event.kind else {
                    continue;
                };
                if event.past.is_empty() {
                    if let Some((_, feature)) = dropped_by.iter().find(|(r, _)| *r == rev) {
                        let (origin, seq) = event.id();
                        dropped.push((origin, seq, feature.to_string()));
                    }
                } else if !event.past.contains(&kept.name) {
                    discarded.push(event.id());
                }
            }
            let waiting_for: Vec<String> = (0..3)
                .filter(|member| *member != me)
                .filter(|member| {
                    !held.iter().any(|event| {
                        event.origin == *member
                            && (event.name == kept.name || event.past.contains(&kept.name))
                    })
                })
                .map(|member| IDS[member].to_string())
                .collect();
            let (origin, seq) = kept.id();
            Want::Live {
                rev,
                docs: (schedule.doc)(rev, delivered),
                status: RebaseStatus {
                    origin,
                    seq,
                    to: rev.digest(),
                    hidden,
                    dropped,
                    discarded,
                    waiting_for,
                    superseded: cuts(None),
                    superseded_rebases: rebases(Some(kept.name)),
                }
                .render(),
            }
        }
    }
}

/// Every replica, every interleaving of the other replicas' streams after
/// its own, a check after every step. Answers the number of replica
/// schedules run.
fn run(schedule: &Schedule) -> usize {
    let mut runs = 0;
    let mut steps = 0;
    for me in [A, B, C] {
        let others: Vec<usize> = [A, B, C]
            .into_iter()
            .filter(|origin| {
                *origin != me
                    && schedule
                        .plan
                        .iter()
                        .any(|planned| planned.origin == *origin)
            })
            .collect();
        let lengths: Vec<usize> = others
            .iter()
            .map(|origin| schedule.plan.iter().filter(|p| p.origin == *origin).count())
            .collect();
        for order in interleavings(&lengths) {
            let mut base = base();
            // Every replica writes its own stream before any exchange: its
            // own events first, the replica under test checked after each.
            let mut events: Vec<Sent> = Vec::new();
            let mut messages: BTreeMap<&'static str, Msg> = BTreeMap::new();
            let mut delivered: BTreeSet<&'static str> = BTreeSet::new();
            let label = |delivered: &BTreeSet<&'static str>| {
                format!(
                    "{} on {} (order {order:?} of {others:?}), having delivered {delivered:?}",
                    schedule.name, IDS[me]
                )
            };
            for writer in [me].into_iter().chain(others.iter().copied()).chain(
                [A, B, C]
                    .into_iter()
                    .filter(|o| *o != me && !others.contains(o)),
            ) {
                let mut written = 0u64;
                let mut past = Vec::new();
                for planned in schedule.plan.iter().filter(|p| p.origin == writer) {
                    let op = (planned.make)(&base.reps[writer]);
                    let message = send(&mut base.reps[writer], op, planned.name);
                    written += 1;
                    let seq = if writer == A { 70 + written } else { written };
                    let sent = Sent {
                        name: planned.name,
                        origin: writer,
                        seq,
                        stamp: 70 + written,
                        kind: planned.kind.clone(),
                        past: past.clone(),
                    };
                    assert_eq!(
                        (
                            message.event().id().seq() as u64,
                            message.event().lamport().val() as u64
                        ),
                        (seq, 70 + written),
                        "premise: {} is {}@{seq} at Lamport stamp {}",
                        planned.name,
                        IDS[writer],
                        70 + written
                    );
                    if sent.is_migration() {
                        past.push(planned.name);
                    }
                    events.push(sent);
                    messages.insert(planned.name, message);
                    if writer == me {
                        delivered.insert(planned.name);
                        let want = expected(schedule, &events, &delivered, me, &base);
                        check(&base.reps[me], &base, &want, &label(&delivered));
                        steps += 1;
                    }
                }
            }
            let streams: Vec<Vec<&'static str>> = others
                .iter()
                .map(|origin| {
                    schedule
                        .plan
                        .iter()
                        .filter(|p| p.origin == *origin)
                        .map(|p| p.name)
                        .collect()
                })
                .collect();
            let mut next = vec![0usize; streams.len()];
            for stream in &order {
                let name = streams[*stream][next[*stream]];
                next[*stream] += 1;
                base.reps[me].receive(messages[name].clone());
                delivered.insert(name);
                let want = expected(schedule, &events, &delivered, me, &base);
                check(&base.reps[me], &base, &want, &label(&delivered));
                steps += 1;
            }
            if let Some(last) = &schedule.last {
                let (op, doc) = last(&base.reps[me], &base);
                let before = status14(&base.reps[me].state().migration());
                send(
                    &mut base.reps[me],
                    op,
                    &format!(
                        "{} (M17, M20): an edit under the kept revision on {}",
                        schedule.name, IDS[me]
                    ),
                );
                let projected = project_keep(&identified(&base.reps[me]), &base.labels);
                assert_eq!(
                    projected,
                    doc,
                    "{}: after its own edit under the kept revision",
                    label(&delivered)
                );
                assert_eq!(
                    status14(&base.reps[me].state().migration()),
                    before,
                    "{}: its own edit moved the status",
                    label(&delivered)
                );
            }
            runs += 1;
        }
    }
    println!(
        "SI14-{}: {runs} replica schedules, {steps} steps checked",
        schedule.name
    );
    runs
}

fn has(delivered: &BTreeSet<&'static str>, name: &str) -> bool {
    delivered.contains(name)
}

// ---------------------------------------------------------------------------
// RA, the rebase in place, and RF's in-process half
// ---------------------------------------------------------------------------

/// RA: `a` rebases `m` to D2 with no edit concurrent with it. On `a` at once
/// and on `b` and `c` once they deliver it: the same model id, D2's digest,
/// table and text (RF: `b` and `c` held no D2 but what the operation
/// carried), every object under the identity it had, the running example
/// under D2 in both reads, the status naming the rebase, D1 held and the
/// members not yet heard from, and the intake checking against D2.
#[test]
fn si14_ra_a_rebase_keeps_the_model_its_id_and_every_identity() {
    let mut base = base();
    let op = rebase_op(&base.reps[A], Rev::D2, &[]);
    let message = send(&mut base.reps[A], op, "RA: a's rebase");
    assert_eq!(
        (message.event().id().seq(), message.event().lamport().val()),
        (71, 71),
        "premise: a's rebase is a@71 at stamp 71"
    );
    let status = |waiting: &[&str]| {
        RebaseStatus {
            origin: "a".into(),
            seq: 71,
            to: Rev::D2.digest(),
            waiting_for: waiting.iter().map(|s| s.to_string()).collect(),
            ..RebaseStatus::default()
        }
        .render()
    };
    let want = |waiting: &[&str]| Want::Live {
        rev: Rev::D2,
        docs: vec![Rev::D2.running_example()],
        status: status(waiting),
    };
    check(
        &base.reps[A],
        &base,
        &want(&["b", "c"]),
        "RA on a, right after its rebase",
    );
    check(
        &base.reps[B],
        &base,
        &Want::Live {
            rev: Rev::D1,
            docs: vec![Rev::D1.running_example()],
            status: quiet(),
        },
        "RA on b, before it delivers the rebase",
    );
    base.reps[B].receive(message.clone());
    base.reps[C].receive(message);
    check(
        &base.reps[B],
        &base,
        &want(&["c"]),
        "RA on b, having delivered the rebase",
    );
    check(
        &base.reps[C],
        &base,
        &want(&["b"]),
        "RA on c, having delivered the rebase",
    );
    assert_eq!(
        identified(&base.reps[A]),
        identified(&base.reps[B]),
        "RA: a and b serve one document"
    );
    assert_eq!(identified(&base.reps[A]), identified(&base.reps[C]));

    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    for (index, replica) in base.reps.iter().enumerate() {
        let log = replica.state();
        assert!(
            log.refusal(&d2.rename_door("Gate")).is_ok(),
            "RA (M17) on {}: the intake refuses an operation written under D2: {:?}",
            IDS[index],
            log.refusal(&d2.rename_door("Gate"))
        );
        assert!(
            log.refusal(&d1.name_char(DOOR, 4, 's')).is_err(),
            "RA (M17) on {}: the intake accepts an operation spelled by D1's slots and rules after \
             the rebase",
            IDS[index]
        );
    }
    println!(
        "SI14-RA: a rebase to D2 keeps si14-m, its identities and the running example on a, b and c"
    );
}

// ---------------------------------------------------------------------------
// RB, the late operations
// ---------------------------------------------------------------------------

/// RB: SI.12 C3's schedule with a rebase in place of the cut. `b` renames
/// `Door` to `Doors` (a character, `b@1`) and makes it abstract (`b@2`), `c`
/// adds the stereotype `persistent` to `Car` (`c@1`), `a` rebases to D2
/// (`a@71`); none has delivered the others. Every order each replica can
/// deliver the others' streams in: 3 on `a`, 2 on `b`, 3 on `c`, 8 replica
/// schedules, which cover SI.12 C3's 18 combined orders since a replica's
/// result depends on its own order alone. Every replica that
/// has the rebase reads the running example under D2 with each delivered
/// edit in it, `name` one register value; at the end `Doors`, abstract,
/// `persistent`, on all three.
#[test]
fn si14_rb_late_operations_are_translated_and_kept_in_every_order() {
    let d1 = Uml::new(D1);
    let schedule = Schedule {
        name: "RB",
        plan: vec![
            rebase_to("R", A, Rev::D2, &[]),
            plain_edit("b1", B, d1.name_char(DOOR, 4, 's')),
            plain_edit("b2", B, d1.leaf(DOOR, "abstract", LeafOp::Enable)),
            plain_edit(
                "c1",
                C,
                d1.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent"))),
            ),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            if has(delivered, "b1") {
                set(&mut doc, "Door", "name", json!("Doors"));
            }
            if has(delivered, "b2") {
                set(&mut doc, "Door", "abstract", json!(true));
            }
            if has(delivered, "c1") {
                add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
            }
            vec![doc]
        }),
        last: None,
    };
    assert_eq!(
        run(&schedule),
        3 + 2 + 3,
        "RB: the registered number of replica schedules"
    );
}

// ---------------------------------------------------------------------------
// RC, register semantics after text
// ---------------------------------------------------------------------------

/// RC: a late character edit and writes after the rebase on `Door`'s name,
/// the winner the Lamport stamps give (the register's rule on the
/// translated writes), on every replica, in every order.
///
/// - RC1: `b` appends `s` at stamp 71; `a` rebases (71) and writes `Gate`
///   (72). `Gate`.
/// - RC2, the older name returns: `b` writes four stereotypes on `Sedan`
///   (71 to 74) and then appends `s` (75); `a` rebases and writes `Gate`
///   (72). `Doors`, on every replica, `a` included, which read `Gate` until
///   `b@5` arrived.
/// - RC3, a tie: `b` appends `s` and `c` appends `x`, both at 71; `a`
///   rebases (71). Under D2's fair register `Doors`, under D2_LWW's
///   last-writer-wins register `Doorx` (F4 holds the tie rule).
/// - RC4, the base's tag: `a` writes two stereotypes on `Sedan` (71, 72)
///   and rebases (73); `b` appends `s` (71). `Doors`: the base register is
///   tagged with `Door`'s last character event (`a@18`), not with the
///   rebase, so the late `b@1` wins on every replica.
#[test]
fn si14_rc_a_name_turned_register_keeps_the_write_its_stamps_give() {
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let mut runs = 0;

    let rc1 = Schedule {
        name: "RC1",
        plan: vec![
            rebase_to("R", A, Rev::D2, &[]),
            plain_edit("a72", A, d2.rename_door("Gate")),
            plain_edit("b1", B, d1.name_char(DOOR, 4, 's')),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            let name = match rev {
                Rev::D1 if has(delivered, "b1") => "Doors",
                Rev::D1 => "Door",
                _ if has(delivered, "a72") => "Gate",
                _ if has(delivered, "b1") => "Doors",
                _ => "Door",
            };
            set(&mut doc, "Door", "name", json!(name));
            vec![doc]
        }),
        last: None,
    };
    runs += run(&rc1);

    let mut plan = vec![
        rebase_to("R", A, Rev::D2, &[]),
        plain_edit("a72", A, d2.rename_door("Gate")),
    ];
    for (name, tag) in [("b1", "s1"), ("b2", "s2"), ("b3", "s3"), ("b4", "s4")] {
        plan.push(plain_edit(
            name,
            B,
            d1.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text(tag))),
        ));
    }
    plan.push(plain_edit("b5", B, d1.name_char(DOOR, 4, 's')));
    let rc2 = Schedule {
        name: "RC2",
        plan,
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            for (name, tag) in [("b1", "s1"), ("b2", "s2"), ("b3", "s3"), ("b4", "s4")] {
                if has(delivered, name) {
                    add_to_set(&mut doc, "Sedan", "stereotype", &[tag]);
                }
            }
            let name = match rev {
                Rev::D1 if has(delivered, "b5") => "Doors",
                Rev::D1 => "Door",
                _ if has(delivered, "b5") => "Doors",
                _ if has(delivered, "a72") => "Gate",
                _ => "Door",
            };
            set(&mut doc, "Door", "name", json!(name));
            vec![doc]
        }),
        last: None,
    };
    runs += run(&rc2);

    for (rev, tie) in [(Rev::D2, "Doors"), (Rev::Lww, "Doorx")] {
        let rc3 = Schedule {
            name: if rev == Rev::D2 { "RC3" } else { "RC3-lww" },
            plan: vec![
                rebase_to("R", A, rev, &[]),
                plain_edit("b1", B, d1.name_char(DOOR, 4, 's')),
                plain_edit("c1", C, d1.name_char(DOOR, 4, 'x')),
            ],
            doc: Box::new(move |in_force, delivered| {
                let doc = in_force.running_example();
                let names: Vec<&str> = match (in_force, has(delivered, "b1"), has(delivered, "c1"))
                {
                    (_, false, false) => vec!["Door"],
                    (_, true, false) => vec!["Doors"],
                    (_, false, true) => vec!["Doorx"],
                    // Both under D1's text: the two characters, in the
                    // order the text gives them; not this item's claim.
                    (Rev::D1, true, true) => vec!["Doorsx", "Doorxs"],
                    (_, true, true) => vec![tie],
                };
                names
                    .into_iter()
                    .map(|name| {
                        let mut doc = doc.clone();
                        set(&mut doc, "Door", "name", json!(name));
                        doc
                    })
                    .collect()
            }),
            last: None,
        };
        runs += run(&rc3);
    }

    let rc4 = Schedule {
        name: "RC4",
        plan: vec![
            plain_edit(
                "a71",
                A,
                d1.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("t1"))),
            ),
            plain_edit(
                "a72",
                A,
                d1.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("t2"))),
            ),
            rebase_to("R", A, Rev::D2, &[]),
            plain_edit("b1", B, d1.name_char(DOOR, 4, 's')),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            for (name, tag) in [("a71", "t1"), ("a72", "t2")] {
                if has(delivered, name) {
                    add_to_set(&mut doc, "Sedan", "stereotype", &[tag]);
                }
            }
            let name = if has(delivered, "b1") {
                "Doors"
            } else {
                "Door"
            };
            set(&mut doc, "Door", "name", json!(name));
            vec![doc]
        }),
        last: None,
    };
    runs += run(&rc4);
    // RC1 3 + 1 + 1, RC2 21 + 1 + 1, RC3 2 + 2 + 2 twice, RC4 1 + 4 + 1.
    assert_eq!(
        runs,
        5 + 23 + 12 + 6,
        "RC: the registered number of replica schedules"
    );
}

// ---------------------------------------------------------------------------
// RD, removal
// ---------------------------------------------------------------------------

/// RD: removals the request names.
///
/// - RD1, D2_DROP with `drops` `ModelElement.taggedValue` and `TaggedValue`:
///   `a` rebases; `b` appends `s` to `Door`'s name (kept, translated); `c`
///   inserts `0` into `wheels`'s value (`c@1`, a path through
///   `ModelElement.taggedValue`). Every replica with the rebase hides
///   `wheels` and every `taggedValue`, lists `wheels` as hidden and, once it
///   holds `c@1`, `c@1` as dropped by `ModelElement.taggedValue`, whichever
///   came first.
/// - RD2, D2_NOASSOC with `drops` `Association`: `a` rebases; `b` adds the
///   stereotype `x` to `hasDoors` (`b@1`, a path naming `Association`,
///   dropped); `c` inserts a class at `ownedElements[5]`, after `hasDoors`,
///   and types its name `Wheel` (`c@1` to `c@6`, positions resolved against
///   their own versions, kept). Every replica with the rebase hides
///   `hasDoors` and reads `Wheel` after `String`; at the end of every run
///   the replica under test writes, under D2_NOASSOC, the stereotype `w`
///   on `ownedElements[4]`, which `D2`'s read serves as `Wheel`.
#[test]
fn si14_rd_a_removal_named_in_drops_hides_and_reports_and_nothing_else() {
    let d1 = Uml::new(D1);
    let mut runs = 0;
    let rd1 = Schedule {
        name: "RD1",
        plan: vec![
            rebase_to("R", A, Rev::Drop, &DROPS_TAGGED),
            plain_edit("b1", B, d1.name_char(DOOR, 4, 's')),
            edit(
                "c1",
                C,
                vec![(Rev::Drop, "ModelElement.taggedValue")],
                move |_| {
                    Uml::new(D1).leaf(CAR_WHEELS, "value", LeafOp::InsertChar { pos: 1, ch: '0' })
                },
            ),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            if has(delivered, "b1") {
                set(&mut doc, "Door", "name", json!("Doors"));
            }
            if rev == Rev::D1 && has(delivered, "c1") {
                set(&mut doc, "wheels", "value", json!("40"));
            }
            vec![doc]
        }),
        last: None,
    };
    runs += run(&rd1);

    let wheel_name = ["c2", "c3", "c4", "c5", "c6"];
    let mut plan = vec![
        rebase_to("R", A, Rev::Noassoc, &DROPS_ASSOC),
        edit("b1", B, vec![(Rev::Noassoc, "Association")], move |_| {
            Uml::new(D1).leaf(HAS_DOORS, "stereotype", LeafOp::Add(Scalar::text("x")))
        }),
        plain_edit("c1", C, d1.insert(At::Root, "ownedElements", 5, "Class")),
    ];
    for (pos, (name, ch)) in wheel_name.iter().zip("Wheel".chars()).enumerate() {
        plan.push(plain_edit(
            name,
            C,
            d1.name_char(At::Owned(5, "Class"), pos, ch),
        ));
    }
    let typed = move |delivered: &BTreeSet<&'static str>| -> String {
        wheel_name
            .iter()
            .zip("Wheel".chars())
            .filter(|(name, _)| has(delivered, name))
            .map(|(_, ch)| ch)
            .collect()
    };
    let rd2 = Schedule {
        name: "RD2",
        plan,
        doc: Box::new(move |rev, delivered| {
            let mut doc = rev.running_example();
            if rev == Rev::D1 && has(delivered, "b1") {
                add_to_set(&mut doc, "hasDoors", "stereotype", &["x"]);
            }
            if has(delivered, "c1") {
                push_owned(&mut doc, wheel(&typed(delivered), &[]));
            }
            vec![doc]
        }),
        last: Some(Box::new(|_, _| {
            let op = Uml::new(D2_NOASSOC).leaf(
                At::Owned(4, "Class"),
                "stereotype",
                LeafOp::Add(Scalar::text("w")),
            );
            let mut doc = Rev::Noassoc.running_example();
            push_owned(&mut doc, wheel("Wheel", &["w"]));
            (op, doc)
        })),
    };
    runs += run(&rd2);
    // RD1 2 + 2 + 2; RD2 7 + 7 + 2.
    assert_eq!(
        runs,
        6 + 16,
        "RD: the registered number of replica schedules"
    );
}

// ---------------------------------------------------------------------------
// RE, refusals
// ---------------------------------------------------------------------------

/// RE: revisions a rebase refuses, each naming the feature, the one copy
/// accepts naming copy too; nothing is written, and `m` stays writable
/// under D1 on every replica.
#[test]
fn si14_re_a_revision_rebase_cannot_translate_is_refused_by_name() {
    let mut base = base();
    let cases: [(&str, &str, &[&str], bool); 4] = [
        (
            "D2_RENAME without drops",
            D2_RENAME,
            &["Class.abstract"],
            false,
        ),
        (
            "D2_FLAG (enable-wins to disable-wins)",
            D2_FLAG,
            &["Class.abstract"],
            true,
        ),
        (
            "D2_KIND (reference by text to by identity)",
            D2_KIND,
            &["Packageable.owner"],
            false,
        ),
        (
            "D2_DROP without drops",
            D2_DROP,
            &["ModelElement.taggedValue"],
            false,
        ),
    ];
    for (what, text, names, copy) in cases {
        let before = identified(&base.reps[A]);
        match rebase(base.reps[A].state(), text, &[]) {
            Ok(op) => panic!("RE (M19): the rebase to {what} was accepted: {op:?}"),
            Err(why) => {
                let why = why.to_string();
                for name in names {
                    assert!(
                        why.contains(name),
                        "RE (M19): the refusal of {what} does not name `{name}`: {why}"
                    );
                }
                if copy {
                    assert!(
                        why.to_lowercase().contains("copy"),
                        "RE (M19): copy accepts {what}, and the refusal does not say so: {why}"
                    );
                }
                println!("SI14-RE: {what} refused: {why}");
            }
        }
        assert_eq!(
            identified(&base.reps[A]),
            before,
            "RE: {what}: the model moved"
        );
        assert_eq!(
            status14(&base.reps[A].state().migration()),
            quiet(),
            "RE: {what}: a migration is named after the refusal"
        );
    }
    let edit = Uml::new(D1).leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("final")));
    let message = send(&mut base.reps[A], edit, "RE: an edit after four refusals");
    base.reps[B].receive(message.clone());
    base.reps[C].receive(message);
    let mut doc = Rev::D1.running_example();
    add_to_set(&mut doc, "Sedan", "stereotype", &["final"]);
    for (index, replica) in base.reps.iter().enumerate() {
        check(
            replica,
            &base,
            &Want::Live {
                rev: Rev::D1,
                docs: vec![doc.clone()],
                status: quiet(),
            },
            &format!("RE on {}, after the refusals and an edit", IDS[index]),
        );
    }
}

// ---------------------------------------------------------------------------
// RG, concurrent migrations, one order
// ---------------------------------------------------------------------------

/// RG: `a` rebases to D2 and `b` to D2_DROP, neither having the other's
/// rebase, each then writing an edit under its own revision.
///
/// - RG-tie: equal stamps (71, 71): `a`'s, the least origin, is kept; `b`'s
///   edit (`final` on `Door`) is discarded, `b`'s rebase superseded.
/// - RG-unequal: `b` first writes `x` on `Door` under D1 (71), so its rebase
///   (72) is kept; `a`'s edit (`Car.isLeaf`) is discarded, `a`'s rebase
///   superseded, `b@1` is in the kept rebase's base.
///
/// On every replica, in every order (6 and 10 on `c`, one each on `a` and
/// `b`), by the two operations alone and never by arrival.
#[test]
fn si14_rg_two_concurrent_rebases_are_ordered_by_one_rule() {
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let drop_uml = Uml::new(D2_DROP);
    let mut runs = 0;
    let rg_tie = Schedule {
        name: "RG-tie",
        plan: vec![
            rebase_to("R1", A, Rev::D2, &[]),
            plain_edit("a72", A, d2.leaf(CAR, "isLeaf", LeafOp::Enable)),
            rebase_to("R2", B, Rev::Drop, &DROPS_TAGGED),
            plain_edit(
                "b2",
                B,
                drop_uml.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("final"))),
            ),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            match rev {
                Rev::D2 if has(delivered, "a72") => set(&mut doc, "Car", "isLeaf", json!(true)),
                Rev::Drop if has(delivered, "b2") => {
                    add_to_set(&mut doc, "Door", "stereotype", &["final"])
                }
                _ => {}
            }
            vec![doc]
        }),
        last: None,
    };
    runs += run(&rg_tie);

    let rg_unequal = Schedule {
        name: "RG-unequal",
        plan: vec![
            rebase_to("R1", A, Rev::D2, &[]),
            plain_edit("a72", A, d2.leaf(CAR, "isLeaf", LeafOp::Enable)),
            plain_edit(
                "b1",
                B,
                d1.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("x"))),
            ),
            rebase_to("R2", B, Rev::Drop, &DROPS_TAGGED),
            plain_edit(
                "b3",
                B,
                drop_uml.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("final"))),
            ),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            if has(delivered, "b1") {
                add_to_set(&mut doc, "Door", "stereotype", &["x"]);
            }
            match rev {
                Rev::D2 if has(delivered, "a72") => set(&mut doc, "Car", "isLeaf", json!(true)),
                Rev::Drop if has(delivered, "b3") => {
                    add_to_set(&mut doc, "Door", "stereotype", &["final"])
                }
                _ => {}
            }
            vec![doc]
        }),
        last: None,
    };
    runs += run(&rg_unequal);
    // RG-tie 1 + 1 + 6, RG-unequal 1 + 1 + 10.
    assert_eq!(
        runs,
        8 + 12,
        "RG: the registered number of replica schedules"
    );
}

/// RG with both strategies: `a` cuts `m` (copy to `si14-copy`, SI.12) and
/// `b` rebases it to D2_DROP, neither having the other's operation.
///
/// - Tie (71, 71): the cut, `a`'s, is kept: `m` frozen at `a`'s reads,
///   under D1, `b`'s rebase and its edit refused (SI.12 M5) and the rebase
///   listed as superseded; a local edit refused naming the successor.
/// - Unequal (`b` writes `x` under D1 first; 72 against 71): the rebase is
///   kept: `m` under D2_DROP, writable on every replica, `a` included, the
///   cut's successor listed under `superseded`.
#[test]
fn si14_rg_a_cut_and_a_rebase_are_ordered_by_the_same_rule() {
    let d1 = Uml::new(D1);
    let drop_uml = Uml::new(D2_DROP);
    let mut runs = 0;
    let tie = Schedule {
        name: "RG-copy-tie",
        plan: vec![
            cut("C", A),
            rebase_to("R", B, Rev::Drop, &DROPS_TAGGED),
            plain_edit(
                "b2",
                B,
                drop_uml.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("final"))),
            ),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            if rev == Rev::Drop && has(delivered, "b2") {
                add_to_set(&mut doc, "Door", "stereotype", &["final"]);
            }
            vec![doc]
        }),
        last: None,
    };
    runs += run(&tie);
    // The read-only half of a kept cut, once everything is delivered.
    {
        let mut base = base();
        let c = (cut("C", A).make)(&base.reps[A]);
        let from_a = send(&mut base.reps[A], c, "the cut");
        let r = rebase_op(&base.reps[B], Rev::Drop, &DROPS_TAGGED);
        let from_b = send(&mut base.reps[B], r, "the rebase");
        base.reps[A].receive(from_b.clone());
        base.reps[B].receive(from_a.clone());
        base.reps[C].receive(from_b);
        base.reps[C].receive(from_a);
        for (index, replica) in base.reps.iter_mut().enumerate() {
            match replica.send(d1.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("late")))) {
                Ok(_) => panic!(
                    "RG-copy-tie (M22, SI.12 M3) on {}: an edit of the model a kept cut froze \
                     was accepted",
                    IDS[index]
                ),
                Err(why) => assert!(
                    why.to_string().contains(COPY),
                    "RG-copy-tie (M22, SI.12 M3) on {}: the refusal does not name {COPY}: {why}",
                    IDS[index]
                ),
            }
        }
    }

    let unequal = Schedule {
        name: "RG-copy-unequal",
        plan: vec![
            cut("C", A),
            plain_edit(
                "b1",
                B,
                d1.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("x"))),
            ),
            rebase_to("R", B, Rev::Drop, &DROPS_TAGGED),
            plain_edit(
                "b3",
                B,
                drop_uml.leaf(DOOR, "stereotype", LeafOp::Add(Scalar::text("final"))),
            ),
        ],
        doc: Box::new(|rev, delivered| {
            let mut doc = rev.running_example();
            if has(delivered, "b1") {
                add_to_set(&mut doc, "Door", "stereotype", &["x"]);
            }
            if rev == Rev::Drop && has(delivered, "b3") {
                add_to_set(&mut doc, "Door", "stereotype", &["final"]);
            }
            vec![doc]
        }),
        last: Some(Box::new(|_, _| {
            let op = Uml::new(D2_DROP).leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("z")));
            let mut doc = Rev::Drop.running_example();
            add_to_set(&mut doc, "Door", "stereotype", &["final", "x"]);
            add_to_set(&mut doc, "Sedan", "stereotype", &["z"]);
            (op, doc)
        })),
    };
    runs += run(&unequal);
    // Tie 1 + 1 + 3, unequal 1 + 1 + 4.
    assert_eq!(
        runs,
        5 + 6,
        "RG with copy: the registered number of replica schedules"
    );
}

// ---------------------------------------------------------------------------
// RH, the window
// ---------------------------------------------------------------------------

/// RH: D1 is held until the rebase is causally stable on the replica, and
/// the status names the members not yet heard from. `a` rebases to D2 and
/// `b` delivers it; `b` writes under D2 (so `a` hears `b` has it); `c`, not
/// having it, writes under D1 and `a` and `b` translate that late edit
/// while they wait for `c`; `c` delivers the rebase and `b`'s edit (stable
/// on `c` at once: it has heard from both); `c` writes under D2, and `a` and
/// `b` drop D1. No timer: nothing but deliveries moves the window.
#[test]
fn si14_rh_d1_is_held_until_the_rebase_is_stable_and_names_whom_it_waits_for() {
    let mut base = base();
    let d1 = Uml::new(D1);
    let d2 = Uml::new(D2);
    let status = |waiting: &[&str]| {
        RebaseStatus {
            origin: "a".into(),
            seq: 71,
            to: Rev::D2.digest(),
            waiting_for: waiting.iter().map(|s| s.to_string()).collect(),
            ..RebaseStatus::default()
        }
        .render()
    };
    let live = |doc: &Value, waiting: &[&str]| Want::Live {
        rev: Rev::D2,
        docs: vec![doc.clone()],
        status: status(waiting),
    };
    let mut doc = Rev::D2.running_example();

    let op = rebase_op(&base.reps[A], Rev::D2, &[]);
    let r = send(&mut base.reps[A], op, "RH: a's rebase");
    base.reps[B].receive(r.clone());
    check(
        &base.reps[A],
        &base,
        &live(&doc, &["b", "c"]),
        "RH step 1 on a",
    );
    check(&base.reps[B], &base, &live(&doc, &["c"]), "RH step 1 on b");

    let eb = send(
        &mut base.reps[B],
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hb"))),
        "RH: b's edit under D2",
    );
    base.reps[A].receive(eb.clone());
    add_to_set(&mut doc, "Sedan", "stereotype", &["hb"]);
    check(
        &base.reps[A],
        &base,
        &live(&doc, &["c"]),
        "RH step 2 on a, having heard from b",
    );
    check(&base.reps[B], &base, &live(&doc, &["c"]), "RH step 2 on b");

    let ec = send(
        &mut base.reps[C],
        d1.leaf(CAR, "stereotype", LeafOp::Add(Scalar::text("persistent"))),
        "RH: c's edit under D1, concurrent with the rebase",
    );
    base.reps[A].receive(ec.clone());
    base.reps[B].receive(ec);
    add_to_set(&mut doc, "Car", "stereotype", &["persistent"]);
    check(
        &base.reps[A],
        &base,
        &live(&doc, &["c"]),
        "RH step 3 on a, having translated c's late edit while waiting for c",
    );
    check(&base.reps[B], &base, &live(&doc, &["c"]), "RH step 3 on b");

    base.reps[C].receive(r);
    base.reps[C].receive(eb);
    check(
        &base.reps[C],
        &base,
        &live(&doc, &[]),
        "RH step 4 on c, having heard a and b deliver the rebase",
    );
    check(
        &base.reps[A],
        &base,
        &live(&doc, &["c"]),
        "RH step 4 on a, still",
    );

    let ec2 = send(
        &mut base.reps[C],
        d2.leaf(SEDAN, "stereotype", LeafOp::Add(Scalar::text("hc"))),
        "RH: c's edit under D2",
    );
    base.reps[A].receive(ec2.clone());
    base.reps[B].receive(ec2);
    add_to_set(&mut doc, "Sedan", "stereotype", &["hc"]);
    for (index, replica) in base.reps.iter().enumerate() {
        check(
            replica,
            &base,
            &live(&doc, &[]),
            &format!("RH step 5 on {}, the rebase stable everywhere", IDS[index]),
        );
    }
    assert_eq!(identified(&base.reps[A]), identified(&base.reps[B]));
    assert_eq!(identified(&base.reps[A]), identified(&base.reps[C]));
    println!(
        "SI14-RH: D1 held while a member had not been heard from, dropped once stable, on a, b and c"
    );
}
