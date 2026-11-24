//! NW.31c oracle: the toy replica and the port-level contract suite of
//! moirai-netstack-testkit (vault spec/items/NW.31c.md).
//!
//! Three parts. (A) every contract case passes on the toy. (B) every case can
//! fail: a wrapper around the toy changes one rule and the case must panic.
//! (C) the toy checked directly against hand-computed answers.

use std::panic::{AssertUnwindSafe, catch_unwind};

use moirai_netstack::{Opaque, Progress, ReplicaFactory, ReplicaPort};
use moirai_netstack_testkit::contract::{self, Inputs};
use moirai_netstack_testkit::{ToyFactory, ToyReplica};
use serde_json::{Value, json};

fn inputs() -> Inputs {
    Inputs {
        ops: ["red", "green", "blue", "amber", "teal", "plum"].map(|s| json!(s)).to_vec(),
        refused_op: json!(42),
        malformed_remote: json!(42),
    }
}

type BentCase = fn(&mut BentFactory, &Inputs);

// ---- (A) the suite passes on the toy -------------------------------------

#[test]
fn toy_passes_exchange_converges() {
    contract::exchange_converges(&mut ToyFactory::default(), &inputs());
}
#[test]
fn toy_passes_refusal_with_cause() {
    contract::refusal_with_cause(&mut ToyFactory::default(), &inputs());
}
#[test]
fn toy_passes_has_history() {
    contract::has_history(&mut ToyFactory::default(), &inputs());
}
#[test]
fn toy_passes_batch_stops_at_first_refusal() {
    contract::batch_stops_at_first_refusal(&mut ToyFactory::default(), &inputs());
}
#[test]
fn toy_passes_port_defaults() {
    contract::port_defaults(&mut ToyFactory::default(), &inputs());
}
#[test]
fn toy_passes_progress_counts() {
    contract::progress_counts(&mut ToyFactory::default(), &inputs());
}

// ---- (B) each case can fail -----------------------------------------------

/// One rule changed around a correct replica.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Bend {
    ApplyDropped,
    OpsSinceEmpty,
    SubmitDropped,
    RefusedSubmitAccepted,
    RefusedSubmitStillApplies,
    MalformedAccepted,
    ForeignAccepted,
    EmptyCause,
    HistoryNever,
    HistoryAlways,
    HistoryBySender,
    BatchGoesOn,
    BatchNoEvents,
    IdentifiedDiffers,
    SnapshotOffered,
    ProgressZero,
    DeliveredCountsApplies,
    DeliveredCountsReapply,
    PendingLeft,
}

struct Bent {
    inner: ToyReplica,
    bend: Bend,
    malformed: Value,
    senders: Vec<String>,
    seen: Vec<Value>,
    extra: u64,
}

impl ReplicaPort for Bent {
    fn replica_id(&self) -> &str {
        self.inner.replica_id()
    }
    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        if self.bend == Bend::SubmitDropped {
            return Ok(Value::Null);
        }
        match (self.bend, self.inner.submit(op)) {
            (Bend::RefusedSubmitAccepted, Err(_)) => Ok(Value::Null),
            (Bend::RefusedSubmitStillApplies, Err(e)) => {
                let _ = self.inner.submit(json!("nw31c-spare"));
                Err(e)
            }
            (Bend::EmptyCause, Err(_)) => Err(String::new()),
            (_, r) => r,
        }
    }
    fn apply_remote(&mut self, from: &str, ops: Opaque) -> Result<(), String> {
        self.senders.push(from.to_string());
        let again = self.seen.contains(&ops);
        self.seen.push(ops.clone());
        match self.bend {
            Bend::ApplyDropped => return Ok(()),
            Bend::DeliveredCountsApplies => self.extra += 1,
            Bend::DeliveredCountsReapply if again => self.extra += 1,
            _ => {}
        }
        let malformed = ops == self.malformed;
        match (self.bend, self.inner.apply_remote(from, ops)) {
            (Bend::MalformedAccepted, Err(_)) if malformed => Ok(()),
            (Bend::ForeignAccepted, Err(_)) if !malformed => Ok(()),
            (Bend::EmptyCause, Err(_)) => Err(String::new()),
            (_, r) => r,
        }
    }
    fn frontier(&self) -> Opaque {
        self.inner.frontier()
    }
    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
        if self.bend == Bend::OpsSinceEmpty {
            let own = self.inner.frontier();
            return self.inner.ops_since(own);
        }
        self.inner.ops_since(frontier)
    }
    fn progress(&self) -> Progress {
        let p = self.inner.progress();
        match self.bend {
            Bend::ProgressZero => Progress::default(),
            Bend::PendingLeft => Progress { pending: p.pending + 1, ..p },
            _ => Progress { delivered: p.delivered + self.extra, ..p },
        }
    }
    fn members(&self) -> Vec<String> {
        self.inner.members()
    }
    fn has_history_from(&self, peer: &str) -> bool {
        match self.bend {
            Bend::HistoryNever => false,
            Bend::HistoryAlways => true,
            Bend::HistoryBySender => self.senders.iter().any(|s| s == peer),
            _ => self.inner.has_history_from(peer),
        }
    }
    fn read_state(&self) -> Value {
        self.inner.read_state()
    }
    fn read_identified(&self) -> Value {
        match self.bend {
            Bend::IdentifiedDiffers => Value::Null,
            _ => self.inner.read_identified(),
        }
    }
    fn snapshot_for(&mut self, requester: &str) -> Option<Result<Opaque, String>> {
        match self.bend {
            Bend::SnapshotOffered => Some(Ok(Value::Null)),
            _ => self.inner.snapshot_for(requester),
        }
    }
    fn submit_batch(&mut self, ops: Vec<Value>) -> (usize, Option<String>, Vec<Opaque>) {
        match self.bend {
            Bend::BatchGoesOn => {
                let (mut events, mut refusal) = (Vec::new(), None);
                for op in ops {
                    match self.submit(op) {
                        Ok(e) => events.push(e),
                        Err(e) => refusal = refusal.or(Some(e)),
                    }
                }
                (events.len(), refusal, events)
            }
            Bend::BatchNoEvents => {
                let (n, refusal, _) = self.inner.submit_batch(ops);
                (n, refusal, Vec::new())
            }
            _ => self.inner.submit_batch(ops),
        }
    }
}

struct BentFactory {
    inner: ToyFactory,
    bend: Bend,
}

impl ReplicaFactory for BentFactory {
    type Replica = Bent;
    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Bent {
        Bent {
            inner: self.inner.open(log, self_id, members),
            bend: self.bend,
            malformed: inputs().malformed_remote,
            senders: Vec::new(),
            seen: Vec::new(),
            extra: 0,
        }
    }
}

fn must_fail(case: BentCase, bend: Bend) {
    let mut f = BentFactory { inner: ToyFactory::default(), bend };
    let r = catch_unwind(AssertUnwindSafe(|| case(&mut f, &inputs())));
    assert!(r.is_err(), "the case passed on a replica bent by {bend:?}");
}

macro_rules! fails {
    ($($name:ident: $case:ident, $bend:ident;)*) => {$(
        #[test]
        fn $name() {
            must_fail(contract::$case::<BentFactory>, Bend::$bend);
        }
    )*};
}

fails! {
    exchange_fails_when_apply_is_dropped: exchange_converges, ApplyDropped;
    exchange_fails_when_ops_since_sends_nothing: exchange_converges, OpsSinceEmpty;
    exchange_fails_when_submit_is_dropped: exchange_converges, SubmitDropped;
    refusal_fails_when_a_refused_op_is_accepted: refusal_with_cause, RefusedSubmitAccepted;
    refusal_fails_when_a_refused_op_still_applies: refusal_with_cause, RefusedSubmitStillApplies;
    refusal_fails_when_a_malformed_remote_is_accepted: refusal_with_cause, MalformedAccepted;
    refusal_fails_when_a_foreign_event_is_accepted: refusal_with_cause, ForeignAccepted;
    refusal_fails_when_the_cause_is_empty: refusal_with_cause, EmptyCause;
    history_fails_when_never_held: has_history, HistoryNever;
    history_fails_when_always_held: has_history, HistoryAlways;
    history_fails_when_counted_by_sender: has_history, HistoryBySender;
    batch_fails_when_it_goes_on_after_a_refusal: batch_stops_at_first_refusal, BatchGoesOn;
    batch_fails_when_events_are_lost: batch_stops_at_first_refusal, BatchNoEvents;
    defaults_fail_when_read_identified_differs: port_defaults, IdentifiedDiffers;
    defaults_fail_when_a_snapshot_is_offered: port_defaults, SnapshotOffered;
    progress_fails_when_always_zero: progress_counts, ProgressZero;
    progress_fails_when_delivered_counts_applies: progress_counts, DeliveredCountsApplies;
    progress_fails_when_a_reapply_counts: progress_counts, DeliveredCountsReapply;
    progress_fails_when_pending_is_left: progress_counts, PendingLeft;
}

// ---- (C) the toy against hand-computed answers ---------------------------

fn members() -> Vec<String> {
    ["ann", "bob", "cy"].map(String::from).to_vec()
}

/// x answers y's frontier, y applies the answer.
fn pull(y: &mut ToyReplica, x: &mut ToyReplica) {
    let answer = x.ops_since(y.frontier()).expect("ops_since");
    let from = x.replica_id().to_string();
    y.apply_remote(&from, answer).expect("apply_remote");
}

#[test]
fn toy_identity_and_fresh_state() {
    let mut f = ToyFactory::default();
    let a = f.open("log-1", "ann", &members());
    assert_eq!(a.replica_id(), "ann");
    assert_eq!(a.members(), members());
    assert_eq!(a.read_state(), json!([]));
    assert_eq!(a.progress(), Progress::default());
    assert!(!a.has_history_from("bob"));
}

#[test]
fn toy_exchange_gives_the_sorted_union_and_origin_history() {
    let mut f = ToyFactory::default();
    let (mut a, mut b, mut c) =
        (f.open("log-1", "ann", &members()), f.open("log-1", "bob", &members()), f.open("log-1", "cy", &members()));
    a.submit(json!("red")).unwrap();
    a.submit(json!("blue")).unwrap();
    b.submit(json!("green")).unwrap();
    pull(&mut b, &mut a);
    pull(&mut a, &mut b);
    let union = json!(["blue", "green", "red"]);
    assert_eq!(a.read_state(), union);
    assert_eq!(b.read_state(), union);
    assert_eq!(a.progress().delivered, 3);
    assert_eq!(b.progress().delivered, 3);
    assert_eq!(c.read_state(), json!([]));
    // cy hears only from bob, who relays ann's ops.
    pull(&mut c, &mut b);
    assert_eq!(c.read_state(), union);
    assert_eq!((c.progress().delivered, c.progress().pending), (3, 0));
    assert!(c.has_history_from("ann") && c.has_history_from("bob") && !c.has_history_from("cy"));
    // A second pull, and the whole log sent again, bring nothing new and count nothing.
    pull(&mut c, &mut b);
    let fresh = f.open("log-1", "dee", &members()).frontier();
    let whole = b.ops_since(fresh).unwrap();
    c.apply_remote("bob", whole).unwrap();
    assert_eq!(c.read_state(), union);
    assert_eq!(c.progress().delivered, 3);
}

#[test]
fn toy_refuses_a_non_string_op_a_malformed_remote_and_a_foreign_event() {
    let mut f = ToyFactory::default();
    let mut a = f.open("log-1", "ann", &members());
    let mut other = f.open("log-2", "bob", &members());
    a.submit(json!("red")).unwrap();
    for op in [json!(42), json!(null), json!(["red"]), json!({"add": "red"})] {
        let e = a.submit(op.clone()).expect_err("a non-string op is refused");
        assert!(!e.is_empty(), "refusal of {op} has a cause");
    }
    let e = a.apply_remote("bob", json!(42)).expect_err("a malformed remote is refused");
    assert!(!e.is_empty());
    other.submit(json!("green")).unwrap();
    let fresh = f.open("log-2", "cy", &members()).frontier();
    let foreign = other.ops_since(fresh).unwrap();
    let e = a.apply_remote("bob", foreign).expect_err("an event of log-2 is refused on log-1");
    assert!(!e.is_empty());
    assert_eq!(a.read_state(), json!(["red"]));
    assert_eq!(a.progress().delivered, 1);
    assert!(!a.has_history_from("bob"));
}

#[test]
fn toy_batch_applies_the_prefix_before_the_first_refusal() {
    let mut f = ToyFactory::default();
    let mut a = f.open("log-1", "ann", &members());
    let (n, refusal, events) = a.submit_batch(vec![json!("red"), json!(7), json!("blue")]);
    assert_eq!(n, 1);
    assert!(refusal.is_some_and(|r| !r.is_empty()));
    assert_eq!(events.len(), 1);
    assert_eq!(a.read_state(), json!(["red"]));
    let mut b = f.open("log-1", "bob", &members());
    b.apply_remote("ann", events[0].clone()).unwrap();
    assert_eq!(b.read_state(), json!(["red"]));
    assert!(b.has_history_from("ann"));
}

#[test]
fn toy_keeps_the_port_defaults() {
    let mut f = ToyFactory::default();
    let mut a = f.open("log-1", "ann", &members());
    a.submit(json!("red")).unwrap();
    assert_eq!(a.read_identified(), a.read_state());
    assert!(a.snapshot_for("bob").is_none());
}
