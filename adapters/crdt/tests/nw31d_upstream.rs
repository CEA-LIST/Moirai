//! NW.31d oracle: the upstream Moirai adapter behind moirai-netstack's port
//! (vault spec/items/NW.31d.md).
//!
//! Four parts. (A) the S3 contract suite's six cases pass through the adapter
//! on upstream add-wins-set replicas. (B) each case can fail there: a wrapper
//! around the adapter's replica changes one rule and the case must panic.
//! (C) three replicas with concurrent ops and one member offline converge to
//! a hand-computed add-wins state. (D) a payload whose sender is not the peer
//! it came from is refused with a cause naming both, and nothing is applied.

use std::panic::{AssertUnwindSafe, catch_unwind};

use moirai_netstack::{Opaque, Progress, ReplicaFactory, ReplicaPort};
use moirai_netstack_testkit::contract::{self, Inputs};
use moirai_netstack_crdt::awset;
use serde_json::{Value, json};

fn add(s: &str) -> Value {
    json!({ "Add": s })
}
fn remove(s: &str) -> Value {
    json!({ "Remove": s })
}

fn inputs() -> Inputs {
    Inputs {
        ops: ["red", "green", "blue", "amber", "teal", "plum"]
            .map(add)
            .to_vec(),
        refused_op: json!({ "Add": 7 }),
        malformed_remote: json!({ "nw31d": "not a payload" }),
    }
}

fn members() -> Vec<String> {
    ["ann", "bob", "cy"].map(String::from).to_vec()
}

// ---- (A) the suite passes through the adapter ------------------------------

macro_rules! passes {
    ($($name:ident: $case:ident;)*) => {$(
        #[test]
        fn $name() {
            contract::$case(&mut awset::factory(), &inputs());
        }
    )*};
}

passes! {
    upstream_passes_exchange_converges: exchange_converges;
    upstream_passes_refusal_with_cause: refusal_with_cause;
    upstream_passes_has_history: has_history;
    upstream_passes_batch_stops_at_first_refusal: batch_stops_at_first_refusal;
    upstream_passes_port_defaults: port_defaults;
    upstream_passes_progress_counts: progress_counts;
}

// ---- (B) each case can fail on the adapter ----------------------------------

/// One rule changed around the adapter's replica (NW.31c's bends, made generic).
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

struct Bent<R> {
    inner: R,
    bend: Bend,
    senders: Vec<String>,
    seen: Vec<Value>,
    extra: u64,
}

impl<R: ReplicaPort> ReplicaPort for Bent<R> {
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
                let _ = self.inner.submit(add("nw31d-spare"));
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
        let malformed = ops == inputs().malformed_remote;
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
            Bend::PendingLeft => Progress {
                pending: p.pending + 1,
                ..p
            },
            _ => Progress {
                delivered: p.delivered + self.extra,
                ..p
            },
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

struct BentFactory<F> {
    inner: F,
    bend: Bend,
}

impl<F: ReplicaFactory> ReplicaFactory for BentFactory<F> {
    type Replica = Bent<F::Replica>;
    fn open(&mut self, log: &str, self_id: &str, members: &[String]) -> Self::Replica {
        Bent {
            inner: self.inner.open(log, self_id, members),
            bend: self.bend,
            senders: Vec::new(),
            seen: Vec::new(),
            extra: 0,
        }
    }
}

fn must_fail<F: ReplicaFactory>(case: fn(&mut BentFactory<F>, &Inputs), inner: F, bend: Bend) {
    let mut f = BentFactory { inner, bend };
    let r = catch_unwind(AssertUnwindSafe(|| case(&mut f, &inputs())));
    assert!(
        r.is_err(),
        "the case passed on an adapter replica bent by {bend:?}"
    );
}

macro_rules! fails {
    ($($name:ident: $case:ident, $bend:ident;)*) => {$(
        #[test]
        fn $name() {
            must_fail(contract::$case, awset::factory(), Bend::$bend);
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

// ---- (C) three replicas, concurrent ops, one member offline -----------------

/// `to` applies `from`'s answer to `to`'s frontier, sent as from `from`.
fn pull<R: ReplicaPort>(to: &mut R, from: &mut R) {
    let answer = from.ops_since(to.frontier()).expect("ops_since answers");
    let id = from.replica_id().to_string();
    to.apply_remote(&id, answer)
        .expect("a genuine answer is accepted");
}

fn submit<R: ReplicaPort>(r: &mut R, op: Value) -> Opaque {
    r.submit(op).expect("a valid op is accepted")
}

#[test]
fn three_replicas_converge_under_concurrent_ops() {
    let mut f = awset::factory();
    let (mut ann, mut bob, mut cy) = (
        f.open("nw31d-log", "ann", &members()),
        f.open("nw31d-log", "bob", &members()),
        f.open("nw31d-log", "cy", &members()),
    );
    // cy is offline until the last step.
    let e1 = submit(&mut ann, add("red"));
    submit(&mut bob, add("green"));
    submit(&mut bob, add("teal"));
    bob.apply_remote("ann", e1).expect("ann's event from ann");
    pull(&mut ann, &mut bob);
    assert_eq!(ann.read_state(), json!(["green", "red", "teal"]));
    // Concurrent: ann removes red and teal; bob adds red again; cy, offline,
    // adds blue and removes green, which it never saw.
    let e4 = submit(&mut ann, remove("red"));
    let e5 = submit(&mut ann, remove("teal"));
    submit(&mut bob, add("red"));
    submit(&mut cy, add("blue"));
    submit(&mut cy, remove("green"));
    // bob receives ann's two events in reverse order: the second waits.
    bob.apply_remote("ann", e5)
        .expect("ann's later event from ann");
    assert_eq!(
        bob.progress().pending,
        1,
        "an event ahead of its cause waits"
    );
    assert_eq!(
        bob.read_state(),
        json!(["green", "red", "teal"]),
        "a waiting event is not applied"
    );
    bob.apply_remote("ann", e4)
        .expect("ann's earlier event from ann");
    assert_eq!(bob.progress().pending, 0);
    pull(&mut ann, &mut bob);
    // cy comes back: it hears bob, ann hears cy, bob hears cy's ops through ann.
    pull(&mut cy, &mut bob);
    pull(&mut ann, &mut cy);
    pull(&mut bob, &mut ann);
    // Add wins over the concurrent remove of red; teal's remove saw its add;
    // cy's remove of green saw no add of green.
    let expected = json!(["blue", "green", "red"]);
    for r in [&ann, &bob, &cy] {
        let id = r.replica_id();
        assert_eq!(r.read_state(), expected, "{id} reads the add-wins state");
        assert_eq!(
            r.progress().delivered,
            8,
            "{id} delivered all eight ops once"
        );
        assert_eq!(r.progress().pending, 0, "{id} has nothing pending");
        for m in members() {
            assert!(r.has_history_from(&m), "{id} has history from {m}");
        }
    }
}

// ---- (D) a payload whose sender is not the peer it came from ----------------

/// `r` refuses `payload` from `from`, naming `claimed` and `from`, and stays as it was.
fn refused_forged<R: ReplicaPort>(r: &mut R, from: &str, payload: Opaque, claimed: &str) {
    let (state, progress) = (r.read_state(), r.progress());
    let cause = r
        .apply_remote(from, payload)
        .expect_err("a payload whose sender is not its origin is refused");
    assert!(
        cause.contains(claimed) && cause.contains(from),
        "the cause names the claimed sender {claimed} and the origin {from}: {cause}"
    );
    assert_eq!(r.read_state(), state, "a refused payload changes no state");
    assert_eq!(r.progress(), progress, "a refused payload changes no count");
    for m in members() {
        assert!(
            !r.has_history_from(&m),
            "a refused payload gives no history from {m}"
        );
    }
}

#[test]
fn forged_origin_single_event_is_refused() {
    let mut f = awset::factory();
    let mut ann = f.open("nw31d-log", "ann", &members());
    let mut cy = f.open("nw31d-log", "cy", &members());
    let event = submit(&mut ann, add("red"));
    refused_forged(&mut cy, "bob", event.clone(), "ann");
    cy.apply_remote("ann", event)
        .expect("the same event from ann is accepted");
    assert_eq!(cy.read_state(), json!(["red"]));
}

#[test]
fn forged_origin_relayed_batch_is_refused() {
    let mut f = awset::factory();
    let mut ann = f.open("nw31d-log", "ann", &members());
    let mut bob = f.open("nw31d-log", "bob", &members());
    let mut cy = f.open("nw31d-log", "cy", &members());
    submit(&mut ann, add("red"));
    pull(&mut bob, &mut ann);
    submit(&mut bob, add("green"));
    // bob's answer holds ann's event and bob's: it is bob's, not ann's.
    let answer = bob.ops_since(cy.frontier()).expect("ops_since answers");
    refused_forged(&mut cy, "ann", answer.clone(), "bob");
    cy.apply_remote("bob", answer)
        .expect("the same answer from bob is accepted");
    assert_eq!(cy.read_state(), json!(["green", "red"]));
    assert!(cy.has_history_from("ann") && cy.has_history_from("bob"));
}

#[test]
fn forged_origin_ops_since_answer_is_refused() {
    let mut f = awset::factory();
    let mut ann = f.open("nw31d-log", "ann", &members());
    let mut cy = f.open("nw31d-log", "cy", &members());
    submit(&mut ann, add("red"));
    submit(&mut ann, add("blue"));
    let answer = ann.ops_since(cy.frontier()).expect("ops_since answers");
    refused_forged(&mut cy, "bob", answer.clone(), "ann");
    cy.apply_remote("ann", answer)
        .expect("the same answer from ann is accepted");
    assert_eq!(cy.read_state(), json!(["blue", "red"]));
}
