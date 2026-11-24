//! The port-level contract suite (NW.31c). Each case is generic over a
//! [`ReplicaFactory`] and panics on a violation; it reads only the port.

use moirai_netstack::{ReplicaFactory, ReplicaPort};
use serde_json::Value;

const LOG: &str = "nw31c-log";
const LOG_OTHER: &str = "nw31c-other-log";

/// What a case feeds the replica it is given.
pub struct Inputs {
    /// At least six distinct valid local ops; the cases use them disjointly.
    pub ops: Vec<Value>,
    /// A local op the replica refuses.
    pub refused_op: Value,
    /// A remote input the replica refuses as malformed.
    pub malformed_remote: Value,
}

fn members() -> Vec<String> {
    ["ann", "bob", "cy"].map(String::from).to_vec()
}

fn open<F: ReplicaFactory>(factory: &mut F, log: &str, id: &str) -> F::Replica {
    factory.open(log, id, &members())
}

fn submit<R: ReplicaPort>(replica: &mut R, op: &Value) {
    replica
        .submit(op.clone())
        .expect("a valid local op is accepted");
}

/// `to` takes `from`'s answer to `to`'s frontier.
fn pull<R: ReplicaPort>(to: &mut R, from: &mut R) {
    let answer = from.ops_since(to.frontier()).expect("ops_since answers");
    let id = from.replica_id().to_string();
    to.apply_remote(&id, answer)
        .expect("a same-log answer is accepted");
}

/// Two replicas submit disjoint ops, exchange both ways only through
/// `frontier` and `ops_since`, and converge on something new.
pub fn exchange_converges<F: ReplicaFactory>(factory: &mut F, inputs: &Inputs) {
    let mut a = open(factory, LOG, "ann");
    let mut b = open(factory, LOG, "bob");
    let fresh = open(factory, LOG, "dee").read_state();
    for (i, op) in inputs.ops.iter().enumerate() {
        submit(if i % 2 == 0 { &mut a } else { &mut b }, op);
    }
    pull(&mut b, &mut a);
    pull(&mut a, &mut b);
    assert_eq!(a.read_state(), b.read_state(), "the replicas converge");
    assert_ne!(a.read_state(), fresh, "the exchange delivered something");
}

/// A refused submit, a malformed remote input and another log's answer are
/// each refused with a cause and change nothing.
pub fn refusal_with_cause<F: ReplicaFactory>(factory: &mut F, inputs: &Inputs) {
    let mut a = open(factory, LOG, "ann");
    submit(&mut a, &inputs.ops[0]);
    let state = a.read_state();
    let progress = a.progress();

    let cause = a
        .submit(inputs.refused_op.clone())
        .expect_err("a refused op is refused");
    assert!(!cause.is_empty(), "the refusal has a cause");
    assert_eq!(a.read_state(), state, "a refused op changes nothing");
    assert_eq!(a.progress(), progress, "a refused op changes no count");

    let cause = a
        .apply_remote("bob", inputs.malformed_remote.clone())
        .expect_err("a malformed remote input is refused");
    assert!(!cause.is_empty(), "the refusal has a cause");
    assert_eq!(
        a.read_state(),
        state,
        "a malformed remote input changes nothing"
    );
    assert_eq!(
        a.progress(),
        progress,
        "a malformed remote input changes no count"
    );

    let mut other = open(factory, LOG_OTHER, "bob");
    submit(&mut other, &inputs.ops[1]);
    let fresh = open(factory, LOG_OTHER, "cy");
    let foreign = other
        .ops_since(fresh.frontier())
        .expect("ops_since answers");
    let cause = a
        .apply_remote("bob", foreign)
        .expect_err("another log's answer is refused");
    assert!(!cause.is_empty(), "the refusal has a cause");
    assert_eq!(
        a.read_state(),
        state,
        "another log's answer changes nothing"
    );
    assert_eq!(
        a.progress(),
        progress,
        "another log's answer changes no count"
    );
}

/// History follows the op's origin, through a relay that originated nothing.
pub fn has_history<F: ReplicaFactory>(factory: &mut F, inputs: &Inputs) {
    let mut r1 = open(factory, LOG, "ann");
    let mut r2 = open(factory, LOG, "bob");
    let mut r3 = open(factory, LOG, "cy");
    for member in members() {
        assert!(
            !r3.has_history_from(&member),
            "a fresh replica has no history"
        );
    }
    submit(&mut r1, &inputs.ops[0]);
    pull(&mut r2, &mut r1);
    pull(&mut r3, &mut r2);
    assert!(
        r3.has_history_from("ann"),
        "history follows the origin through a relay"
    );
    assert!(!r3.has_history_from("bob"), "the relay originated nothing");
    assert!(
        !r3.has_history_from("cy"),
        "the receiver originated nothing itself"
    );
}

/// `submit_batch` applies the prefix before its first refusal and stops there.
pub fn batch_stops_at_first_refusal<F: ReplicaFactory>(factory: &mut F, inputs: &Inputs) {
    let mut a = open(factory, LOG, "ann");
    let mut alone = open(factory, LOG, "ann");
    submit(&mut alone, &inputs.ops[0]);
    let batch = vec![
        inputs.ops[0].clone(),
        inputs.refused_op.clone(),
        inputs.ops[1].clone(),
    ];
    let (applied, refusal, events) = a.submit_batch(batch);
    assert_eq!(applied, 1, "the batch stops at the refused op");
    let refusal = refusal.expect("the batch reports its refusal");
    assert!(!refusal.is_empty(), "the refusal has a cause");
    assert_eq!(events.len(), 1, "the batch gives the applied op's event");
    assert_eq!(a.read_state(), alone.read_state(), "the prefix is applied");
    let mut fresh = open(factory, LOG, "bob");
    fresh
        .apply_remote("ann", events[0].clone())
        .expect("the event is accepted");
    assert_eq!(
        fresh.read_state(),
        a.read_state(),
        "the event carries the applied prefix"
    );
}

/// The port defaults an adapter may keep are the ones the suite expects.
pub fn port_defaults<F: ReplicaFactory>(factory: &mut F, inputs: &Inputs) {
    let mut a = open(factory, LOG, "ann");
    submit(&mut a, &inputs.ops[0]);
    assert_eq!(
        a.read_identified(),
        a.read_state(),
        "read_identified defaults to read_state"
    );
    assert!(
        a.snapshot_for("bob").is_none(),
        "the replica offers no snapshot"
    );
}

/// `delivered` counts each distinct op once, local or applied, and nothing
/// is left pending.
pub fn progress_counts<F: ReplicaFactory>(factory: &mut F, inputs: &Inputs) {
    let mut a = open(factory, LOG, "ann");
    assert_eq!(
        a.progress().delivered,
        0,
        "a fresh replica delivered nothing"
    );
    let local = &inputs.ops[..3];
    for op in local {
        submit(&mut a, op);
    }
    let p = a.progress();
    assert_eq!(
        p.delivered,
        local.len() as u64,
        "delivered counts local ops"
    );
    assert_eq!(p.pending, 0, "nothing is pending");

    let mut b = open(factory, LOG, "bob");
    let answer = a.ops_since(b.frontier()).expect("ops_since answers");
    b.apply_remote("ann", answer.clone())
        .expect("the answer is accepted");
    let p = b.progress();
    assert_eq!(
        p.delivered,
        local.len() as u64,
        "delivered counts applied ops"
    );
    assert_eq!(p.pending, 0, "nothing is pending after the exchange");
    let state = b.read_state();
    let _ = b.apply_remote("ann", answer);
    assert_eq!(
        b.progress().delivered,
        local.len() as u64,
        "a re-applied answer counts nothing"
    );
    assert_eq!(b.read_state(), state, "a re-applied answer changes nothing");
    let before = b.progress();
    b.submit(inputs.refused_op.clone())
        .expect_err("a refused op is refused");
    assert_eq!(b.progress(), before, "a refused op changes no count");
}
