//! NW.31k oracle, round 4, part (2b) (vault spec/items/NW.31k.md): the
//! adapter's adopt side is guarded. apply_remote refuses a snapshot on a
//! replica that already has history (its own write, or an event it received)
//! with an error naming that history, and leaves its state as it was. A
//! snapshot that parses but names a member its member list does not hold (the
//! donor's member list without c, while c's events stay in it) is refused
//! with an error, without a panic and without a change of state, and the
//! same replica then adopts the true snapshot.

use std::panic::{AssertUnwindSafe, catch_unwind};

use moirai_netstack::{ReplicaFactory, ReplicaPort};
use moirai_netstack_crdt::{UpstreamReplica, awset};
use serde_json::{Value, json};

type Port = UpstreamReplica<awset::Log>;

fn open(id: &str) -> Port {
    awset::factory().open("set", id, &[id.to_string()])
}

/// a, b and c each add "<id>1" and every add reaches the two others. Returns
/// the three replicas and the three adds as sent.
fn donors() -> (Vec<Port>, Vec<Value>) {
    let ids = ["a", "b", "c"].map(String::from);
    let mut r: Vec<Port> = ids
        .iter()
        .map(|id| awset::factory().open("set", id, &ids))
        .collect();
    let adds: Vec<Value> = (0..3)
        .map(|i| {
            r[i].submit(json!({ "Add": format!("{}1", ids[i]) }))
                .expect("the add is taken")
        })
        .collect();
    for (i, add) in adds.iter().enumerate() {
        for j in (0..3).filter(|&j| j != i) {
            r[j].apply_remote(&ids[i], add.clone())
                .expect("the add is applied");
        }
    }
    (r, adds)
}

fn snapshot(donor: &mut Port) -> Value {
    donor
        .snapshot_for("joiner")
        .expect("a replica with history offers a snapshot")
        .expect("the snapshot is made")
}

/// Every JSON array of strings that lists the member c among members only
/// loses c.
fn without_c(v: &mut Value) -> usize {
    match v {
        Value::Array(items) => {
            let members = ["a", "b", "c", "joiner"];
            let is_list = items.iter().any(|i| i == "c")
                && items
                    .iter()
                    .all(|i| i.as_str().is_some_and(|s| members.contains(&s)));
            if is_list {
                items.retain(|i| i != "c");
                1
            } else {
                items.iter_mut().map(without_c).sum()
            }
        }
        Value::Object(fields) => fields.values_mut().map(without_c).sum(),
        _ => 0,
    }
}

#[test]
fn nw31k_apply_remote_refuses_a_snapshot_on_a_replica_with_history() {
    let (mut r, adds) = donors();
    let snap = snapshot(&mut r[0]);

    // e wrote once itself.
    let mut e = open("e");
    e.submit(json!({ "Add": "e1" })).expect("the add is taken");
    let err = e
        .apply_remote("a", snap.clone())
        .expect_err("a snapshot on a replica that wrote must be refused");
    assert!(
        err.contains("history"),
        "the refusal names the history: {err}"
    );
    assert_eq!(e.read_state(), json!(["e1"]), "e's state is unchanged");

    // f only received a's add.
    let mut f = open("f");
    f.apply_remote("a", adds[0].clone())
        .expect("a's add is applied");
    let err = f
        .apply_remote("a", snap)
        .expect_err("a snapshot on a replica that received an event must be refused");
    assert!(
        err.contains("history"),
        "the refusal names the history: {err}"
    );
    assert_eq!(f.read_state(), json!(["a1"]), "f's state is unchanged");
}

#[test]
fn nw31k_apply_remote_refuses_an_inconsistent_snapshot_without_panicking() {
    let (mut r, _) = donors();
    let snap = snapshot(&mut r[0]);
    let mut bad = snap.clone();
    assert!(
        without_c(&mut bad) > 0,
        "the snapshot lists the donor's members as a JSON array of their ids"
    );

    let mut g = open("g");
    let before = g.read_state();
    let outcome = catch_unwind(AssertUnwindSafe(|| g.apply_remote("a", bad)));
    let result = outcome.expect("apply_remote must not panic on an inconsistent snapshot");
    assert!(result.is_err(), "an inconsistent snapshot must be refused");
    assert_eq!(g.read_state(), before, "a refused snapshot changes nothing");

    // The same replica then adopts the true snapshot and takes part.
    g.apply_remote("a", snap)
        .expect("the true snapshot is adopted");
    assert_eq!(g.read_state(), json!(["a1", "b1", "c1"]));
    let add = g.submit(json!({ "Add": "g1" })).expect("g's add is taken");
    r[1].apply_remote("g", add).expect("g's add reaches b");
    assert_eq!(r[1].read_state(), json!(["a1", "b1", "c1", "g1"]));
}
