//! NW.31a: the replica, factory and extension traits of the NW.31 design
//! (vault spec/items/NW.31-network-split-design.md, section 2), written against
//! by a toy replica that names no Moirai type.
//!
//! The toy holds an `Rc`: the port has no `Send` bound, because upstream's
//! `Resolver` is an `Rc` and replicas live on the node's loop thread.

use std::rc::Rc;

use moirai_netstack::{Extension, LogId, Opaque, Progress, ReplicaFactory, ReplicaPort};
use serde_json::{Value, json};

/// A grow-only list of strings; an op that is not a string is refused.
struct Toy {
    id: String,
    members: Vec<String>,
    items: Vec<String>,
    _loop_thread_only: Rc<()>,
}

impl ReplicaPort for Toy {
    fn replica_id(&self) -> &str {
        &self.id
    }

    fn submit(&mut self, op: Value) -> Result<Opaque, String> {
        let item = op.as_str().ok_or("a toy op is a string")?.to_string();
        self.items.push(item.clone());
        Ok(json!({ "from": self.id, "item": item }))
    }

    fn apply_remote(&mut self, _from: &str, ops: Opaque) -> Result<(), String> {
        let item = ops["item"].as_str().ok_or("not a toy event")?;
        self.items.push(item.to_string());
        Ok(())
    }

    fn frontier(&self) -> Opaque {
        json!(self.items.len())
    }

    fn ops_since(&mut self, frontier: Opaque) -> Result<Opaque, String> {
        let from = frontier.as_u64().ok_or("not a toy frontier")? as usize;
        Ok(json!(self.items.get(from..).unwrap_or_default()))
    }

    fn progress(&self) -> Progress {
        Progress {
            delivered: self.items.len() as u64,
            pending: 0,
            retained: 0,
        }
    }

    fn members(&self) -> Vec<String> {
        self.members.clone()
    }

    fn has_history_from(&self, _peer: &str) -> bool {
        false
    }

    fn read_state(&self) -> Value {
        json!(self.items)
    }
}

struct ToyFactory;

impl ReplicaFactory for ToyFactory {
    type Replica = Toy;

    fn open(&mut self, _log: &str, self_id: &str, members: &[String]) -> Toy {
        Toy {
            id: self_id.to_string(),
            members: members.to_vec(),
            items: Vec::new(),
            _loop_thread_only: Rc::new(()),
        }
    }
}

/// An extension that writes no method: every hook has a default.
struct Plain;

impl Extension<Toy> for Plain {}

fn open_alice() -> Toy {
    let members = ["alice".to_string(), "bob".to_string()];
    ToyFactory.open("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2", "alice", &members)
}

#[test]
fn the_port_defaults_follow_the_design() {
    let mut toy = open_alice();
    assert_eq!(toy.replica_id(), "alice");
    assert_eq!(toy.members(), ["alice", "bob"]);

    // submit_batch: submit in order, stop at the first refusal, report the
    // applied prefix, the refusal and the events of the applied ops.
    let (applied, refusal, events) =
        toy.submit_batch(vec![json!("a"), json!("b"), json!(3), json!("c")]);
    assert_eq!(
        applied, 2,
        "the applied prefix is the two ops before the refusal"
    );
    assert_eq!(refusal.as_deref(), Some("a toy op is a string"));
    assert_eq!(
        events,
        vec![
            json!({ "from": "alice", "item": "a" }),
            json!({ "from": "alice", "item": "b" })
        ]
    );
    assert_eq!(
        toy.read_state(),
        json!(["a", "b"]),
        "nothing after the refusal is applied"
    );
    assert_eq!(toy.progress().delivered, 2);

    // read_identified defaults to read_state; a replica that does not offer a
    // state transfer answers None.
    assert_eq!(toy.read_identified(), toy.read_state());
    assert!(toy.snapshot_for("bob").is_none());
}

#[test]
fn an_extension_that_writes_no_method_admits_local_ops() {
    let log: LogId =
        serde_json::from_value(json!("a1b2a1b2a1b2a1b2a1b2a1b2a1b2a1b2")).expect("a wire log id");
    assert_eq!(Plain.admit_local(&log, &json!("a")), Ok(()));
}
