//! EC.03c1, part (3) (spec/items/EC.03c.md): a state transfer that arrives
//! from the network and names a member its own table lacks is refused, with
//! that member named, never a panic, and leaves the joiner able to adopt the
//! next sound transfer.
//!
//! The donor is a real replica of the set log; its snapshot and log go through
//! JSON (the form a `StateResponse` travels in), and one place in them is made
//! to name `zz9`, a member the snapshot's delivered version does not list. At
//! `d5f9668` `Replica::adopt` panics on each of the five ("adopted member",
//! "adopted event origin"; spec section 8).
//!
//! Each case runs the joiner in a child process (this test binary, re-run on
//! itself), so a panic is an exit status and the cause is read on stderr.

#[path = "ec03c/side.rs"]
mod side;

use std::net::TcpListener;
use std::process::Command;

use moirai_crdt::set::update::SetUpdate;
use moirai_network::direct_transport::DirectTransport;
use moirai_network::generic::GenericNode;
use moirai_network::state_transfer::LogPayload;
use moirai_network::transport::TransportMessage;
use moirai_network::HashMap;
use moirai_protocol::broadcast::tcsb::{IsTcsb, Tcsb};
use moirai_protocol::crdt::query::Read;
use moirai_protocol::replica::{IsReplica, Replica, ReplicaId};
use serde_json::{json, Value};
use side::{LogId, SetLog};

const CASE: &str = "EC03C_REFUSAL_CASE";
const STRANGER: &str = "zz9";

/// `a` and `b` each add one element and deliver each other's, then `a` adds a
/// third: `a`'s snapshot holds two rows, a stable frontier, two outbox events.
fn donor() -> (Value, Value) {
    let mut a: Replica<SetLog, Tcsb<side::Op>> =
        Replica::bootstrap(ReplicaId("a".into()), &["a", "b"]);
    let mut b: Replica<SetLog, Tcsb<side::Op>> =
        Replica::bootstrap(ReplicaId("b".into()), &["a", "b"]);
    let x = a.send(SetUpdate::Add("x".into())).unwrap();
    let y = b.send(SetUpdate::Add("y".into())).unwrap();
    b.receive(x);
    a.receive(y);
    a.send(SetUpdate::Add("z".into())).unwrap();
    (
        serde_json::to_value(a.snapshot()).unwrap(),
        serde_json::to_value(a.state()).unwrap(),
    )
}

/// Append the stranger to a serialized version, frontier or event id's member
/// list, and return the index it takes there.
fn name_stranger(v: &mut Value) -> usize {
    let members = v["resolver"]
        .as_array_mut()
        .expect("a member list (d5f9668's serde form)");
    members.push(json!(STRANGER));
    members.len() - 1
}

fn with_entry(v: &mut Value, seq: u64) {
    name_stranger(v);
    v["entries"]
        .as_array_mut()
        .expect("entries")
        .push(json!(seq));
}

fn with_origin(v: &mut Value) {
    let idx = name_stranger(v);
    v["idx"] = json!(idx);
}

fn sorted(v: Value) -> Vec<String> {
    let mut items: Vec<String> = serde_json::from_value(v).expect("the set reads as strings");
    items.sort();
    items
}

/// The joiner's side, in the child: a node hosting one empty log receives the
/// altered transfer from `a`, then the sound one.
fn joiner(alter: fn(&mut Value, &mut Value)) {
    let (snapshot, log) = donor();
    let (mut bad_snapshot, mut bad_log) = (snapshot.clone(), log.clone());
    alter(&mut bad_snapshot, &mut bad_log);

    let id = LogId::generate();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let transport =
        DirectTransport::with_listener("j".into(), listener, HashMap::default()).unwrap();
    let mut node: GenericNode<SetLog, _> =
        GenericNode::with_transport_and_log_id("j".into(), &["j"], transport, id.clone());
    node.enable_state_transfer();
    let response = |snapshot: Value, log: Value| TransportMessage::StateResponse {
        snapshot: serde_json::from_value(snapshot).expect("the altered snapshot still parses"),
        log: LogPayload::Plain(log),
        log_id: id.clone(),
    };

    node.handle_transport_message("a".into(), response(bad_snapshot, bad_log));
    let hosted = node.hosted(&id).expect("the node hosts its log");
    let read = serde_json::to_value(hosted.query(&Read::new())).unwrap();
    assert_eq!(
        sorted(read),
        Vec::<String>::new(),
        "the refused transfer left state behind"
    );
    assert!(
        !hosted.tcsb().members().iter().any(|m| m.0 == STRANGER),
        "the refused transfer left {STRANGER} in the member table: {:?}",
        hosted.tcsb().members()
    );

    node.handle_transport_message("a".into(), response(snapshot, log));
    let read = serde_json::to_value(node.hosted(&id).unwrap().query(&Read::new())).unwrap();
    assert_eq!(
        sorted(read),
        vec!["x", "y", "z"],
        "the sound transfer after the refusal"
    );
}

/// Parent side: run `name` in a child and judge its exit and its stderr.
fn refused(name: &str, alter: fn(&mut Value, &mut Value)) {
    if std::env::var(CASE).as_deref() == Ok(name) {
        joiner(alter);
        return;
    }
    let out = Command::new(std::env::current_exe().unwrap())
        .args([name, "--exact", "--nocapture", "--test-threads=1"])
        .env(CASE, name)
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        out.status.success(),
        "{name}: the joiner did not survive the transfer ({}):\n{stderr}\n{stdout}",
        out.status
    );
    assert!(
        stderr
            .lines()
            .any(|l| l.to_lowercase().contains("refus") && l.contains(STRANGER)),
        "{name}: no stderr line refuses the transfer and names {STRANGER}:\n{stderr}"
    );
}

#[test]
fn ec03c_r1_a_row_naming_a_stranger_is_refused() {
    refused("ec03c_r1_a_row_naming_a_stranger_is_refused", |s, _| {
        with_entry(&mut s["rows"][1], 1)
    });
}

#[test]
fn ec03c_r2_a_stable_frontier_naming_a_stranger_is_refused() {
    refused(
        "ec03c_r2_a_stable_frontier_naming_a_stranger_is_refused",
        |s, _| with_entry(&mut s["stable"], 0),
    );
}

#[test]
fn ec03c_r3_an_outbox_event_from_a_stranger_is_refused() {
    refused(
        "ec03c_r3_an_outbox_event_from_a_stranger_is_refused",
        |s, _| with_origin(&mut s["outbox"][1]["id"]),
    );
}

#[test]
fn ec03c_r4_an_outbox_version_naming_a_stranger_is_refused() {
    refused(
        "ec03c_r4_an_outbox_version_naming_a_stranger_is_refused",
        |s, _| with_entry(&mut s["outbox"][1]["version"], 1),
    );
}

#[test]
fn ec03c_r5_a_log_naming_a_stranger_is_refused() {
    refused("ec03c_r5_a_log_naming_a_stranger_is_refused", |_, l| {
        with_origin(&mut l["unstable"][1]["tag"]["id"])
    });
}
