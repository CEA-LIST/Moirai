#![cfg(feature = "crdt")]

// Tests for event pulling in CRDTs.

use moirai::crdt::test_util::twins;
use moirai::crdt::{counter::resettable_counter::Counter, set::aw_set::AWSet};
use moirai::protocol::broadcast::tcsb::IsTcsbTest;
use moirai::protocol::replica::IsReplica;
use moirai::set_from_slice;

#[test]
fn events_since_concurrent_counter() {
    let (mut replica_a, mut replica_b) = twins::<Counter<i32>>();

    let _ = replica_a.send(Counter::Inc(1)).unwrap();
    let _ = replica_a.send(Counter::Inc(1)).unwrap();
    let _ = replica_a.send(Counter::Inc(1)).unwrap();
    let _ = replica_a.send(Counter::Inc(1)).unwrap();
    let _ = replica_a.send(Counter::Inc(1)).unwrap();
    let _ = replica_a.send(Counter::Inc(1)).unwrap();
    assert_eq!(6, replica_a.query());

    let _ = replica_b.send(Counter::Dec(1)).unwrap();
    let _ = replica_b.send(Counter::Dec(1)).unwrap();
    let _ = replica_b.send(Counter::Dec(1)).unwrap();
    let _ = replica_b.send(Counter::Dec(1)).unwrap();
    let _ = replica_b.send(Counter::Dec(1)).unwrap();
    let _ = replica_b.send(Counter::Dec(1)).unwrap();
    assert_eq!(-6, replica_b.query());

    let msg_batch = replica_a.pull(replica_b.since());
    assert_eq!(6, msg_batch.batch().events().len());

    replica_b.receive_batch(msg_batch);

    let msg_batch = replica_b.pull(replica_a.since());
    assert_eq!(6, msg_batch.batch().events().len());

    replica_a.receive_batch(msg_batch);

    assert_eq!(replica_a.tcsb().inbox_len(), 0);
    assert_eq!(replica_b.tcsb().inbox_len(), 0);
    assert_eq!(replica_a.query(), 0);
    assert_eq!(replica_b.query(), 0);
}

#[test]
fn event_since_concurrent_aw_set() {
    let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

    let _ = replica_a.send(AWSet::Add("a")).unwrap();
    let _ = replica_a.send(AWSet::Add("b")).unwrap();
    let _ = replica_a.send(AWSet::Add("c")).unwrap();
    let _ = replica_a.send(AWSet::Remove("a")).unwrap();

    let _ = replica_b.send(AWSet::Add("a")).unwrap();
    let _ = replica_b.send(AWSet::Add("e")).unwrap();
    let _ = replica_b.send(AWSet::Add("p")).unwrap();
    let _ = replica_b.send(AWSet::Remove("e")).unwrap();

    let batch = replica_a.pull(replica_b.since());
    replica_b.receive_batch(batch);

    let batch = replica_b.pull(replica_a.since());
    replica_a.receive_batch(batch);

    assert_eq!(replica_a.tcsb().inbox_len(), 0);
    assert_eq!(replica_b.tcsb().inbox_len(), 0);
    assert_eq!(replica_a.query(), replica_b.query());
    assert_eq!(replica_a.query(), set_from_slice(&["a", "b", "c", "p"]));
}

#[test]
fn event_since_concurrent_complex_aw_set() {
    let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

    let event = replica_a.send(AWSet::Add("a")).unwrap();
    replica_b.receive(event);

    let _ = replica_a.send(AWSet::Add("b")).unwrap();
    let _ = replica_a.send(AWSet::Add("c")).unwrap();
    let _ = replica_a.send(AWSet::Remove("a")).unwrap();

    let _ = replica_b.send(AWSet::Add("e")).unwrap();
    let _ = replica_b.send(AWSet::Add("p")).unwrap();
    let _ = replica_b.send(AWSet::Remove("e")).unwrap();

    let msg_batch = replica_a.pull(replica_b.since());
    assert_eq!(msg_batch.batch().events().len(), 3);
    replica_b.receive_batch(msg_batch);

    let msg_batch = replica_b.pull(replica_a.since());
    assert_eq!(msg_batch.batch().events().len(), 3);
    replica_a.receive_batch(msg_batch);

    assert_eq!(replica_a.tcsb().inbox_len(), 0);
    assert_eq!(replica_b.tcsb().inbox_len(), 0);
    assert_eq!(replica_a.query(), replica_b.query());
    assert_eq!(replica_a.query(), set_from_slice(&["b", "c", "p"]));
}
