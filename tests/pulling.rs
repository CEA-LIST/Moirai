#![cfg(feature = "crdt")]

// Tests for event pulling in CRDTs.

use std::collections::HashSet;

use moirai::{
    crdt::{counter::resettable_counter::Counter, set::aw_set::AWSet, test_util::twins_graph},
    protocol::pulling::Since,
};

#[test_log::test]
fn events_since_concurrent_counter() {
    let (mut tcsb_a, mut tcsb_b) = twins_graph::<Counter<i32>>();

    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    assert_eq!(6, tcsb_a.eval());
    assert_eq!(6, tcsb_a.state.unstable.node_count());

    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    assert_eq!(-6, tcsb_b.eval());
    assert_eq!(6, tcsb_b.state.unstable.node_count());

    let batch = tcsb_a.events_since(&Since::new_from(&tcsb_b));
    assert_eq!(6, batch.clone().unwrap().events.len());

    tcsb_b.deliver_batch(batch);

    let batch = tcsb_b.events_since(&Since::new_from(&tcsb_a));
    assert_eq!(6, batch.clone().unwrap().events.len());

    tcsb_a.deliver_batch(batch);

    assert_eq!(tcsb_a.pending.len(), 0);
    assert_eq!(tcsb_b.pending.len(), 0);
    assert_eq!(tcsb_a.eval(), 0);
    assert_eq!(tcsb_b.eval(), 0);
}

#[test_log::test]
fn event_since_concurrent_aw_set() {
    let (mut tcsb_a, mut tcsb_b) = twins_graph::<AWSet<&str>>();

    let _ = tcsb_a.tc_bcast(AWSet::Add("a"));
    let _ = tcsb_a.tc_bcast(AWSet::Add("b"));
    let _ = tcsb_a.tc_bcast(AWSet::Add("c"));
    let _ = tcsb_a.tc_bcast(AWSet::Remove("a"));

    let _ = tcsb_b.tc_bcast(AWSet::Add("a"));
    let _ = tcsb_b.tc_bcast(AWSet::Add("e"));
    let _ = tcsb_b.tc_bcast(AWSet::Add("p"));
    let _ = tcsb_b.tc_bcast(AWSet::Remove("e"));

    let batch = tcsb_a.events_since(&Since::new_from(&tcsb_b));
    tcsb_b.deliver_batch(batch);

    let batch = tcsb_b.events_since(&Since::new_from(&tcsb_a));
    tcsb_a.deliver_batch(batch);

    assert_eq!(tcsb_a.pending.len(), 0);
    assert_eq!(tcsb_b.pending.len(), 0);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), HashSet::from(["a", "b", "c", "p"]));
}

#[test_log::test]
fn event_since_concurrent_complex_aw_set() {
    let (mut tcsb_a, mut tcsb_b) = twins_graph::<AWSet<&str>>();

    let event = tcsb_a.tc_bcast(AWSet::Add("a"));
    tcsb_b.try_deliver(event);

    let _ = tcsb_a.tc_bcast(AWSet::Add("b"));
    let _ = tcsb_a.tc_bcast(AWSet::Add("c"));
    let _ = tcsb_a.tc_bcast(AWSet::Remove("a"));

    let _ = tcsb_b.tc_bcast(AWSet::Add("e"));
    let _ = tcsb_b.tc_bcast(AWSet::Add("p"));
    let _ = tcsb_b.tc_bcast(AWSet::Remove("e"));

    let since = Since::new_from(&tcsb_b);
    let batch = tcsb_a.events_since(&since);
    assert_eq!(batch.clone().unwrap().events.len(), 3);
    tcsb_b.deliver_batch(batch);

    let since = Since::new_from(&tcsb_a);
    let batch = tcsb_b.events_since(&since);
    assert_eq!(batch.clone().unwrap().events.len(), 3);
    tcsb_a.deliver_batch(batch);

    assert_eq!(tcsb_a.pending.len(), 0);
    assert_eq!(tcsb_b.pending.len(), 0);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), HashSet::from(["b", "c", "p"]));
}
