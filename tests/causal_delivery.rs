#![cfg(feature = "crdt")]

use po_crdt::crdt::{
    counter::Counter,
    test_util::{triplet_graph, twins_graph},
};

#[test_log::test]
fn causal_delivery_twins() {
    let (mut tcsb_a, mut tcsb_b) = twins_graph::<Counter<i32>>();

    let event_a_1 = tcsb_a.tc_bcast(Counter::Inc(1));
    let event_a_2 = tcsb_a.tc_bcast(Counter::Inc(1));

    tcsb_b.try_deliver(event_a_2);
    tcsb_b.try_deliver(event_a_1);

    assert_eq!(tcsb_b.eval(), 2);
    assert_eq!(tcsb_a.eval(), 2);

    let event_b_1 = tcsb_b.tc_bcast(Counter::Inc(1));
    let event_b_2 = tcsb_b.tc_bcast(Counter::Inc(1));
    let event_b_3 = tcsb_b.tc_bcast(Counter::Inc(1));
    let event_b_4 = tcsb_b.tc_bcast(Counter::Inc(1));

    tcsb_a.try_deliver(event_b_3);
    tcsb_a.try_deliver(event_b_1);
    tcsb_a.try_deliver(event_b_4);
    tcsb_a.try_deliver(event_b_2);

    assert_eq!(tcsb_a.eval(), 6);
    assert_eq!(tcsb_b.eval(), 6);
}

#[test_log::test]
fn causal_delivery_triplet() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet_graph::<Counter<i32>>();

    let event_b = tcsb_b.tc_bcast(Counter::Inc(2));

    tcsb_a.try_deliver(event_b.clone());
    let event_a = tcsb_a.tc_bcast(Counter::Dec(7));

    tcsb_b.try_deliver(event_a.clone());
    tcsb_c.try_deliver(event_a.clone());
    tcsb_c.try_deliver(event_b.clone());

    assert_eq!(tcsb_a.eval(), -5);
    assert_eq!(tcsb_b.eval(), -5);
    assert_eq!(tcsb_c.eval(), -5);
}
