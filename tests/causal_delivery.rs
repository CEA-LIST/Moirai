use po_crdt::crdt::{
    counter::Counter,
    test_util::{triplet, twins},
};

#[test_log::test]
fn causal_delivery() {
    let (mut tcsb_a, mut tcsb_b) = twins::<Counter<i32>>();

    let event_a_1 = tcsb_a.tc_bcast_op(Counter::Inc(1));
    let event_a_2 = tcsb_a.tc_bcast_op(Counter::Inc(1));

    tcsb_b.tc_deliver_op(event_a_2);
    tcsb_b.tc_deliver_op(event_a_1);

    assert_eq!(tcsb_b.eval(), 2);
    assert_eq!(tcsb_a.eval(), 2);

    let event_b_1 = tcsb_b.tc_bcast_op(Counter::Inc(1));
    let event_b_2 = tcsb_b.tc_bcast_op(Counter::Inc(1));
    let event_b_3 = tcsb_b.tc_bcast_op(Counter::Inc(1));
    let event_b_4 = tcsb_b.tc_bcast_op(Counter::Inc(1));

    tcsb_a.tc_deliver_op(event_b_4);
    tcsb_a.tc_deliver_op(event_b_3);
    tcsb_a.tc_deliver_op(event_b_1);
    tcsb_a.tc_deliver_op(event_b_2);

    assert_eq!(tcsb_a.eval(), 6);
    assert_eq!(tcsb_b.eval(), 6);
}

#[test_log::test]
fn causal_delivery_triplet() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<Counter<i32>>();

    let event_b = tcsb_b.tc_bcast_op(Counter::Inc(2));

    tcsb_a.tc_deliver_op(event_b.clone());
    let event_a = tcsb_a.tc_bcast_op(Counter::Dec(7));

    tcsb_b.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_b.clone());

    assert_eq!(tcsb_a.eval(), -5);
    assert_eq!(tcsb_b.eval(), -5);
    assert_eq!(tcsb_c.eval(), -5);
}
