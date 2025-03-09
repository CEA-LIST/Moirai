use po_crdt::crdt::{counter::Counter, test_util::triplet_po};

#[test_log::test]
fn no_maximum_stable_event() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet_po::<Counter<i32>>();

    let event_a = tcsb_a.tc_bcast(Counter::Inc(1));
    let event_b = tcsb_b.tc_bcast(Counter::Inc(7));

    tcsb_b.try_deliver(event_a.clone());
    tcsb_a.try_deliver(event_b.clone());
    tcsb_c.try_deliver(event_a);
    tcsb_c.try_deliver(event_b);

    let event_b_2 = tcsb_b.tc_bcast(Counter::Inc(3));
    let event_c = tcsb_c.tc_bcast(Counter::Dec(4));

    println!("{:?}", tcsb_a.state.unstable);

    tcsb_a.try_deliver(event_b_2.clone());
    tcsb_a.try_deliver(event_c.clone());
    tcsb_b.try_deliver(event_c);
    tcsb_c.try_deliver(event_b_2);

    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), tcsb_c.eval());
    assert_eq!(tcsb_a.eval(), 7);
    assert_eq!(tcsb_a.state.stable.len(), 2);

    println!("{}", tcsb_a.lsv);
    println!("{:?}", tcsb_a.state.unstable);
}
