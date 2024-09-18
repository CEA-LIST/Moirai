use po_crdt::{
    crdt::{counter::Counter, membership_set::MSet},
    protocol::tcsb::Tcsb,
};

#[test_log::test]
fn join() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");

    let event_a = tcsb_a.tc_bcast_gms(MSet::Add("b"));
    let event_b = tcsb_b.tc_bcast_gms(MSet::Add("a"));

    tcsb_b.tc_deliver_gms(event_a);
    tcsb_a.tc_deliver_gms(event_b);

    let event = tcsb_a.tc_bcast(Counter::Inc(5));
    tcsb_b.tc_deliver(event);

    let event = tcsb_b.tc_bcast(Counter::Dec(5));
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_a.ltm.keys(), tcsb_b.ltm.keys());

    let result = 0;
    assert_eq!(tcsb_a.eval(), result);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
}

#[test_log::test]
fn leave() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");

    let event_a = tcsb_a.tc_bcast_gms(MSet::Add("b"));
    let event_b = tcsb_b.tc_bcast_gms(MSet::Add("a"));

    tcsb_b.tc_deliver_gms(event_a);
    tcsb_a.tc_deliver_gms(event_b);

    let event = tcsb_a.tc_bcast(Counter::Inc(5));
    tcsb_b.tc_deliver(event);

    let event = tcsb_b.tc_bcast(Counter::Dec(5));
    tcsb_a.tc_deliver(event);

    let event = tcsb_a.tc_bcast_gms(MSet::Remove("a"));
    tcsb_b.tc_deliver_gms(event);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["b"]);
}
