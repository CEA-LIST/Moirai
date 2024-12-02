#[cfg(feature = "utils")]
use camino::Utf8PathBuf;
use po_crdt::crdt::{
    counter::Counter,
    membership_set::MSet,
    test_util::{quadruplet, triplet, twins},
};

#[test_log::test]
fn cross_evict() {
    let (mut tcsb_a, mut tcsb_b) = twins::<Counter<i32>>();

    let event_a = tcsb_a.tc_bcast_membership(MSet::remove("b"));
    let event_b = tcsb_b.tc_bcast_membership(MSet::remove("a"));

    tcsb_a.tc_deliver_membership(event_b);
    tcsb_b.tc_deliver_membership(event_a);

    assert_eq!(tcsb_a.eval_group_membership().len(), 1);
    assert_eq!(tcsb_b.eval_group_membership().len(), 1);
    assert_eq!(tcsb_b.ltm.keys(), vec!["b"]);
    assert_eq!(tcsb_a.ltm.keys(), vec!["a"]);
}

#[test_log::test]
fn simple_evict() {
    let (mut tcsb_a, mut tcsb_b) = twins::<Counter<i32>>();

    let event_a = tcsb_a.tc_bcast_membership(MSet::remove("b"));
    tcsb_b.tc_deliver_membership(event_a);

    assert_eq!(tcsb_a.eval_group_membership().len(), 1);
    assert_eq!(tcsb_b.eval_group_membership().len(), 1);
    assert_eq!(tcsb_b.ltm.keys(), vec!["b"]);
    assert_eq!(tcsb_a.ltm.keys(), vec!["a"]);
}

#[test_log::test]
fn evict_full_scenario() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c, mut tcsb_d) = quadruplet();

    let event = tcsb_a.tc_bcast_op(Counter::Inc(5));
    tcsb_b.tc_deliver_op(event.clone());
    tcsb_c.tc_deliver_op(event.clone());
    tcsb_d.tc_deliver_op(event);

    let event = tcsb_b.tc_bcast_op(Counter::Dec(5));
    tcsb_a.tc_deliver_op(event.clone());
    tcsb_c.tc_deliver_op(event.clone());
    tcsb_d.tc_deliver_op(event);

    assert_eq!(tcsb_a.eval(), 0);
    assert_eq!(tcsb_c.eval(), 0);
    assert_eq!(tcsb_b.eval(), 0);
    assert_eq!(tcsb_d.eval(), 0);

    let event_a = tcsb_a.tc_bcast_membership(MSet::remove("c"));
    let event_c_1 = tcsb_c.tc_bcast_op(Counter::Inc(5));
    let event_c_2 = tcsb_c.tc_bcast_op(Counter::Inc(15));
    let event_c_3 = tcsb_c.tc_bcast_op(Counter::Inc(25));

    // b delivers events
    tcsb_b.tc_deliver_membership(event_a.clone());
    tcsb_b.tc_deliver_op(event_c_1.clone());
    tcsb_b.tc_deliver_op(event_c_2.clone());
    tcsb_b.tc_deliver_op(event_c_3.clone());

    // d delivers events
    tcsb_d.tc_deliver_op(event_c_1.clone());
    tcsb_d.tc_deliver_op(event_c_2.clone());
    tcsb_d.tc_deliver_membership(event_a.clone());
    tcsb_d.tc_deliver_op(event_c_3.clone());

    let event_d = tcsb_d.tc_bcast_op(Counter::Dec(5));
    let event_b = tcsb_b.tc_bcast_op(Counter::Inc(5));

    // a delivers
    tcsb_a.tc_deliver_op(event_c_1.clone());
    tcsb_a.tc_deliver_op(event_c_2.clone());
    tcsb_a.tc_deliver_op(event_c_3.clone());
    tcsb_a.tc_deliver_op(event_d.clone());
    tcsb_a.tc_deliver_op(event_b.clone());

    // b delivers
    tcsb_b.tc_deliver_op(event_d.clone());

    // d delivers
    tcsb_d.tc_deliver_op(event_b.clone());

    // c delivers
    tcsb_c.tc_deliver_membership(event_a);
    assert_eq!(tcsb_c.ltm.keys(), vec!["c"]);
    tcsb_c.tc_deliver_op(event_d);

    // <- AT THIS POINT, A AND B MUST SEND A MESSAGE TO PROVE THEY HAVE RECEIVED THE GOOD NUMBER OF MESSAGES FROM C ->

    let event_b = tcsb_b.tc_bcast_op(Counter::Inc(5));
    tcsb_a.tc_deliver_op(event_b.clone());
    tcsb_c.tc_deliver_op(event_b.clone());
    tcsb_d.tc_deliver_op(event_b);

    let event_a = tcsb_a.tc_bcast_op(Counter::Dec(5));

    tcsb_b.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_a.clone());
    tcsb_d.tc_deliver_op(event_a);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "d"]);

    assert_eq!(tcsb_a.eval(), 20);
    assert_eq!(tcsb_b.eval(), 20);
    assert_eq!(tcsb_c.eval(), 45);
    assert_eq!(tcsb_d.eval(), 20);
}

#[test_log::test]
fn evict_multiple_messages() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c, mut tcsb_d) = quadruplet();

    let event_d_1 = tcsb_d.tc_bcast_op(Counter::Inc(2));
    let event_d_2 = tcsb_d.tc_bcast_op(Counter::Dec(8));
    let event_d_3 = tcsb_d.tc_bcast_op(Counter::Inc(6));
    let event_d_4 = tcsb_d.tc_bcast_op(Counter::Dec(1));

    tcsb_a.tc_deliver_op(event_d_1.clone());
    tcsb_c.tc_deliver_op(event_d_1.clone());

    let event_c = tcsb_c.tc_bcast_op(Counter::Inc(10));
    let event_b = tcsb_b.tc_bcast_membership(MSet::remove("d"));

    tcsb_a.tc_deliver_membership(event_b.clone());
    tcsb_a.tc_deliver_op(event_c.clone());
    tcsb_c.tc_deliver_membership(event_b.clone());
    tcsb_b.tc_deliver_op(event_c.clone());
    tcsb_d.tc_deliver_op(event_c);

    tcsb_b.tc_deliver_op(event_d_1.clone());
    tcsb_a.tc_deliver_op(event_d_2.clone());

    let event_c = tcsb_c.tc_bcast_op(Counter::Inc(14));

    tcsb_b.tc_deliver_op(event_c.clone());
    tcsb_b.tc_deliver_op(event_d_2.clone());

    tcsb_a.tc_deliver_op(event_d_3.clone());
    tcsb_a.tc_deliver_op(event_c.clone());

    let event_a = tcsb_a.tc_bcast_op(Counter::Dec(22));

    tcsb_b.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_d_2.clone());
    tcsb_c.tc_deliver_op(event_a.clone());

    tcsb_c.tc_deliver_op(event_d_3.clone());
    tcsb_c.tc_deliver_op(event_d_4.clone());

    tcsb_b.tc_deliver_op(event_d_3.clone());
    tcsb_b.tc_deliver_op(event_d_4.clone());

    tcsb_a.tc_deliver_op(event_d_4.clone());

    tcsb_d.tc_deliver_membership(event_b.clone());

    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["d"]);

    let event_b = tcsb_b.tc_bcast_op(Counter::Inc(1));
    tcsb_a.tc_deliver_op(event_b.clone());
    tcsb_c.tc_deliver_op(event_b.clone());
    tcsb_d.tc_deliver_op(event_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);

    assert_eq!(tcsb_a.eval(), 5);
    assert_eq!(tcsb_b.eval(), 5);
    assert_eq!(tcsb_c.eval(), 5);
    assert_eq!(tcsb_d.eval(), 9);

    #[cfg(feature = "utils")]
    tcsb_b
        .tracer
        .serialize_to_file(&Utf8PathBuf::from(
            "traces/membership_evict_multiple_msg_b_trace.json",
        ))
        .unwrap();
}

#[test_log::test]
fn concurrent_evicts() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<Counter<i32>>();

    let event_a = tcsb_a.tc_bcast_membership(MSet::remove("d"));
    let event_b = tcsb_b.tc_bcast_membership(MSet::remove("d"));

    tcsb_c.tc_deliver_membership(event_b.clone());
    tcsb_c.tc_deliver_membership(event_a.clone());

    tcsb_a.tc_deliver_membership(event_b);
    tcsb_b.tc_deliver_membership(event_a);
}
