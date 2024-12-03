use po_crdt::{
    crdt::{
        counter::Counter,
        membership_set::MSet,
        test_util::{quadruplet, twins},
    },
    protocol::tcsb::Tcsb,
};

#[test_log::test]
fn join() {
    let (mut tcsb_a, mut tcsb_b) = twins();

    let event = tcsb_a.tc_bcast_op(Counter::Inc(5));
    tcsb_b.tc_deliver_op(event);

    let event = tcsb_b.tc_bcast_op(Counter::Dec(5));
    tcsb_a.tc_deliver_op(event);

    assert_eq!(tcsb_a.ltm.keys(), tcsb_b.ltm.keys());

    let result = 0;
    assert_eq!(tcsb_a.eval(), result);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
}

#[test_log::test]
fn join_multiple_members() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");
    let mut tcsb_c = Tcsb::<Counter<i32>>::new("c");

    let _ = tcsb_a.tc_bcast_membership(MSet::add("b"));
    tcsb_b.state_transfer(&mut tcsb_a);

    // At this point, a and b are in the same group

    // b welcomes c
    let event_b = tcsb_b.tc_bcast_membership(MSet::add("c"));
    tcsb_a.tc_deliver_membership(event_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);

    // Useless event: just to exchange causal information
    let event_a = tcsb_a.tc_bcast_membership(MSet::add("c"));
    tcsb_b.tc_deliver_membership(event_a);

    tcsb_c.state_transfer(&mut tcsb_b);

    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);
}

#[test_log::test]
fn concurrent_joins() {
    let (mut tcsb_a, mut tcsb_b) = twins();

    let mut tcsb_c = Tcsb::<Counter<i32>>::new("c");
    let mut tcsb_d = Tcsb::<Counter<i32>>::new("d");

    // a welcomes c
    let event_b = tcsb_b.tc_bcast_membership(MSet::add("c"));
    // b welcomes d
    let event_a = tcsb_a.tc_bcast_membership(MSet::add("d"));

    // Concurrent delivery
    tcsb_a.tc_deliver_membership(event_b);
    tcsb_b.tc_deliver_membership(event_a);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "d"]);

    // Useless event: just to exchange causal information
    let event_a = tcsb_a.tc_bcast_op(Counter::Inc(5));
    tcsb_b.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_a.clone());
    tcsb_d.tc_deliver_op(event_a);

    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    tcsb_c.state_transfer(&mut tcsb_b);

    let event_b = tcsb_b.tc_bcast_op(Counter::Dec(5));
    tcsb_a.tc_deliver_op(event_b.clone());
    tcsb_c.tc_deliver_op(event_b.clone());
    tcsb_d.tc_deliver_op(event_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    tcsb_d.state_transfer(&mut tcsb_a);

    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d"]);

    let result = 0;
    assert_eq!(tcsb_a.eval(), result);
    assert_eq!(tcsb_b.eval(), result);
    assert_eq!(tcsb_c.eval(), result);
    assert_eq!(tcsb_d.eval(), result);
}

#[test_log::test]
fn join_multiple_members_same_node() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");

    let _ = tcsb_a.tc_bcast_membership(MSet::add("b"));

    tcsb_b.state_transfer(&mut tcsb_a);

    let event_a = tcsb_a.tc_bcast_membership(MSet::add("c"));

    tcsb_b.tc_deliver_membership(event_a);
    let event_a = tcsb_a.tc_bcast_op(Counter::Inc(5));

    tcsb_b.tc_deliver_op(event_a);

    let event_b = tcsb_b.tc_bcast_op(Counter::Dec(1));
    tcsb_a.tc_deliver_op(event_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_a.ltm.keys(), tcsb_b.ltm.keys())
}

#[test_log::test]
fn rejoin() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c, mut tcsb_d) = quadruplet();

    let event = tcsb_a.tc_bcast_membership(MSet::remove("d"));
    tcsb_b.tc_deliver_membership(event.clone());
    tcsb_c.tc_deliver_membership(event.clone());
    tcsb_d.tc_deliver_membership(event);

    let event = tcsb_b.tc_bcast_op(Counter::Inc(2));
    tcsb_a.tc_deliver_op(event.clone());
    tcsb_c.tc_deliver_op(event.clone());
    tcsb_d.tc_deliver_op(event);

    let event = tcsb_c.tc_bcast_op(Counter::Dec(1));
    tcsb_a.tc_deliver_op(event.clone());
    tcsb_b.tc_deliver_op(event.clone());
    tcsb_d.tc_deliver_op(event);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["d"]);
    assert_eq!(tcsb_a.eval(), 1);
    assert_eq!(tcsb_b.eval(), 1);
    assert_eq!(tcsb_c.eval(), 1);
    assert_eq!(tcsb_d.eval(), 0);

    let event = tcsb_b.tc_bcast_membership(MSet::add("d"));
    tcsb_a.tc_deliver_membership(event.clone());
    tcsb_c.tc_deliver_membership(event.clone());

    let event = tcsb_a.tc_bcast_op(Counter::Inc(4));
    tcsb_b.tc_deliver_op(event.clone());
    tcsb_c.tc_deliver_op(event.clone());

    let event = tcsb_c.tc_bcast_op(Counter::Dec(5));
    tcsb_a.tc_deliver_op(event.clone());
    tcsb_b.tc_deliver_op(event.clone());

    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["d"]);

    // --> Causal stability <--
    tcsb_d.state_transfer(&mut tcsb_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_a.eval(), 0);
    assert_eq!(tcsb_b.eval(), 0);
    assert_eq!(tcsb_c.eval(), 0);
    assert_eq!(tcsb_d.eval(), 0);
}

#[test_log::test]
fn early_rejoin() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c, mut tcsb_d) = quadruplet::<Counter<i32>>();

    let event = tcsb_a.tc_bcast_membership(MSet::remove("d"));
    tcsb_b.tc_deliver_membership(event.clone());
    tcsb_c.tc_deliver_membership(event.clone());
    tcsb_d.tc_deliver_membership(event);

    assert_eq!(tcsb_d.ltm.keys(), vec!["d"]);

    let event = tcsb_b.tc_bcast_membership(MSet::add("d"));
    tcsb_a.tc_deliver_membership(event.clone());
    tcsb_c.tc_deliver_membership(event.clone());
    tcsb_d.tc_deliver_membership(event);

    let event = tcsb_c.tc_bcast_op(Counter::Dec(1));
    tcsb_a.tc_deliver_op(event.clone());
    tcsb_b.tc_deliver_op(event.clone());
    tcsb_d.tc_deliver_op(event);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c"]);

    let event = tcsb_a.tc_bcast_op(Counter::Dec(1));
    tcsb_c.tc_deliver_op(event.clone());
    tcsb_b.tc_deliver_op(event.clone());
    tcsb_d.tc_deliver_op(event);

    // --> Causal stability <--
    tcsb_d.state_transfer(&mut tcsb_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d"]);

    let event = tcsb_d.tc_bcast_op(Counter::Dec(10));
    tcsb_c.tc_deliver_op(event.clone());
    tcsb_b.tc_deliver_op(event.clone());
    tcsb_a.tc_deliver_op(event);
}

#[test_log::test]
fn prevent_missing_messages() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");
    let mut tcsb_c = Tcsb::<Counter<i32>>::new("c");
    let mut tcsb_d = Tcsb::<Counter<i32>>::new("d");
    let mut tcsb_e = Tcsb::<Counter<i32>>::new("e");

    let event_a = tcsb_a.tc_bcast_membership(MSet::add("b"));
    tcsb_b.tc_deliver_membership(event_a);

    tcsb_b.state_transfer(&mut tcsb_a);

    let event_a = tcsb_a.tc_bcast_membership(MSet::add("c"));
    tcsb_b.tc_deliver_membership(event_a);

    let event_b = tcsb_b.tc_bcast_membership(MSet::add("d"));
    tcsb_a.tc_deliver_membership(event_b.clone());
    tcsb_c.tc_deliver_membership(event_b);

    tcsb_c.state_transfer(&mut tcsb_a);

    let event_b = tcsb_b.tc_bcast_membership(MSet::add("e"));
    tcsb_a.tc_deliver_membership(event_b.clone());
    tcsb_c.tc_deliver_membership(event_b);

    let event_a = tcsb_a.tc_bcast_op(Counter::Inc(1));
    tcsb_b.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_a.clone());
    tcsb_d.tc_deliver_op(event_a);

    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d", "e"]);

    let event_c = tcsb_c.tc_bcast_op(Counter::Inc(1));
    tcsb_a.tc_deliver_op(event_c.clone());
    tcsb_b.tc_deliver_op(event_c.clone());
    tcsb_d.tc_deliver_op(event_c.clone());
    tcsb_e.tc_deliver_op(event_c);

    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d", "e"]);

    tcsb_d.state_transfer(&mut tcsb_b);
    tcsb_e.state_transfer(&mut tcsb_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d", "e"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d", "e"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d", "e"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d", "e"]);
    assert_eq!(tcsb_e.ltm.keys(), vec!["a", "b", "c", "d", "e"]);

    let event_a = tcsb_a.tc_bcast_membership(MSet::remove("a"));
    tcsb_b.tc_deliver_membership(event_a.clone());
    tcsb_c.tc_deliver_membership(event_a.clone());
    tcsb_d.tc_deliver_membership(event_a.clone());
    tcsb_e.tc_deliver_membership(event_a);

    let event_b = tcsb_b.tc_bcast_membership(MSet::remove("a"));
    tcsb_a.tc_deliver_membership(event_b.clone());
    tcsb_c.tc_deliver_membership(event_b.clone());
    tcsb_d.tc_deliver_membership(event_b.clone());
    tcsb_e.tc_deliver_membership(event_b);
}
