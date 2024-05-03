use po_crdt::{
    crdt::mv_register::Op,
    protocol::{
        event::Message,
        membership::{Membership, Welcome},
        tcsb::Tcsb,
    },
};

#[test_log::test]
fn simple_evict() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");
    let mut tcsb_c = Tcsb::<&str, u64, Op<&str>>::new("c");

    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Join));

    tcsb_b.tc_deliver(event_a.clone());
    tcsb_b.tc_deliver(event_c.clone());
    tcsb_a.tc_deliver(event_c);
    tcsb_c.tc_deliver(event_a);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    let event = tcsb_c.tc_bcast(Message::Op(Op::Write("z")));
    tcsb_b.tc_deliver(event.clone());

    let event = tcsb_a.tc_bcast(Message::Op(Op::Write("y")));
    tcsb_c.tc_deliver(event.clone()); // Should not be delivered
    tcsb_b.tc_deliver(event); // Should be delivered

    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_c.ltm.len(), 2);
    assert_eq!(tcsb_a.eval(), vec!["y"]);
    assert_eq!(tcsb_c.eval(), vec!["z"]);
    assert_eq!(tcsb_b.eval(), vec!["z"]);
}

/// A peer emits events concurrently to its eviction.
#[test_log::test]
fn evict_concurrent_whith_another_event() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");
    let mut tcsb_c = Tcsb::<&str, u64, Op<&str>>::new("c");

    let _ = tcsb_b.tc_bcast(Message::Op(Op::Write("c")));

    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Join));

    tcsb_b.tc_deliver(event_a.clone());
    tcsb_b.tc_deliver(event_c.clone());
    tcsb_a.tc_deliver(event_c);
    tcsb_c.tc_deliver(event_a);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    let event_evict = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));
    let event_write = tcsb_a.tc_bcast(Message::Op(Op::Write("a")));
    tcsb_a.tc_deliver(event_evict.clone());
    tcsb_b.tc_deliver(event_write.clone());
    tcsb_c.tc_deliver(event_write);
    tcsb_c.tc_deliver(event_evict);

    let event = tcsb_c.tc_bcast(Message::Op(Op::Write("z")));
    tcsb_b.tc_deliver(event.clone());
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_c.ltm.len(), 2);
    assert_eq!(tcsb_a.eval(), vec!["a"]);
    assert_eq!(tcsb_c.eval(), vec!["z"]);
    assert_eq!(tcsb_b.eval(), vec!["z"]);
}

#[test_log::test]
fn evict_then_rejoin() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");
    let mut tcsb_c = Tcsb::<&str, u64, Op<&str>>::new("c");

    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Join));

    tcsb_b.tc_deliver(event_a.clone());
    tcsb_b.tc_deliver(event_c.clone());
    tcsb_a.tc_deliver(event_c);
    tcsb_c.tc_deliver(event_a);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event.clone());

    let event = tcsb_c.tc_bcast(Message::Op(Op::Write("z")));
    tcsb_b.tc_deliver(event.clone());
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_c.ltm.len(), 2);

    let event = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    tcsb_b.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_a.ltm.len(), 3);
    assert_eq!(tcsb_b.ltm.len(), 3);
    assert_eq!(tcsb_c.ltm.len(), 3);
}

#[test_log::test]
fn multiple_concurrent_evicts() {
    todo!()
}

#[test_log::test]
fn evict_while_leave() {
    todo!()
}

#[test_log::test]
fn evict_while_rejoin() {
    todo!()
}
