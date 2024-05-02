use po_crdt::{
    crdt::mv_register::Op,
    protocol::{
        event::Message,
        membership::{Membership, Welcome},
        tcsb::Tcsb,
    },
};

#[test]
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
#[test]
fn evict_concurrent_events() {
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

    println!("EVAL B {:?}", tcsb_b.eval());
    println!("EVAL C {:?}", tcsb_c.eval());

    println!("BEFORE STATE C {:?}", tcsb_c.state);
    println!("BEFORE STATE B {:?}", tcsb_b.state);
    let event = tcsb_c.tc_bcast(Message::Op(Op::Write("z")));
    println!("STATE C {:?}", tcsb_c.state);
    tcsb_b.tc_deliver(event.clone());
    tcsb_a.tc_deliver(event);

    println!("B LTM {:?}", tcsb_b.ltm);
    println!("C LTM {:?}", tcsb_c.ltm);

    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_c.ltm.len(), 2);
    assert_eq!(tcsb_a.eval(), vec!["a"]);
    assert_eq!(tcsb_c.eval(), vec!["z"]);
    assert_eq!(tcsb_b.eval(), vec!["z"]);
}

#[test]
fn evict_then_rejoin() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");
    let mut tcsb_c = Tcsb::<&str, u64, Op<&str>>::new("c");

    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Join));

    tcsb_b.tc_deliver(event_a);
    tcsb_b.tc_deliver(event_c);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event.clone());

    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Join));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event.clone());

    assert_eq!(tcsb_a.ltm.len(), 2);
    assert_eq!(tcsb_b.ltm.len(), 3);
    assert_eq!(tcsb_c.ltm.len(), 3);
}
