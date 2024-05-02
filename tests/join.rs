use po_crdt::{
    crdt::mv_register::Op,
    protocol::{
        event::Message,
        membership::{Membership, Welcome},
        tcsb::Tcsb,
    },
};

#[test]
fn simple_join() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

    let _ = tcsb_a.tc_bcast(Message::Op(Op::Write("a")));
    let _ = tcsb_a.tc_bcast(Message::Op(Op::Write("b")));

    assert_eq!(tcsb_a.eval(), vec!["b"]);

    let event = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    tcsb_b.tc_deliver(event);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_a.eval(), Vec::<&str>::new());

    let event_a = tcsb_a.tc_bcast(Message::Op(Op::Write("a")));
    let event_b = tcsb_b.tc_bcast(Message::Op(Op::Write("b")));
    tcsb_b.tc_deliver(event_a);
    tcsb_a.tc_deliver(event_b);

    let result = vec!["b", "a"];
    assert_eq!(tcsb_a.ltm.len(), 2);
    assert_eq!(tcsb_a.ltm.len(), tcsb_b.ltm.len());
    assert_eq!(tcsb_a.eval(), result);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
}

#[test]
fn concurrent_join_on_same_node() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");
    let mut tcsb_c = Tcsb::<&str, u64, Op<&str>>::new("c");

    let _ = tcsb_b.tc_bcast(Message::Op(Op::Write("a")));
    let _ = tcsb_b.tc_bcast(Message::Op(Op::Write("b")));

    assert_eq!(tcsb_b.eval(), vec!["b"]);

    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Join));

    tcsb_b.tc_deliver(event_a);
    tcsb_b.tc_deliver(event_c);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event.clone());
    tcsb_c.tc_deliver(event);

    assert_eq!(tcsb_a.ltm.len(), 3);
    assert_eq!(tcsb_b.ltm.len(), 3);
    assert_eq!(tcsb_c.ltm.len(), 3);
    assert_eq!(tcsb_a.eval(), vec!["b"]);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), tcsb_c.eval());
}

#[test]
fn concurrent_join_on_different_nodes() {}

#[test]
fn concurrent_join_on_same_node_with_new_event() {}
