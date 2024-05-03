use std::vec;

use po_crdt::{
    crdt::mv_register::Op,
    protocol::{
        event::Message,
        membership::{Membership, Welcome},
        tcsb::Tcsb,
    },
};

#[test_log::test]
fn simple_leave() {
    let mut tcsb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

    let event = tcsb_a.tc_bcast(Message::Membership(Membership::Join));
    tcsb_b.tc_deliver(event);

    let welcome = Welcome::new(&tcsb_b);
    let event = tcsb_b.tc_bcast(Message::Membership(Membership::Welcome(welcome)));
    tcsb_a.tc_deliver(event);

    let event = tcsb_a.tc_bcast(Message::Membership(Membership::Leave));
    tcsb_b.tc_deliver(event);

    let _ = tcsb_a.tc_bcast(Message::Op(Op::Write("a")));
    let _ = tcsb_b.tc_bcast(Message::Op(Op::Write("b")));

    assert_eq!(tcsb_a.eval(), vec!["a"]);
    assert_eq!(tcsb_b.eval(), vec!["b"]);
    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_b.ltm.len(), 1);
}
