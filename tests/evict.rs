use po_crdt::{
    crdt::mv_register::Op,
    protocol::{
        event::Message,
        membership::{Membership, Welcome},
        pure_crdt::PureCRDT,
        tcsb::Tcsb,
    },
};
use std::fmt::Debug;
use std::{thread, time};

fn triplets<O: PureCRDT + Clone + Debug>() -> (
    Tcsb<&'static str, u64, O>,
    Tcsb<&'static str, u64, O>,
    Tcsb<&'static str, u64, O>,
) {
    let mut tcsb_a = Tcsb::<&str, u64, O>::new("a");
    let mut tcsb_b = Tcsb::<&str, u64, O>::new("b");
    let mut tcsb_c = Tcsb::<&str, u64, O>::new("c");

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

    assert_eq!(tcsb_a.ltm.len(), 3);
    assert_eq!(tcsb_b.ltm.len(), 3);
    assert_eq!(tcsb_c.ltm.len(), 3);

    (tcsb_a, tcsb_b, tcsb_c)
}

/// A node evicts another node.
#[test_log::test]
fn simple_evict() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplets::<Op<&str>>();

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

/// A node emits events concurrently to its eviction.
#[test_log::test]
fn evict_concurrent_whith_another_event() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplets::<Op<&str>>();

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

/// A node is evicted and then rejoin the network.
#[test_log::test]
fn evict_then_rejoin() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplets::<Op<&str>>();

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

/// A node is evicted while it is leaving the network.
#[test_log::test]
fn evict_while_leaving() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplets::<Op<&str>>();

    let event_b = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));
    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Leave));

    tcsb_a.tc_deliver(event_b.clone());
    tcsb_c.tc_deliver(event_a.clone());
    tcsb_c.tc_deliver(event_b);
    tcsb_b.tc_deliver(event_a);

    let event = tcsb_c.tc_bcast(Message::Op(Op::Write("z")));
    tcsb_b.tc_deliver(event.clone());
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_c.ltm.len(), 2);
}

/// A node is evicted but rejoins the network while the eviction event has not yet stabilized.
#[ignore]
#[test_log::test]
fn evict_while_rejoin() {
    // Should the node change its id when rejoining while the eviction process is ongoing?
    // Should we prevent the node to rejoin the network while the eviction process is ongoing?
    todo!()
}

/// Multiple nodes concurrently try to evict the same node.
#[test_log::test]
fn multiple_concurrent_evict_of_the_same_node() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplets::<Op<&str>>();

    let event_b = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));
    let event_c = tcsb_c.tc_bcast(Message::Membership(Membership::Evict("a")));

    tcsb_a.tc_deliver(event_b.clone());
    tcsb_a.tc_deliver(event_c.clone());
    tcsb_c.tc_deliver(event_b);
    tcsb_b.tc_deliver(event_c);

    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_c.ltm.len(), 2);
    assert_eq!(tcsb_a.ltm.len(), 1);
}

/// A node tries to evict multiple other nodes.
#[ignore]
#[test_log::test]
fn evict_multiple_nodes() {
    todo!()
}

/// Two nodes concurrently try to evict each other.
#[test_log::test]
fn evict_while_evicting() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplets::<Op<&str>>();

    let event_b = tcsb_b.tc_bcast(Message::Membership(Membership::Evict("a")));

    // Prevent `event_b` and `event_a` to be delivered at the same time (would result in comparing their id)
    let ten_millis = time::Duration::from_millis(10);
    thread::sleep(ten_millis);

    let event_a = tcsb_a.tc_bcast(Message::Membership(Membership::Evict("b")));
    tcsb_a.tc_deliver(event_b.clone());
    tcsb_c.tc_deliver(event_a.clone());
    tcsb_c.tc_deliver(event_b);
    tcsb_b.tc_deliver(event_a);

    let event = tcsb_c.tc_bcast(Message::Op(Op::Write("z")));
    tcsb_b.tc_deliver(event.clone());
    tcsb_a.tc_deliver(event);

    assert_eq!(tcsb_b.ltm.len(), 2);
    assert_eq!(tcsb_a.ltm.len(), 1);
    assert_eq!(tcsb_c.ltm.len(), 2);
}
