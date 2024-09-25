use std::path::PathBuf;

use po_crdt::{
    crdt::{counter::Counter, membership_set::MSet},
    protocol::tcsb::Tcsb,
};

fn twins() -> (Tcsb<Counter<i32>>, Tcsb<Counter<i32>>) {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new_with_trace("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new_with_trace("b");

    let event_a = tcsb_a.tc_bcast_membership(MSet::add("b"));
    let event_b = tcsb_b.tc_bcast_membership(MSet::add("a"));

    tcsb_b.tc_deliver_membership(event_a);
    tcsb_a.tc_deliver_membership(event_b);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b"]);

    (tcsb_a, tcsb_b)
}

fn quadruplet() -> (
    Tcsb<Counter<i32>>,
    Tcsb<Counter<i32>>,
    Tcsb<Counter<i32>>,
    Tcsb<Counter<i32>>,
) {
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

    // Useless event: just to exchange causal information
    let event_a = tcsb_a.tc_bcast_op(Counter::Inc(0));
    tcsb_b.tc_deliver_op(event_a);

    // --> Causal stability <--

    // State transfer
    tcsb_c.group_membership = tcsb_b.group_membership.clone();
    tcsb_c.lsv = tcsb_b.lsv.clone();
    tcsb_c.ltm = tcsb_b.ltm.clone();

    // State transfer
    tcsb_d.group_membership = tcsb_a.group_membership.clone();
    tcsb_d.lsv = tcsb_a.lsv.clone();
    tcsb_d.ltm = tcsb_a.ltm.clone();

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d"]);

    (tcsb_a, tcsb_b, tcsb_c, tcsb_d)
}

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

    let event_a = tcsb_a.tc_bcast_membership(MSet::add("b"));
    let event_b = tcsb_b.tc_bcast_membership(MSet::add("a"));

    tcsb_b.tc_deliver_membership(event_a);
    tcsb_a.tc_deliver_membership(event_b);

    // At this point, a and b are in the same group

    // b welcomes c
    let event_b = tcsb_b.tc_bcast_membership(MSet::add("c"));
    tcsb_a.tc_deliver_membership(event_b);

    // Useless event: just to exchange causal information
    let event_a = tcsb_a.tc_bcast_membership(MSet::add("c"));
    tcsb_b.tc_deliver_membership(event_a);

    // State transfer
    tcsb_c.group_membership = tcsb_b.group_membership.clone();
    tcsb_c.lsv = tcsb_b.lsv.clone();
    tcsb_c.ltm = tcsb_b.ltm.clone();

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c"]);
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

    // Useless event: just to exchange causal information
    let event_a = tcsb_a.tc_bcast_op(Counter::Inc(5));
    tcsb_b.tc_deliver_op(event_a);

    // --> Causal stability <--

    // State transfer
    tcsb_c.group_membership = tcsb_b.group_membership.clone();
    tcsb_c.lsv = tcsb_b.lsv.clone();
    tcsb_c.ltm = tcsb_b.ltm.clone();

    // State transfer
    tcsb_d.group_membership = tcsb_a.group_membership.clone();
    tcsb_d.lsv = tcsb_a.lsv.clone();
    tcsb_d.ltm = tcsb_a.ltm.clone();

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "c", "d"]);
}

#[test_log::test]
fn leave() {
    let (mut tcsb_a, mut tcsb_b) = twins();

    let event = tcsb_a.tc_bcast_op(Counter::Inc(5));
    tcsb_b.tc_deliver_op(event);

    let event = tcsb_b.tc_bcast_op(Counter::Dec(5));
    tcsb_a.tc_deliver_op(event);

    let event = tcsb_a.tc_bcast_membership(MSet::remove("a"));
    tcsb_b.tc_deliver_membership(event);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["b"]);
}

#[test_log::test]
fn evict() {
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
    tcsb_c.tc_deliver_op(event_d);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["c"]);
    assert_eq!(tcsb_d.ltm.keys(), vec!["a", "b", "d"]);

    assert_eq!(tcsb_a.eval(), 45);
    assert_eq!(tcsb_b.eval(), 45);
    assert_eq!(tcsb_c.eval(), 45);
    assert_eq!(tcsb_d.eval(), 45);

    tcsb_a
        .tracer
        .serialize_to_file(&PathBuf::from("membership_evict_a_trace.json"))
        .unwrap();
}

// #[test_log::test]
// fn evict_multiple_messages() {

// }
