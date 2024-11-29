#[cfg(feature = "utils")]
use camino::Utf8PathBuf;
use po_crdt::crdt::{
    counter::Counter,
    membership_set::MSet,
    test_util::{quadruplet, twins},
};

#[test_log::test]
fn leave() {
    let (mut tcsb_a, mut tcsb_b) = twins();

    let event = tcsb_a.tc_bcast_op(Counter::Inc(5));
    tcsb_b.tc_deliver_op(event);

    let event = tcsb_b.tc_bcast_op(Counter::Dec(5));
    tcsb_a.tc_deliver_op(event);

    let event = tcsb_a.tc_bcast_membership(MSet::remove("a"));

    assert_eq!(tcsb_a.ltm.keys(), vec!["a"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b"]);

    tcsb_b.tc_deliver_membership(event);

    assert_eq!(tcsb_a.ltm.keys(), vec!["a"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["b"]);
}

#[test_log::test]
fn leave_and_evict() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c, mut tcsb_d) = quadruplet::<Counter<i32>>();

    let event = tcsb_a.tc_bcast_membership(MSet::remove("a"));
    tcsb_b.tc_deliver_membership(event.clone());
    tcsb_c.tc_deliver_membership(event.clone());
    tcsb_d.tc_deliver_membership(event);

    let event = tcsb_c.tc_bcast_membership(MSet::remove("b"));
    tcsb_a.tc_deliver_membership(event.clone());
    tcsb_b.tc_deliver_membership(event.clone());
    tcsb_d.tc_deliver_membership(event);
}
