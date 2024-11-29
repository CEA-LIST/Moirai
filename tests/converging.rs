#[cfg(feature = "utils")]
use camino::Utf8PathBuf;
use po_crdt::{
    crdt::{counter::Counter, membership_set::MSet, test_util::triplet},
    protocol::tcsb::Tcsb,
};

#[test_log::test]
fn converging_members() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<Counter<i32>>();

    let mut tcsb_d = Tcsb::<Counter<i32>>::new("d");

    let event_a = tcsb_a.tc_bcast_membership(MSet::add("d"));
    tcsb_b.tc_deliver_membership(event_a.clone());
    tcsb_c.tc_deliver_membership(event_a);

    let event_a = tcsb_a.tc_bcast_op(Counter::Inc(1));

    let event_b = tcsb_b.tc_bcast_op(Counter::Inc(1));
    tcsb_a.tc_deliver_op(event_b.clone());
    tcsb_c.tc_deliver_op(event_b);

    let event_c = tcsb_c.tc_bcast_op(Counter::Inc(1));
    tcsb_a.tc_deliver_op(event_c.clone());
    tcsb_b.tc_deliver_op(event_c);

    assert_eq!(tcsb_a.converging_members.len(), 0);
    assert_eq!(tcsb_b.converging_members.len(), 1);
    assert_eq!(tcsb_c.converging_members.len(), 1);
    assert_eq!(tcsb_a.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_b.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(tcsb_c.ltm.keys(), vec!["a", "b", "c", "d"]);
    assert_eq!(
        tcsb_a.ltm.get(&"a".to_string()).unwrap(),
        tcsb_a.ltm.get(&"d".to_string()).unwrap()
    );
    assert_eq!(
        tcsb_b.ltm.get(&"a".to_string()).unwrap(),
        tcsb_b.ltm.get(&"d".to_string()).unwrap()
    );
    assert_eq!(
        tcsb_c.ltm.get(&"a".to_string()).unwrap(),
        tcsb_c.ltm.get(&"d".to_string()).unwrap()
    );

    tcsb_b.tc_deliver_op(event_a.clone());
    tcsb_c.tc_deliver_op(event_a);

    assert_eq!(
        tcsb_b.ltm.get(&"a".to_string()).unwrap(),
        tcsb_b.ltm.get(&"d".to_string()).unwrap()
    );
    assert_eq!(
        tcsb_c.ltm.get(&"a".to_string()).unwrap(),
        tcsb_c.ltm.get(&"d".to_string()).unwrap()
    );

    tcsb_d.state_transfer(&mut tcsb_a);

    let event_d = tcsb_d.tc_bcast_op(Counter::Inc(1));
    tcsb_a.tc_deliver_op(event_d.clone());
    tcsb_b.tc_deliver_op(event_d.clone());
    tcsb_c.tc_deliver_op(event_d);

    assert_eq!(tcsb_a.converging_members.len(), 0);
    assert_eq!(tcsb_b.converging_members.len(), 0);
    assert_eq!(tcsb_c.converging_members.len(), 0);
    assert_eq!(tcsb_d.converging_members.len(), 0);
}
