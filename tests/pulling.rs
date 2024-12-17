use po_crdt::{
    crdt::{counter::Counter, test_util::twins},
    protocol::metadata::Metadata,
};

#[test_log::test]
fn events_since_concurrent_counter() {
    let (mut tcsb_a, mut tcsb_b) = twins::<Counter<i32>>();

    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    assert_eq!(6, tcsb_a.eval());
    assert_eq!(6, tcsb_a.state.unstable.len());

    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    let _ = tcsb_b.tc_bcast(Counter::Dec(1));
    assert_eq!(-6, tcsb_b.eval());
    assert_eq!(6, tcsb_b.state.unstable.len());

    let batch = tcsb_a.events_since(&Metadata::new(tcsb_b.my_clock().clone(), &tcsb_b.id));
    assert_eq!(6, batch.clone().unwrap().events.len());

    tcsb_b.deliver_batch(batch);

    let batch = tcsb_b.events_since(&Metadata::new(tcsb_a.my_clock().clone(), &tcsb_a.id));
    assert_eq!(6, batch.clone().unwrap().events.len());

    tcsb_a.deliver_batch(batch);

    assert_eq!(tcsb_a.eval(), 0);
    assert_eq!(tcsb_b.eval(), 0);
}
