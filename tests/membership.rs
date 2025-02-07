use po_crdt::{
    crdt::{counter::Counter, test_util::triplet_po},
    protocol::{po_log::POLog, pulling::Since, tcsb::Tcsb},
};

fn batch(from: Vec<&Tcsb<POLog<Counter<i32>>>>, to: &mut Tcsb<POLog<Counter<i32>>>) {
    for f in from {
        if to.stable_across_views().contains(&&f.id) {
            let batch = f.events_since(&Since::new_from(to));
            to.deliver_batch(batch);
        }
    }
}

#[test_log::test]
fn join_new_group() {
    let mut tcsb_a = Tcsb::<POLog<Counter<i32>>>::new("a");
    let mut tcsb_b = Tcsb::<POLog<Counter<i32>>>::new("b");

    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Dec(1));

    let _ = tcsb_b.tc_bcast(Counter::Inc(7));
    let _ = tcsb_b.tc_bcast(Counter::Dec(11));
    let _ = tcsb_b.tc_bcast(Counter::Dec(9));

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_a.start_installing_view();
    tcsb_a.mark_view_installed();
    tcsb_b.state_transfer(&mut tcsb_a);

    assert_eq!(tcsb_a.group_members(), tcsb_a.group_members(),);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
}

#[test_log::test]
fn join_existing_group() {
    let mut tcsb_a = Tcsb::<POLog<Counter<i32>>>::new("a");
    let mut tcsb_b = Tcsb::<POLog<Counter<i32>>>::new("b");
    let mut tcsb_c = Tcsb::<POLog<Counter<i32>>>::new("c");

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_a.start_installing_view();
    tcsb_a.mark_view_installed();

    tcsb_b.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_b.start_installing_view();
    tcsb_b.mark_view_installed();

    let event_a_1 = tcsb_a.tc_bcast(Counter::Inc(1));
    let event_b_1 = tcsb_b.tc_bcast(Counter::Inc(7));
    tcsb_b.try_deliver(event_a_1);
    tcsb_a.try_deliver(event_b_1);

    let event_a_2 = tcsb_a.tc_bcast(Counter::Inc(1));
    tcsb_b.try_deliver(event_a_2);

    let event_a_3 = tcsb_a.tc_bcast(Counter::Dec(1));
    tcsb_b.try_deliver(event_a_3);

    let event_b_2 = tcsb_b.tc_bcast(Counter::Dec(11));
    let event_b_3 = tcsb_b.tc_bcast(Counter::Dec(9));
    tcsb_a.try_deliver(event_b_2);
    tcsb_a.try_deliver(event_b_3);

    assert_eq!(tcsb_a.eval(), tcsb_b.eval());

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    tcsb_a.start_installing_view();

    tcsb_b.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    tcsb_b.start_installing_view();

    let batch_from_a = tcsb_a.events_since(&Since::new_from(&tcsb_b));
    tcsb_b.deliver_batch(batch_from_a);

    let batch_from_b = tcsb_b.events_since(&Since::new_from(&tcsb_a));
    tcsb_a.deliver_batch(batch_from_b);

    tcsb_a.mark_view_installed();
    tcsb_b.mark_view_installed();

    tcsb_c.state_transfer(&mut tcsb_a);

    assert_eq!(tcsb_a.group_members(), tcsb_b.group_members());
    assert_eq!(tcsb_a.group_members(), tcsb_c.group_members());
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), tcsb_c.eval());
}

#[test_log::test]
fn leave() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet_po::<Counter<i32>>();

    let event_a = tcsb_a.tc_bcast(Counter::Inc(1));
    let event_b = tcsb_b.tc_bcast(Counter::Inc(7));

    tcsb_b.try_deliver(event_a.clone());
    tcsb_a.try_deliver(event_b.clone());
    tcsb_c.try_deliver(event_a);
    tcsb_c.try_deliver(event_b);

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_a.start_installing_view();

    let event_c = tcsb_c.tc_bcast(Counter::Inc(3));

    tcsb_b.try_deliver(event_c.clone());
    tcsb_a.try_deliver(event_c);

    tcsb_b.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_b.start_installing_view();

    tcsb_c.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_c.start_installing_view();

    batch(vec![&tcsb_c, &tcsb_b], &mut tcsb_a);
    batch(vec![&tcsb_a, &tcsb_c], &mut tcsb_b);
    batch(vec![&tcsb_a, &tcsb_b], &mut tcsb_c);

    for tcsb in [&mut tcsb_a, &mut tcsb_c, &mut tcsb_b] {
        tcsb.mark_view_installed();
    }

    assert_eq!(tcsb_a.group_members(), tcsb_b.group_members());
    assert_eq!(&vec!["c".to_string()], tcsb_c.group_members());
    assert_eq!(tcsb_c.eval(), 11);
    assert_eq!(tcsb_a.eval(), 11);
    assert_eq!(tcsb_b.eval(), 11);
}

#[test_log::test]
fn rejoin() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet_po::<Counter<i32>>();

    let event_a = tcsb_a.tc_bcast(Counter::Inc(1));
    tcsb_b.try_deliver(event_a.clone());
    tcsb_c.try_deliver(event_a);

    let event_c = tcsb_c.tc_bcast(Counter::Inc(3));
    tcsb_a.try_deliver(event_c.clone());
    tcsb_b.try_deliver(event_c);

    for tcsb in [&mut tcsb_a, &mut tcsb_c, &mut tcsb_b] {
        tcsb.add_pending_view(vec!["a".to_string(), "b".to_string()]);
        tcsb.start_installing_view();
    }

    batch(vec![&tcsb_b, &tcsb_c], &mut tcsb_a);
    batch(vec![&tcsb_a, &tcsb_c], &mut tcsb_b);
    batch(vec![&tcsb_a, &tcsb_b], &mut tcsb_c);

    for tcsb in [&mut tcsb_a, &mut tcsb_c, &mut tcsb_b] {
        tcsb.mark_view_installed();
    }

    assert_eq!(tcsb_a.group_members(), tcsb_b.group_members());
    assert_eq!(tcsb_c.group_members(), &vec!["c".to_string()]);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), tcsb_c.eval());

    let event_b = tcsb_b.tc_bcast(Counter::Inc(7));
    tcsb_a.try_deliver(event_b);

    for tcsb in [&mut tcsb_a, &mut tcsb_c, &mut tcsb_b] {
        tcsb.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
        tcsb.start_installing_view();
    }

    batch(vec![&tcsb_b, &tcsb_c], &mut tcsb_a);
    batch(vec![&tcsb_a, &tcsb_c], &mut tcsb_b);
    batch(vec![&tcsb_a, &tcsb_b], &mut tcsb_c);

    for tcsb in [&mut tcsb_a, &mut tcsb_c, &mut tcsb_b] {
        tcsb.mark_view_installed();
    }

    tcsb_c.state_transfer(&mut tcsb_a);

    assert_eq!(tcsb_a.group_members(), tcsb_b.group_members());
    assert_eq!(tcsb_c.group_members(), tcsb_b.group_members());
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), tcsb_c.eval());
}

#[test_log::test]
fn operations_while_installing() {
    let (mut tcsb_a, _, _) = triplet_po::<Counter<i32>>();

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
    tcsb_a.add_pending_view(vec![
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
        "d".to_string(),
    ]);
    tcsb_a.add_pending_view(vec!["a".to_string(), "c".to_string(), "d".to_string()]);

    tcsb_a.planning(tcsb_a.last_view_id());
    tcsb_a.start_installing_view();

    let _ = tcsb_a.tc_bcast(Counter::Inc(-1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(2));
    let _ = tcsb_a.tc_bcast(Counter::Inc(-3));
    let _ = tcsb_a.tc_bcast(Counter::Inc(11));

    while tcsb_a
        .last_planned_id()
        .is_some_and(|id| id > tcsb_a.view_id())
    {
        tcsb_a.mark_view_installed();
        tcsb_a.start_installing_view();
    }

    assert_eq!(tcsb_a.eval(), 9);
    assert_eq!(tcsb_a.view_id(), 4);
}
