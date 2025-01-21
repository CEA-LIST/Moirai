use po_crdt::{
    crdt::{counter::Counter, test_util::triplet},
    protocol::{pulling::Since, tcsb::Tcsb},
};

#[test_log::test]
fn join_new_group() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");

    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Inc(1));
    let _ = tcsb_a.tc_bcast(Counter::Dec(1));

    let _ = tcsb_b.tc_bcast(Counter::Inc(7));
    let _ = tcsb_b.tc_bcast(Counter::Dec(11));
    let _ = tcsb_b.tc_bcast(Counter::Dec(9));

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_a.start_installing_view();
    tcsb_a.mark_installed_view();
    tcsb_b.state_transfer(&mut tcsb_a);

    assert_eq!(tcsb_a.group_members(), tcsb_a.group_members(),);
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
}

#[test_log::test]
fn join_existing_group() {
    let mut tcsb_a = Tcsb::<Counter<i32>>::new("a");
    let mut tcsb_b = Tcsb::<Counter<i32>>::new("b");
    let mut tcsb_c = Tcsb::<Counter<i32>>::new("c");

    tcsb_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_a.start_installing_view();
    tcsb_a.mark_installed_view();

    tcsb_b.add_pending_view(vec!["a".to_string(), "b".to_string()]);
    tcsb_b.start_installing_view();
    tcsb_b.mark_installed_view();

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

    tcsb_a.mark_installed_view();
    tcsb_b.mark_installed_view();

    tcsb_c.state_transfer(&mut tcsb_a);

    assert_eq!(tcsb_a.group_members(), tcsb_b.group_members());
    assert_eq!(tcsb_a.group_members(), tcsb_c.group_members());
    assert_eq!(tcsb_a.eval(), tcsb_b.eval());
    assert_eq!(tcsb_a.eval(), tcsb_c.eval());
}

#[test_log::test]
fn leave() {
    let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet::<Counter<i32>>();

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

    let batch = |from: Vec<&Tcsb<Counter<i32>>>, to: &mut Tcsb<Counter<i32>>| {
        for f in from {
            if to
                .stable_members_in_transition()
                .is_some_and(|v| v.contains(&&f.id))
            {
                let batch = f.events_since(&Since::new_from(to));
                to.deliver_batch(batch);
            }
        }
    };

    batch(vec![&tcsb_c, &tcsb_b], &mut tcsb_a);
    batch(vec![&tcsb_a, &tcsb_c], &mut tcsb_b);
    batch(vec![&tcsb_a, &tcsb_b], &mut tcsb_c);

    tcsb_a.mark_installed_view();
    tcsb_b.mark_installed_view();
    tcsb_c.mark_installed_view();

    assert_eq!(tcsb_a.group_members(), tcsb_b.group_members());
    assert_eq!(&vec!["c".to_string()], tcsb_c.group_members());
    assert_eq!(tcsb_c.eval(), 11);
    assert_eq!(tcsb_a.eval(), 11);
    assert_eq!(tcsb_b.eval(), 11);
}
