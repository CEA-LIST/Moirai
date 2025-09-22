// #![cfg(feature = "crdt")]

// // Tests for group membership in CRDTs. The group membership protocol
// // used is based on the View-Synchronous Communication (VSC) model.
// // This means that views are totally ordered and all members of the group
// // are aware of the current view.

// use moirai::{
//     crdt::{counter::resettable_counter::Counter, test_util::triplet},
//     protocol::{event_graph::EventGraph, pulling::Since, tcsb::Tcsb},
// };

// fn batch(from: Vec<&Tcsb<EventGraph<Counter<i32>>>>, to: &mut Tcsb<EventGraph<Counter<i32>>>) {
//     for f in from {
//         if to.group_membership.stable_across_views().contains(&&f.id) {
//             let batch = f.events_since(&Since::new_from(to));
//             to.deliver_batch(batch);
//         }
//     }
// }

// #[test]
// fn join_new_group() {
//     let mut replica_a = Tcsb::<EventGraph<Counter<i32>>>::new("a");
//     let mut replica_b = Tcsb::<EventGraph<Counter<i32>>>::new("b");

//     let _ = replica_a.send(Counter::Inc(1)).unwrap();
//     let _ = replica_a.send(Counter::Inc(1)).unwrap();
//     let _ = replica_a.send(Counter::Dec(1)).unwrap();

//     let _ = replica_b.send(Counter::Inc(7)).unwrap();
//     let _ = replica_b.send(Counter::Dec(11)).unwrap();
//     let _ = replica_b.send(Counter::Dec(9)).unwrap();

//     replica_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//     replica_a.start_installing_view();
//     replica_a.mark_view_installed();
//     replica_b.state_transfer(&mut replica_a);

//     assert_eq!(replica_a.group_members(), replica_a.group_members(),);
//     assert_eq!(replica_a.query(), replica_b.query());
// }

// #[test]
// fn join_existing_group() {
//     let mut replica_a = Tcsb::<EventGraph<Counter<i32>>>::new("a");
//     let mut replica_b = Tcsb::<EventGraph<Counter<i32>>>::new("b");
//     let mut replica_c = Tcsb::<EventGraph<Counter<i32>>>::new("c");

//     replica_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//     replica_a.start_installing_view();
//     replica_a.mark_view_installed();

//     replica_b.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//     replica_b.start_installing_view();
//     replica_b.mark_view_installed();

//     let event_a_1 = replica_a.send(Counter::Inc(1)).unwrap();
//     let event_b_1 = replica_b.send(Counter::Inc(7)).unwrap();
//     replica_b.receive(event_a_1);
//     replica_a.receive(event_b_1);

//     let event_a_2 = replica_a.send(Counter::Inc(1)).unwrap();
//     replica_b.receive(event_a_2);

//     let event_a_3 = replica_a.send(Counter::Dec(1)).unwrap();
//     replica_b.receive(event_a_3);

//     let event_b_2 = replica_b.send(Counter::Dec(11)).unwrap();
//     let event_b_3 = replica_b.send(Counter::Dec(9)).unwrap();
//     replica_a.receive(event_b_2);
//     replica_a.receive(event_b_3);

//     assert_eq!(replica_a.query(), replica_b.query());

//     replica_a.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
//     replica_a.start_installing_view();

//     replica_b.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
//     replica_b.start_installing_view();

//     let batch_from_a = replica_a.events_since(&Since::new_from(&replica_b));
//     replica_b.deliver_batch(batch_from_a);

//     let batch_from_b = replica_b.events_since(&Since::new_from(&replica_a));
//     replica_a.deliver_batch(batch_from_b);

//     replica_a.mark_view_installed();
//     replica_b.mark_view_installed();

//     replica_c.state_transfer(&mut replica_a);

//     assert_eq!(replica_a.group_members(), replica_b.group_members());
//     assert_eq!(replica_a.group_members(), replica_c.group_members());
//     assert_eq!(replica_a.query(), replica_b.query());
//     assert_eq!(replica_a.query(), replica_c.query());
// }

// #[test]
// fn leave() {
//     let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Counter<i32>>();

//     let event_a = replica_a.send(Counter::Inc(1)).unwrap();
//     let event_b = replica_b.send(Counter::Inc(7)).unwrap();

//     replica_b.receive(event_a.clone());
//     replica_a.receive(event_b.clone());
//     replica_c.receive(event_a);
//     replica_c.receive(event_b);

//     replica_a.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//     replica_a.start_installing_view();

//     let event_c = replica_c.send(Counter::Inc(3)).unwrap();

//     replica_b.receive(event_c.clone());
//     replica_a.receive(event_c);

//     replica_b.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//     replica_b.start_installing_view();

//     replica_c.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//     replica_c.start_installing_view();

//     batch(vec![&replica_c, &replica_b], &mut replica_a);
//     batch(vec![&replica_a, &replica_c], &mut replica_b);
//     batch(vec![&replica_a, &replica_b], &mut replica_c);

//     for tcsb in [&mut replica_a, &mut replica_c, &mut replica_b] {
//         tcsb.mark_view_installed();
//     }

//     assert_eq!(replica_a.group_members(), replica_b.group_members());
//     assert_eq!(&vec!["c".to_string()], replica_c.group_members());
//     assert_eq!(replica_c.query(), 11);
//     assert_eq!(replica_a.query(), 11);
//     assert_eq!(replica_b.query(), 11);
// }

// #[test]
// fn rejoin() {
//     let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Counter<i32>>();

//     let event_a = replica_a.send(Counter::Inc(1)).unwrap();
//     replica_b.receive(event_a.clone());
//     replica_c.receive(event_a);

//     let event_c = replica_c.send(Counter::Inc(3)).unwrap();
//     replica_a.receive(event_c.clone());
//     replica_b.receive(event_c);

//     for tcsb in [&mut replica_a, &mut replica_c, &mut replica_b] {
//         tcsb.add_pending_view(vec!["a".to_string(), "b".to_string()]);
//         tcsb.start_installing_view();
//     }

//     batch(vec![&replica_b, &replica_c], &mut replica_a);
//     batch(vec![&replica_a, &replica_c], &mut replica_b);
//     batch(vec![&replica_a, &replica_b], &mut replica_c);

//     for tcsb in [&mut replica_a, &mut replica_c, &mut replica_b] {
//         tcsb.mark_view_installed();
//     }

//     assert_eq!(replica_a.group_members(), replica_b.group_members());
//     assert_eq!(replica_c.group_members(), &vec!["c".to_string()]);
//     assert_eq!(replica_a.query(), replica_b.query());
//     assert_eq!(replica_a.query(), replica_c.query());

//     let event_b = replica_b.send(Counter::Inc(7)).unwrap();
//     replica_a.receive(event_b);

//     for tcsb in [&mut replica_a, &mut replica_c, &mut replica_b] {
//         tcsb.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
//         tcsb.start_installing_view();
//     }

//     batch(vec![&replica_b, &replica_c], &mut replica_a);
//     batch(vec![&replica_a, &replica_c], &mut replica_b);
//     batch(vec![&replica_a, &replica_b], &mut replica_c);

//     for tcsb in [&mut replica_a, &mut replica_c, &mut replica_b] {
//         tcsb.mark_view_installed();
//     }

//     replica_c.state_transfer(&mut replica_a);

//     assert_eq!(replica_a.group_members(), replica_b.group_members());
//     assert_eq!(replica_c.group_members(), replica_b.group_members());
//     assert_eq!(replica_a.query(), replica_b.query());
//     assert_eq!(replica_a.query(), replica_c.query());
// }

// #[test]
// fn operations_while_installing() {
//     let (mut replica_a, _, _) = triplet::<Counter<i32>>();

//     replica_a.add_pending_view(vec!["a".to_string(), "b".to_string(), "c".to_string()]);
//     replica_a.add_pending_view(vec![
//         "a".to_string(),
//         "b".to_string(),
//         "c".to_string(),
//         "d".to_string(),
//     ]);
//     replica_a.add_pending_view(vec!["a".to_string(), "c".to_string(), "d".to_string()]);

//     replica_a.group_membership.planning(replica_a.last_view_id());
//     replica_a.start_installing_view();

//     let _ = replica_a.send(Counter::Inc(-1)).unwrap();
//     let _ = replica_a.send(Counter::Inc(2)).unwrap();
//     let _ = replica_a.send(Counter::Inc(-3)).unwrap();
//     let _ = replica_a.send(Counter::Inc(11)).unwrap();

//     while replica_a
//         .group_membership
//         .last_planned_id()
//         .is_some_and(|id| id > replica_a.view_id())
//     {
//         replica_a.mark_view_installed();
//         replica_a.start_installing_view();
//     }

//     assert_eq!(replica_a.query(), 9);
//     assert_eq!(replica_a.view_id(), 5);
// }
