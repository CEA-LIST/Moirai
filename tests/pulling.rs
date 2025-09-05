// #![cfg(feature = "crdt")]

// // Tests for event pulling in CRDTs.

// use std::collections::HashSet;

// use moirai::{
//     crdt::{counter::resettable_counter::Counter, set::aw_set::AWSet, test_util::twins_graph},
//     protocol::pulling::Since,
// };

// #[test]
// fn events_since_concurrent_counter() {
//     let (mut replica_a, mut replica_b) = twins::<Counter<i32>>();

//     let _ = replica_a.send(Counter::Inc(1));
//     let _ = replica_a.send(Counter::Inc(1));
//     let _ = replica_a.send(Counter::Inc(1));
//     let _ = replica_a.send(Counter::Inc(1));
//     let _ = replica_a.send(Counter::Inc(1));
//     let _ = replica_a.send(Counter::Inc(1));
//     assert_eq!(6, replica_a.query());
//     assert_eq!(6, replica_a.state.unstable.node_count());

//     let _ = replica_b.send(Counter::Dec(1));
//     let _ = replica_b.send(Counter::Dec(1));
//     let _ = replica_b.send(Counter::Dec(1));
//     let _ = replica_b.send(Counter::Dec(1));
//     let _ = replica_b.send(Counter::Dec(1));
//     let _ = replica_b.send(Counter::Dec(1));
//     assert_eq!(-6, replica_b.query());
//     assert_eq!(6, replica_b.state.unstable.node_count());

//     let batch = replica_a.events_since(&Since::new_from(&replica_b));
//     assert_eq!(6, batch.clone().unwrap().events.len());

//     replica_b.deliver_batch(batch);

//     let batch = replica_b.events_since(&Since::new_from(&replica_a));
//     assert_eq!(6, batch.clone().unwrap().events.len());

//     replica_a.deliver_batch(batch);

//     assert_eq!(replica_a.pending.len(), 0);
//     assert_eq!(replica_b.pending.len(), 0);
//     assert_eq!(replica_a.query(), 0);
//     assert_eq!(replica_b.query(), 0);
// }

// #[test]
// fn event_since_concurrent_aw_set() {
//     let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

//     let _ = replica_a.send(AWSet::Add("a"));
//     let _ = replica_a.send(AWSet::Add("b"));
//     let _ = replica_a.send(AWSet::Add("c"));
//     let _ = replica_a.send(AWSet::Remove("a"));

//     let _ = replica_b.send(AWSet::Add("a"));
//     let _ = replica_b.send(AWSet::Add("e"));
//     let _ = replica_b.send(AWSet::Add("p"));
//     let _ = replica_b.send(AWSet::Remove("e"));

//     let batch = replica_a.events_since(&Since::new_from(&replica_b));
//     replica_b.deliver_batch(batch);

//     let batch = replica_b.events_since(&Since::new_from(&replica_a));
//     replica_a.deliver_batch(batch);

//     assert_eq!(replica_a.pending.len(), 0);
//     assert_eq!(replica_b.pending.len(), 0);
//     assert_eq!(replica_a.query(), replica_b.query());
//     assert_eq!(replica_a.query(), HashSet::from(["a", "b", "c", "p"]));
// }

// #[test]
// fn event_since_concurrent_complex_aw_set() {
//     let (mut replica_a, mut replica_b) = twins::<AWSet<&str>>();

//     let event = replica_a.send(AWSet::Add("a"));
//     replica_b.receive(event);

//     let _ = replica_a.send(AWSet::Add("b"));
//     let _ = replica_a.send(AWSet::Add("c"));
//     let _ = replica_a.send(AWSet::Remove("a"));

//     let _ = replica_b.send(AWSet::Add("e"));
//     let _ = replica_b.send(AWSet::Add("p"));
//     let _ = replica_b.send(AWSet::Remove("e"));

//     let since = Since::new_from(&replica_b);
//     let batch = replica_a.events_since(&since);
//     assert_eq!(batch.clone().unwrap().events.len(), 3);
//     replica_b.deliver_batch(batch);

//     let since = Since::new_from(&replica_a);
//     let batch = replica_b.events_since(&since);
//     assert_eq!(batch.clone().unwrap().events.len(), 3);
//     replica_a.deliver_batch(batch);

//     assert_eq!(replica_a.pending.len(), 0);
//     assert_eq!(replica_b.pending.len(), 0);
//     assert_eq!(replica_a.query(), replica_b.query());
//     assert_eq!(replica_a.query(), HashSet::from(["b", "c", "p"]));
// }
