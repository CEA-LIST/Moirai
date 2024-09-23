// use std::{
//     collections::{HashMap, HashSet},
//     sync::Arc,
//     time::Instant,
// };

// use deepsize::DeepSizeOf;
// use po_crdt::{
//     crdt::{aw_set::AWSet, uw_map::UWMap},
//     protocol::tcsb::Tcsb,
// };
// use rand::{random, thread_rng, Rng};

// fn generate_ops(tcsb: &mut Tcsb<AWSet<String>>, observer: &mut HashSet<String>, size: usize) {
//     for i in 0..size {
//         let random = random::<bool>();
//         if random {
//             let num = thread_rng().gen_range(0..i + 1);
//             let item = format!("{}", num);
//             tcsb.tc_bcast_op(AWSet::Remove(item.clone()));
//             observer.remove(&item);
//         } else {
//             let item = format!("{}", i);
//             tcsb.tc_bcast_op(AWSet::Add(item.clone()));
//             observer.insert(item);
//         }
//     }
// }

// fn generate_nested_ops(
//     tcsb: &mut Tcsb<UWMap<AWSet<String>>>,
//     observer: &mut HashMap<String, HashSet<String>>,
//     size: usize,
// ) {
//     for i in 0..size {
//         let random = rand::random::<bool>();
//         if random {
//             let num = thread_rng().gen_range(0..i + 1);
//             let item = format!("{}", num);
//             tcsb.tc_bcast_op(UWMap::Remove(item.clone()));
//             observer.remove(&item);
//         } else {
//             let random_bool: bool = rand::random::<bool>();
//             if random_bool {
//                 let num = thread_rng().gen_range(0..i + 1);
//                 let item = format!("{}", num % 2);
//                 let item_2 = format!("{}", num);
//                 let nested = AWSet::Remove(item.clone());
//                 tcsb.tc_bcast_op(UWMap::Update(item_2.clone(), nested));
//                 if let Some(set) = observer.get_mut(&item_2) {
//                     set.remove(&item);
//                 }
//             } else {
//                 let num = thread_rng().gen_range(0..i + 1);
//                 let item = format!("{}", num % 2);
//                 let item_2 = format!("{}", num);
//                 let nested = AWSet::Add(item.clone());
//                 tcsb.tc_bcast_op(UWMap::Update(item_2.clone(), nested));
//                 observer
//                     .entry(item_2)
//                     .or_insert_with(HashSet::new)
//                     .insert(item);
//             }
//         }
//     }
// }

// #[test_log::test]
// fn eval_hash_set() {
//     let mut tcsb = Tcsb::<AWSet<String>>::new("a");
//     let mut observer = HashSet::new();
//     generate_ops(&mut tcsb, &mut observer, 10_000);
//     let before = Instant::now();
//     let result = tcsb.eval();
//     println!("{:?}", result);
//     println!("Elapsed time: {:.2?}", before.elapsed());
// println!(
//     "Deepsize of observer: {:.2} Mo",
//     observer.deep_size_of() as f64 / (1024.0 * 1024.0)
// );
// println!(
//     "Deepsize of CRDT state: {:.2} Mo",
//     tcsb.state.stable.deep_size_of() as f64 / (1024.0 * 1024.0)
// );
//     assert_eq!(result, observer);
// }

// #[test_log::test]
// fn eval_nested() {
//     let mut tcsb = Tcsb::<UWMap<AWSet<String>>>::new("a");
//     let mut observer = HashMap::new();
//     generate_nested_ops(&mut tcsb, &mut observer, 10_000);
//     let before = Instant::now();
//     let result = tcsb.eval();
//     println!("{:?}", result);
//     println!("Elapsed time: {:.2?}", before.elapsed());
//     println!(
//         "Deepsize of observer: {:.2} Mo",
//         observer.deep_size_of() as f64 / (1024.0 * 1024.0)
//     );
//     println!(
//         "Deepsize of result: {:.2} Mo",
//         result.deep_size_of() as f64 / (1024.0 * 1024.0)
//     );
//     assert_eq!(result, observer);
// }
