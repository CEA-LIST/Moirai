fn main() {}

// use po_crdt::crdt::{counter::Counter, test_util::quadruplet_graph};

// fn main() {
//     let (tcsb_a, tcsb_b, tcsb_c, tcsb_d) = quadruplet_graph::<Counter<isize>>();

//     let mut tcsb_arr = [tcsb_a, tcsb_b, tcsb_c, tcsb_d];

//     for x in 0..1 {
//         for i in 0..tcsb_arr.len() {
//             let op = if x % 2 == 0 {
//                 Counter::Inc(1)
//             } else {
//                 Counter::Dec(1)
//             };
//             let event = tcsb_arr[i].tc_bcast(op);
//             for j in 0..tcsb_arr.len() {
//                 if i != j {
//                     tcsb_arr[j].try_deliver(event.clone());
//                 }
//             }
//         }
//     }

//     env_logger::init();

//     for (i, tcsb) in tcsb_arr.iter().enumerate() {
//         log::info!(
//             "TCSB {} : stable ops: {} - unstable ops: {}",
//             i,
//             tcsb.state.stable.len(),
//             tcsb.state.unstable.node_count()
//         );
//         log::info!(
//             "TCSB {} : unstable node capacity: {} - unstable edge capacity: {}",
//             i,
//             tcsb.state.unstable.capacity().0,
//             tcsb.state.unstable.capacity().1,
//         );
//     }
// }
