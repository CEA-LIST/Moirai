// use crate::{
//     crdt::test_util::twins,
//     protocol::{event_graph::EventGraph, log::Log, pure_crdt::PureCRDT},
// };

// pub fn converge<O: PureCRDT>(op1: O, op2: O) -> bool {
//     let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<O>>();

//     let event_a = tcsb_a.tc_bcast(op1.clone());
//     let event_b = tcsb_b.tc_bcast(op2.clone());

//     tcsb_a.try_deliver(event_b);
//     tcsb_b.try_deliver(event_a);

//     tcsb_a.eval() == tcsb_b.eval()
// }

// pub fn converge_all<O: PureCRDT>(ops: Vec<O>, state: O::Value) {
//     let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<O>>();

//     for op in &ops {
//         for o in &ops {
//             let event_a = tcsb_a.tc_bcast(o.clone());
//             let event_b = tcsb_b.tc_bcast(op.clone());

//             tcsb_a.try_deliver(event_b);
//             tcsb_b.try_deliver(event_a);
//         }

//         assert_eq!(state, tcsb_a.eval());
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }
// }

// pub fn converge_all_log<L: Log>(ops: Vec<L::Op>, state: L::Value) {
//     let (mut tcsb_a, mut tcsb_b) = twins::<L>();

//     for op in &ops {
//         for o in &ops {
//             let event_a = tcsb_a.tc_bcast(o.clone());
//             let event_b = tcsb_b.tc_bcast(op.clone());

//             tcsb_a.try_deliver(event_b);
//             tcsb_b.try_deliver(event_a);
//         }

//         assert_eq!(state, tcsb_a.eval());
//         assert_eq!(tcsb_a.eval(), tcsb_b.eval());
//     }
// }
