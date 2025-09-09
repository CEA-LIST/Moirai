// //last version performing well with the paper test case

// use petgraph::{graph::NodeIndex, Direction};
// use std::collections::{BinaryHeap, HashMap};

// use crate::protocol::crdt::pure_crdt::PureCRDT;

// // Single-character, position-based, pure op-based CRDT operations
// #[derive(Clone, Debug, PartialEq, Eq, Hash)]
// pub enum List {
//     Insert { content: char, pos: usize },
//     Delete { pos: usize },
// }

// impl List {
//     pub fn insert(content: char, pos: usize) -> Self {
//         Self::Insert { content, pos }
//     }

//     pub fn delete(pos: usize) -> Self {
//         Self::Delete { pos }
//     }
// }

// #[derive(Clone, Debug, Default)]
// pub struct EgWalkerStableV4;

// // ---------------- Internal state used only during eval ----------------

// const NOT_YET_INSERTED: i32 = -1;
// const INSERTED: i32 = 0;

// enum State {
//     Deleted(i32),
//     Inserted,
//     NotYetInserted,
// }

// #[derive(Clone, Debug)]
// struct CrdtItem {
//     node: NodeIndex,
//     origin_left: Option<NodeIndex>,
//     origin_right: Option<NodeIndex>,
//     deleted: bool,
//     cur_state: i32, // NOT_YET_INSERTED, INSERTED, >0 for number of deletes applied
// }

// #[derive(Default)]
// struct CrdtDoc {
//     items: Vec<CrdtItem>,
//     current_version: Vec<NodeIndex>,
//     del_targets: HashMap<NodeIndex, NodeIndex>,
//     items_by_nx: HashMap<NodeIndex, usize>,
// }

// fn find_by_current_pos(items: &[CrdtItem], target_pos: usize) -> (usize, usize) {
//     // println!(
//     //     "DEBUG: find_by_current_pos called with target_pos={}, items.len()={}",
//     //     target_pos,
//     //     items.len()
//     // );

//     // Debug: Print all items and their states
//     // for (i, item) in items.iter().enumerate() {
//     //     println!(
//     //         "  Item {}: node={:?}, deleted={}, cur_state={}",
//     //         i, item.node, item.deleted, item.cur_state
//     //     );
//     // }

//     let mut cur_pos = 0usize;
//     let mut end_pos = 0usize;
//     let mut idx = 0usize;

//     while cur_pos < target_pos {
//         // println!(
//         //     "  Loop: cur_pos={}, target_pos={}, idx={}, items.len()={}",
//         //     cur_pos,
//         //     target_pos,
//         //     idx,
//         //     items.len()
//         // );

//         if idx >= items.len() {
//             // println!(
//             //     "ERROR: Past end of items list - idx={}, items.len()={}",
//             //     idx,
//             //     items.len()
//             // );
//             panic!("Past end of items list");
//         }

//         let item = &items[idx];
//         // println!(
//         //     "    Checking item {}: cur_state={}, deleted={}",
//         //     idx, item.cur_state, item.deleted
//         // );

//         if item.cur_state == INSERTED {
//             cur_pos += 1;
//             // println!("    Item is INSERTED, cur_pos now {}", cur_pos);
//         }
//         if !item.deleted {
//             end_pos += 1;
//             // println!("    Item not deleted, end_pos now {}", end_pos);
//         }
//         idx += 1;
//     }

//     // println!(
//     //     "DEBUG: find_by_current_pos returning ({}, {})",
//     //     idx, end_pos
//     // );
//     (idx, end_pos)
// }

// fn agent_ordering_less(graph: &EventGraph<EgWalkerOp>, a: NodeIndex, b: NodeIndex) -> bool {
//     // Tiebreaker: compare Dot.origin() lexicographically ascending; if equal, compare Dot.val() ascending
//     // This ensures deterministic ordering between concurrent inserts targeting the same left/right.
//     let dota = graph.dot_index_map.nx_to_dot(&a).expect("dot for a");
//     let dotb = graph.dot_index_map.nx_to_dot(&b).expect("dot for b");
//     if dota.origin() == dotb.origin() {
//         dota.val() < dotb.val()
//     } else {
//         dota.origin() < dotb.origin()
//     }
// }

// fn index_of_node(items: &[CrdtItem], node: NodeIndex) -> usize {
//     items
//         .iter()
//         .position(|it| it.node == node)
//         .expect("Could not find item by NodeIndex")
// }

// fn integrate(
//     doc: &mut CrdtDoc,
//     event_graph: &EventGraph<EgWalkerOp>,
//     new_item: CrdtItem,
//     mut idx: usize,
//     mut end_pos: usize,
//     snapshot: &mut Vec<char>,
//     content: char,
// ) {
//     // println!(
//     //     "DEBUG: integrate called with idx={}, end_pos={}, content='{}', items.len()={}",
//     //     idx,
//     //     end_pos,
//     //     content,
//     //     doc.items.len()
//     // );

//     let mut scan_idx = idx;
//     let mut scan_end_pos = end_pos;

//     // If origin_left is None, we'll pretend there's an item at position -1 which we were inserted to the right of.
//     let left = scan_idx as isize - 1;
//     let right = if let Some(r) = new_item.origin_right {
//         index_of_node(&doc.items, r)
//     } else {
//         doc.items.len()
//     };

//     //println!("  Integration bounds: left={}, right={}", left, right);

//     let mut scanning = false;

//     while scan_idx < right {
//         let other = &doc.items[scan_idx];

//         if other.cur_state != NOT_YET_INSERTED {
//             break;
//         }

//         let oleft = if let Some(ol) = other.origin_left {
//             index_of_node(&doc.items, ol) as isize
//         } else {
//             -1
//         };

//         let oright = if let Some(or) = other.origin_right {
//             index_of_node(&doc.items, or)
//         } else {
//             doc.items.len()
//         };

//         if oleft < left
//             || (oleft == left
//                 && oright == right
//                 && agent_ordering_less(event_graph, new_item.node, other.node))
//         {
//             break;
//         }
//         if oleft == left {
//             scanning = (oright as usize) < right;
//         }

//         if !other.deleted {
//             scan_end_pos += 1;
//         }
//         scan_idx += 1;

//         if !scanning {
//             idx = scan_idx;
//             end_pos = scan_end_pos;
//         }
//     }

//     // Insert item
//     //println!("  Inserting item at idx={}", idx);
//     doc.items.insert(idx, new_item);

//     // Update items_by_nx mapping for all shifted items at and after idx
//     for i in idx..doc.items.len() {
//         let n = doc.items[i].node;
//         doc.items_by_nx.insert(n, i);
//     }

//     // println!(
//     //     "  Inserting '{}' into snapshot at end_pos={}",
//     //     content, end_pos
//     // );
//     // Update snapshot with content
//     snapshot.insert(end_pos, content);

//     // println!(
//     //     "  After integration: items.len()={}, snapshot={:?}",
//     //     doc.items.len(),
//     //     snapshot
//     // );
// }

// fn retreat(doc: &mut CrdtDoc, graph: &EventGraph<EgWalkerOp>, op_nx: NodeIndex) {
//     // println!("DEBUG: retreat called for node {:?}", op_nx);

//     // For inserts, target is the item itself; for deletes, target is the item which was deleted
//     let node_weight = graph.unstable.node_weight(op_nx).expect("node must exist");
//     let target = match &node_weight.0 {
//         EgWalkerOp::Insert { .. } => op_nx,
//         EgWalkerOp::Delete { .. } => doc.del_targets[&op_nx],
//     };

//     if let Some(&item_idx) = doc.items_by_nx.get(&target) {
//         let item = &mut doc.items[item_idx];
//         // println!(
//         //     "  Retreating item {}: cur_state {} -> {}",
//         //     item_idx,
//         //     item.cur_state,
//         //     item.cur_state - 1
//         // );
//         item.cur_state -= 1;
//     } else {
//         //println!("  No item found for target {:?}", target);
//     }
// }

// fn advance(doc: &mut CrdtDoc, graph: &EventGraph<EgWalkerOp>, op_nx: NodeIndex) {
//     //println!("DEBUG: advance called for node {:?}", op_nx);

//     let node_weight = graph.unstable.node_weight(op_nx).expect("node must exist");
//     let target = match &node_weight.0 {
//         EgWalkerOp::Insert { .. } => op_nx,
//         EgWalkerOp::Delete { .. } => doc.del_targets[&op_nx],
//     };

//     if let Some(&item_idx) = doc.items_by_nx.get(&target) {
//         let item = &mut doc.items[item_idx];
//         // println!(
//         //     "  Advancing item {}: cur_state {} -> {}",
//         //     item_idx,
//         //     item.cur_state,
//         //     item.cur_state + 1
//         // );
//         item.cur_state += 1;
//     } else {
//         //println!("  No item found for target {:?}", target);
//     }
// }
// fn apply(
//     doc: &mut CrdtDoc,
//     graph: &EventGraph<EgWalkerOp>,
//     op_nx: NodeIndex,
//     op: EgWalkerOp,
//     snapshot: &mut Vec<char>,
// ) {
//     // println!("DEBUG: apply called for node {:?} with op {:?}", op_nx, op);

//     match op {
//         EgWalkerOp::Delete { pos } => {
//             //println!("DELETE operation at pos {}", pos);
//             let (mut idx, mut end_pos) = find_by_current_pos(&doc.items, pos);

//             // println!(
//             //     "After find_by_current_pos: idx={}, end_pos={}",
//             //     idx, end_pos
//             // );

//             while idx < doc.items.len() && doc.items[idx].cur_state != INSERTED {
//                 // println!(
//                 //     "  Skipping item {} (cur_state={})",
//                 //     idx, doc.items[idx].cur_state
//                 // );
//                 if !doc.items[idx].deleted {
//                     end_pos += 1;
//                 }
//                 idx += 1;
//             }

//             if idx >= doc.items.len() {
//                 println!("ERROR: No INSERTED item found at position {}", pos);
//                 panic!("No INSERTED item found at position {}", pos);
//             }

//             {
//                 let item = &mut doc.items[idx];
//                 //println!("  Deleting item {} (was deleted={})", idx, item.deleted);
//                 if !item.deleted {
//                     item.deleted = true;
//                     //println!("  Removing from snapshot at end_pos={}", end_pos);
//                     snapshot.remove(end_pos);
//                 }
//                 item.cur_state = 1;
//                 doc.del_targets.insert(op_nx, item.node);
//             }
//         }
//         EgWalkerOp::Insert { content, pos } => {
//             //println!("INSERT operation: '{}' at pos {}", content, pos);
//             let (idx, end_pos) = find_by_current_pos(&doc.items, pos);

//             if idx >= 1 && doc.items[idx - 1].cur_state != INSERTED {
//                 println!(
//                     "ERROR: Item to the left is not inserted! idx={}, left_item.cur_state={}",
//                     idx,
//                     doc.items[idx - 1].cur_state
//                 );
//                 panic!("Item to the left is not inserted! What!"); // OLDCODE behavior retained
//             }

//             let origin_left = if idx == 0 {
//                 None
//             } else {
//                 Some(doc.items[idx - 1].node)
//             };

//             let mut origin_right: Option<NodeIndex> = None;
//             for i in idx..doc.items.len() {
//                 if doc.items[i].cur_state != NOT_YET_INSERTED {
//                     origin_right = Some(doc.items[i].node);
//                     break;
//                 }
//             }

//             // println!(
//             //     "  Creating item with origin_left={:?}, origin_right={:?}",
//             //     origin_left, origin_right
//             // );

//             let item = CrdtItem {
//                 node: op_nx,
//                 origin_left,
//                 origin_right,
//                 deleted: false,
//                 cur_state: INSERTED,
//             };

//             integrate(doc, graph, item, idx, end_pos, snapshot, content);
//         }
//     }
// }

// fn parents_of_node(graph: &EventGraph<EgWalkerOp>, nx: NodeIndex) -> Vec<NodeIndex> {
//     let mut parents: Vec<NodeIndex> = graph
//         .unstable
//         .neighbors_directed(nx, Direction::Outgoing)
//         .collect();
//     parents.sort_unstable();
//     parents
// }

// fn diff(
//     graph: &EventGraph<EgWalkerOp>,
//     current_version: &[NodeIndex],
//     parents: &[NodeIndex],
// ) -> (Vec<NodeIndex>, Vec<NodeIndex>) {
//     #[derive(Clone, Copy, PartialEq, Eq)]
//     enum DiffFlag {
//         A,
//         B,
//         Shared,
//     }

//     let mut flags: HashMap<NodeIndex, DiffFlag> = HashMap::new();
//     // Match TS PriorityQueue<Lv> by prioritizing higher NodeIndex first
//     let mut queue: BinaryHeap<(usize, NodeIndex)> = BinaryHeap::new();
//     let mut num_shared = 0usize;

//     fn enq(
//         flags: &mut HashMap<NodeIndex, DiffFlag>,
//         queue: &mut BinaryHeap<(usize, NodeIndex)>,
//         num_shared: &mut usize,
//         v: NodeIndex,
//         flag: DiffFlag,
//     ) {
//         let prev = flags.get(&v).copied();
//         match prev {
//             None => {
//                 queue.push((v.index(), v));
//                 if flag == DiffFlag::Shared {
//                     *num_shared += 1;
//                 }
//                 flags.insert(v, flag);
//             }
//             Some(old_flag) => {
//                 if flag != old_flag && old_flag != DiffFlag::Shared {
//                     flags.insert(v, DiffFlag::Shared);
//                     *num_shared += 1;
//                 }
//             }
//         }
//     }

//     for &cv in current_version {
//         enq(&mut flags, &mut queue, &mut num_shared, cv, DiffFlag::A);
//     }
//     for &p in parents {
//         enq(&mut flags, &mut queue, &mut num_shared, p, DiffFlag::B);
//     }

//     let mut a_only = Vec::new();
//     let mut b_only = Vec::new();

//     while queue.len() > num_shared {
//         let (_, nx) = queue.pop().unwrap();
//         let flag = flags.get(&nx).copied().unwrap();
//         match flag {
//             DiffFlag::Shared => {
//                 num_shared -= 1;
//             }
//             DiffFlag::A => a_only.push(nx),
//             DiffFlag::B => b_only.push(nx),
//         }

//         for p in graph.unstable.neighbors_directed(nx, Direction::Outgoing) {
//             enq(&mut flags, &mut queue, &mut num_shared, p, flag);
//         }
//     }

//     (a_only, b_only)
// }

// fn do1_operation(
//     doc: &mut CrdtDoc,
//     graph: &EventGraph<EgWalkerOp>,
//     nx: NodeIndex,
//     snapshot: &mut Vec<char>,
// ) {
//     let (op, _clock) = graph
//         .unstable
//         .node_weight(nx)
//         .cloned()
//         .expect("node weight must exist");

//     //println!("\n=== Processing operation {:?}: {:?} ===", nx, op);
//     //println!("Current snapshot: {:?}", snapshot);
//     //println!("Current items.len(): {}", doc.items.len());

//     let (a_only, b_only) = diff(graph, &doc.current_version, &parents_of_node(graph, nx));

//     //println!("Diff results: a_only={:?}, b_only={:?}", a_only, b_only);

//     // retreat then advance to move to parents frontier
//     // imagine we are in starting the right branch after we have already applied all ops in left branch
//     //so we need to retreat those already applied in left branch (AOnly)
//     // we can say that this is done in the fork point
//     // aOnly are those that are only in the current version(the version before processing the new current one )
//     for i in a_only {
//         retreat(doc, graph, i);
//     }
//     // we can say this is done in the join point
//     // bOnly are those that are only in the parents of the new to be current version
//     for i in b_only {
//         advance(doc, graph, i);
//     }
//     //apply
//     apply(doc, graph, nx, op, snapshot);
//     doc.current_version = vec![nx];
//     // println!("Updated current_version to [{:?}]", nx);
//     // println!("Final snapshot after operation: {:?}", snapshot);
// }

// impl PureCRDT for List {
//     // Inspired by Joseph Gentle's implementations:
//     // - Egwalker:  https://github.com/josephg/egwalker-from-scratch
//     type Value = String;
//     type Stable = EgWalkerStableV4;

//     fn redundant_itself(_new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
//         false
//     }

//     fn redundant_by_when_redundant(
//         _old_op: &Self,
//         _old_dot: Option<&Dot>,
//         _is_conc: bool,
//         _new_op: &Self,
//         _new_dot: &Dot,
//     ) -> bool {
//         false
//     }

//     fn redundant_by_when_not_redundant(
//         _old_op: &Self,
//         _old_dot: Option<&Dot>,
//         _is_conc: bool,
//         _new_op: &Self,
//         _new_dot: &Dot,
//     ) -> bool {
//         false
//     }

//     fn eval(
//         _stable: &Self::Stable,
//         _unstable: &[Self],
//         event_graph: &EventGraph<Self>,
//     ) -> Self::Value {
//         //println!("\n===== STARTING EVAL =====");

//         let mut doc = CrdtDoc::default();
//         let mut snapshot: Vec<char> = Vec::new();
//         // I was using this first to force sorting
//         /*
//          let mut node_indices =
//              toposort(&event_graph.unstable, None).expect("Graph should be a DAG");
//          node_indices.reverse();

//          // println!("Processing nodes in order: {:?}", node_indices);

//         for nx in node_indices {
//         */
//         for nx in event_graph.unstable.node_indices() {
//             do1_operation(&mut doc, event_graph, nx, &mut snapshot);
//         }

//         let result: String = snapshot.into_iter().collect();
//         //println!("\n===== EVAL COMPLETE: '{}' =====\n", result);
//         result
//     }
// }

// // #[cfg(test)]
// // mod tests {
// //     use super::*;
// //     use crate::crdt::test_util::twins;
// //     // OLDCODE use crate::protocol::event::Event;
// //     use crate::protocol::event_graph::EventGraph;
// //     // OLDCODE use crate::protocol::tcsb;

// //     #[test]
// //     fn test_simple_insertion_egwalker() {
// //         let (mut a, mut b) = twins::<EventGraph<EgWalkerOp>>();

// //         let e1 = a.tc_bcast(EgWalkerOp::insert('A', 0));
// //         b.try_deliver(e1);

// //         assert_eq!(a.eval(), "A");
// //         assert_eq!(a.eval(), b.eval());
// //     }

// //     #[test]
// //     fn test_concurrent_insertions_egwalker() {
// //         let (mut a, mut b) = twins::<EventGraph<EgWalkerOp>>();

// //         let e1 = a.tc_bcast(EgWalkerOp::insert('H', 0));
// //         b.try_deliver(e1);
// //         assert_eq!(a.eval(), "H");

// //         let e2a = a.tc_bcast(EgWalkerOp::insert('e', 1));
// //         let e2b = b.tc_bcast(EgWalkerOp::insert('i', 1));
// //         b.try_deliver(e2a);
// //         a.try_deliver(e2b);

// //         let res_b = b.eval();
// //         assert!(
// //             res_b == "Hei" || res_b == "Hie",
// //             "Unexpected order: {}",
// //             res_b
// //         );
// //         assert_eq!(a.eval(), b.eval());
// //     }

// //     #[test]
// //     fn test_delete_operation_egwalker() {
// //         let (mut a, mut b) = twins::<EventGraph<EgWalkerOp>>();

// //         let e1 = a.tc_bcast(EgWalkerOp::insert('A', 0));
// //         b.try_deliver(e1);

// //         let e2 = a.tc_bcast(EgWalkerOp::delete(0));
// //         b.try_deliver(e2);

// //         assert_eq!(a.eval(), "");
// //         assert_eq!(a.eval(), b.eval());
// //     }

// //     #[test]
// //     fn test_conc_delete_ins_egwalker() {
// //         let (mut a, mut b) = twins::<EventGraph<EgWalkerOp>>();

// //         let e1 = a.tc_bcast(EgWalkerOp::insert('A', 0));
// //         b.try_deliver(e1);

// //         let edel = a.tc_bcast(EgWalkerOp::delete(0));
// //         let eins = b.tc_bcast(EgWalkerOp::insert('B', 1)); // Insert to the right of 'A' in B's view
// //         a.try_deliver(eins);
// //         b.try_deliver(edel);

// //         assert_eq!(a.eval(), "B");
// //         assert_eq!(a.eval(), b.eval());
// //     }

// //     #[test]
// //     fn test_sequential_conc_operations_egwalker() {
// //         let (mut a, mut b) = twins::<EventGraph<EgWalkerOp>>();

// //         let e1 = a.tc_bcast(EgWalkerOp::insert('H', 0));
// //         b.try_deliver(e1);
// //         assert_eq!(a.eval(), "H");

// //         let e2a = a.tc_bcast(EgWalkerOp::insert('e', 1));
// //         let e2b = b.tc_bcast(EgWalkerOp::insert('i', 1));
// //         b.try_deliver(e2a);
// //         a.try_deliver(e2b);
// //         assert!(b.eval() == "Hei" || b.eval() == "Hie");

// //         // Insert a space between e and i from A's perspective (which will be position 2 if e<i)
// //         let e3 = a.tc_bcast(EgWalkerOp::insert(' ', 2));
// //         b.try_deliver(e3);
// //         let res = a.eval();
// //         // Depending on tie-breaker, expected is either "He i" or "Hi e". We accept either space between letters.
// //         assert!(res == "He i" || res == "Hi e", "Unexpected result: {}", res);
// //     }

// //     #[test]
// //     fn test_in_paper() {
// //         let (mut tcsb_a, mut tcsb_b) = twins::<EventGraph<EgWalkerOp>>();

// //         // e1: Insert(0, 'h')
// //         let e1 = tcsb_a.tc_bcast(EgWalkerOp::insert('h', 0));
// //         tcsb_b.try_deliver(e1.clone());

// //         // e2: Insert(1, 'i')
// //         let e2 = tcsb_a.tc_bcast(EgWalkerOp::insert('i', 1));
// //         tcsb_b.try_deliver(e2.clone());

// //         // Branch: Replica A will capitalize 'H', Replica B will change to 'hey'
// //         // e3: Insert(0, 'H') depends on e1,e2
// //         let e3 = tcsb_a.tc_bcast(EgWalkerOp::insert('H', 0));
// //         // tcsb_b.try_deliver(e3.clone());

// //         // e4: Delete(1) (remove lowercase 'h') depends on e3
// //         let e4 = tcsb_a.tc_bcast(EgWalkerOp::delete(1));
// //         // tcsb_b.try_deliver(e4.clone());

// //         // e5: Delete(1) (remove 'i') on other branch
// //         let e5 = tcsb_b.tc_bcast(EgWalkerOp::delete(1));
// //         // tcsb_a.try_deliver(e5.clone());

// //         // e6: Insert(1, 'e')
// //         let e6 = tcsb_b.tc_bcast(EgWalkerOp::insert('e', 1));
// //         // tcsb_a.try_deliver(e6.clone());

// //         // e7: Insert(2, 'y')
// //         let e7 = tcsb_b.tc_bcast(EgWalkerOp::insert('y', 2));

// //         tcsb_b.try_deliver(e3.clone());
// //         tcsb_b.try_deliver(e4.clone());
// //         tcsb_a.try_deliver(e5.clone());
// //         tcsb_a.try_deliver(e6.clone());
// //         tcsb_a.try_deliver(e7.clone());
// //         // Merge both replicas so they see all events before e8
// //         // At this point both should be "Hey"
// //         // assert_eq!(tcsb_a.eval(), "Hey");
// //         // assert_eq!(tcsb_a.eval(), tcsb_b.eval());

// //         // e8: Insert(3, '!')
// //         let e8 = tcsb_b.tc_bcast(EgWalkerOp::insert('!', 3));
// //         tcsb_a.try_deliver(e8.clone());

// //         // Final result should be "Hey!"
// //         assert_eq!(tcsb_a.eval(), "Hey!");
// //         // assert_eq!(tcsb_a.eval(), tcsb_b.eval());
// //     }
// // }
