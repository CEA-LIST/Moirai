// use std::{
//     collections::{HashSet},
//     fmt::Debug,
//     hash::Hash,
// };

// use petgraph::graph::DiGraph;

// use crate::{
//     clocks::dot::Dot,
//     protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
// };

// #[derive(Clone, Debug)]
// pub enum AWGraph<V, E> {
//     AddVertex(V),
//     RemoveVertex(V),
//     AddArc(V, V, E),
//     RemoveArc(V, V, E),
// }

// impl<V, E> PureCRDT for AWGraph<V, E>
// where
//     V: Debug + Clone + PartialEq + Eq + Hash,
//     E: Debug + Clone + PartialEq + Eq + Hash,
// {
//     type Value = DiGraph<V, E>;
//     type Stable = Vec<Self>;

//     fn redundant_itself(new_op: &Self, new_dot: &Dot, state: &EventGraph<Self>) -> bool {
//         match new_op {
//             AWGraph::RemoveVertex(v1) => {
//                 let predecessors = state.causal_predecessors(new_dot);
//                 state.non_tombstones.iter().any(|nx| {
//                     if !predecessors.contains(nx) {
//                         let old_op = &state.unstable.node_weight(*nx).unwrap().0;
//                         match old_op {
//                             AWGraph::AddVertex(v2) => v1 == v2,
//                             AWGraph::AddArc(v2, v3, _) => v1 == v2 || v1 == v3,
//                             _ => false,
//                         }
//                     } else {
//                         false
//                     }
//                 })
//             }
//             AWGraph::RemoveArc(_, _, _) => true,
//             _ => false,
//         }
//     }

//     fn redundant_by_when_redundant(
//         old_op: &Self,
//         _old_dot: Option<&Dot>,
//         is_conc: bool,
//         new_op: &Self,
//         _new_dot: &Dot,
//     ) -> bool {
//         // old_op = addVertex, addArc only
//         // This respect user intent in the sense that removing a vertex has no effect
//         // on the concurrent addArc or addVertex operations.
//         // However, removing a vertex does remove the arcs that are incident to it.
//         //! Concurrent addArc/RemovVertex -> restore the vertex
//         !is_conc
//             && match (old_op, new_op) {
//                 // (AWGraph::AddArc(v1, v2, _), AWGraph::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
//                 (AWGraph::RemoveVertex(_), AWGraph::AddArc(_, _, _)) => false,
//                 (AWGraph::RemoveVertex(v1), AWGraph::RemoveVertex(v2)) => v1 == v2,
//                 (AWGraph::RemoveVertex(v1), AWGraph::AddVertex(v2)) => v1 == v2,
//                 (AWGraph::RemoveVertex(_), AWGraph::RemoveArc(_, _, _)) => false,
//                 (AWGraph::AddVertex(_), AWGraph::RemoveVertex(_)) => false,
//                 (AWGraph::AddArc(_, _, _), AWGraph::RemoveVertex(_)) => false,
//                 (AWGraph::AddArc(v1, v2, e1), AWGraph::AddArc(v3, v4, e2))
//                 | (AWGraph::AddArc(v1, v2, e1), AWGraph::RemoveArc(v3, v4, e2)) => {
//                     v1 == v3 && v2 == v4 && e1 == e2
//                 }
//                 (AWGraph::AddVertex(v1), AWGraph::AddVertex(v2)) => v1 == v2,
//                 _ => false,
//             }
//             || is_conc
//                 && match (old_op, new_op) {
//                     (AWGraph::RemoveVertex(v1), AWGraph::AddArc(v2, v3, _)) => v1 == v2 || v1 == v3,
//                     (AWGraph::RemoveVertex(v1), AWGraph::RemoveVertex(v2)) => v1 == v2,
//                     (AWGraph::RemoveVertex(_), AWGraph::RemoveArc(_, _, _)) => false,
//                     (AWGraph::RemoveVertex(v1), AWGraph::AddVertex(v2)) => v1 == v2,
//                     _ => false,
//                 }
//     }

//     fn redundant_by_when_not_redundant(
//         old_op: &Self,
//         old_dot: Option<&Dot>,
//         is_conc: bool,
//         new_op: &Self,
//         new_dot: &Dot,
//     ) -> bool {
//         Self::redundant_by_when_redundant(old_op, old_dot, is_conc, new_op, new_dot)
//     }

//     fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
//         let mut ops: Vec<&Self> = stable.iter().chain(unstable.iter()).collect();
//         ops.sort_by(|a, b| match (a, b) {
//             (AWGraph::RemoveVertex(_), AWGraph::AddArc(_, _, _)) => std::cmp::Ordering::Less,
//             (AWGraph::RemoveVertex(_), AWGraph::AddVertex(_)) => std::cmp::Ordering::Less,
//             (AWGraph::AddVertex(_), AWGraph::RemoveVertex(_)) => std::cmp::Ordering::Greater,
//             (AWGraph::AddArc(_, _, _), AWGraph::RemoveVertex(_)) => std::cmp::Ordering::Greater,
//             (AWGraph::AddVertex(_), AWGraph::AddArc(_, _, _)) => std::cmp::Ordering::Less,
//             (AWGraph::AddArc(_, _, _), AWGraph::AddVertex(_)) => std::cmp::Ordering::Greater,
//             _ => std::cmp::Ordering::Equal,
//         });
//         let mut graph = DiGraph::new();
//         let mut node_index = HashMap::new();
//         let mut edge_index: HashSet<(&V, &V, &E)> = HashSet::default();
//         let mut rmv_set = HashSet::default();
//         for o in ops {
//             match o {
//                 AWGraph::RemoveVertex(v) => {
//                     rmv_set.insert(v);
//                 }
//                 AWGraph::AddVertex(v) => {
//                     if node_index.contains_key(v) || rmv_set.contains(v) {
//                         continue; // Skip if the vertex already exists
//                     }
//                     let idx = graph.add_node(v.clone());
//                     node_index.insert(v, idx);
//                 }
//                 AWGraph::AddArc(v1, v2, e) => {
//                     if edge_index.contains(&(v1, v2, e)) {
//                         continue; // Skip if the edge already exists
//                     }
//                     if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
//                         graph.add_edge(*a, *b, e.clone());
//                         edge_index.insert((v1, v2, e));
//                     }
//                 }
//                 _ => {}
//             }
//         }
//         graph
//     }
// }

// #[cfg(test)]
// mod tests {
//     use petgraph::{algo::is_isomorphic, graph::DiGraph};

//     use crate::crdt::{aw_multidigraph::AWGraph, test_util::twins_graph};

//     #[test]
//     fn simple_graph() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, &str>>();

//         let event = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         replica_b.receive(event);

//         let event = replica_b.send(AWGraph::AddVertex("B")).unwrap();
//         replica_a.receive(event);

//         let event = replica_a.send(AWGraph::AddArc("B", "A", "arc1")).unwrap();
//         replica_b.receive(event);

//         let event = replica_b.send(AWGraph::RemoveVertex("B")).unwrap();
//         replica_a.receive(event);

//         assert!(is_isomorphic(&replica_a.query(Read::new()), &replica_b.query(Read::new())));
//     }

//     #[test]
//     fn concurrent_graph_arc() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, &str>>();

//         let event = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         replica_b.receive(event);

//         let event = replica_b.send(AWGraph::AddVertex("B")).unwrap();
//         replica_a.receive(event);

//         let event_b = replica_b.send(AWGraph::RemoveVertex("B")).unwrap();
//         let event_a = replica_a.send(AWGraph::AddArc("B", "A", "arc1")).unwrap();
//         replica_b.receive(event_a);
//         replica_a.receive(event_b);

//         println!("{:?}", petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[]));
//         println!("{:?}", petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[]));
//         assert_eq!(replica_a.query(Read::new()).node_count(), 2);
//         assert!(is_isomorphic(&replica_a.query(Read::new()), &replica_b.query(Read::new())));
//     }

//     #[test]
//     fn graph_remove_vertex() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, &str>>();

//         let event = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         replica_b.receive(event);

//         let event = replica_b.send(AWGraph::RemoveVertex("A")).unwrap();
//         replica_a.receive(event);

//         // let event_b = replica_b.send(AWGraph::RemoveVertex("B")).unwrap();
//         let event_a = replica_a.send(AWGraph::AddArc("B", "A", "arc1")).unwrap();
//         replica_b.receive(event_a);
//         // replica_a.receive(event_b);

//         println!("{:?}", petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[]));
//         println!("{:?}", petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[]));
//         assert_eq!(replica_a.query(Read::new()).node_count(), 0);
//         assert!(is_isomorphic(&replica_a.query(Read::new()), &replica_b.query(Read::new())));
//     }

//     #[test]
//     fn concurrent_graph_vertex() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, &str>>();

//         let event_a = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         let event_b = replica_b.send(AWGraph::AddVertex("A")).unwrap();
//         replica_a.receive(event_b);
//         replica_b.receive(event_a);

//         assert_eq!(replica_a.query(Read::new()).node_count(), 1);
//         assert!(is_isomorphic(&replica_a.query(Read::new()), &replica_b.query(Read::new())));
//     }

//     #[test]
//     fn graph_arc_no_vertex() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, u8>>();

//         let event = replica_a.send(AWGraph::AddArc("A", "B", 1)).unwrap();
//         replica_b.receive(event);

//         assert!(is_isomorphic(&replica_a.query(Read::new()), &DiGraph::<&str, ()>::new()));
//     }

//     #[test]
//     fn graph_multiple_vertex_same_id() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, u8>>();

//         let event_a = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         replica_b.receive(event_a);
//         let event_b = replica_b.send(AWGraph::AddVertex("A")).unwrap();
//         replica_a.receive(event_b);
//         let event_a = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         replica_b.receive(event_a);

//         assert_eq!(replica_a.query(Read::new()).node_count(), 1);
//     }

//     #[test]
//     fn multigraph() {
//         let (mut replica_a, mut replica_b) = twins::<AWGraph<&str, u8>>();

//         let event_a = replica_a.send(AWGraph::AddVertex("A")).unwrap();
//         replica_b.receive(event_a);
//         let event_b = replica_b.send(AWGraph::AddVertex("B")).unwrap();
//         replica_a.receive(event_b);

//         let event_a = replica_a.send(AWGraph::AddArc("A", "B", 1)).unwrap();
//         let event_b = replica_b.send(AWGraph::AddArc("A", "B", 2)).unwrap();

//         replica_a.receive(event_b);
//         replica_b.receive(event_a);

//         println!("{:?}", petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[]));
//         println!("{:?}", petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[]));

//         assert_eq!(replica_a.query(Read::new()).edge_count(), 2);
//         assert_eq!(replica_a.query(Read::new()).node_count(), 2);
//         assert!(petgraph::algo::is_isomorphic(
//             &replica_a.query(Read::new()),
//             &replica_b.query(Read::new())
//         ));
//     }
// }
