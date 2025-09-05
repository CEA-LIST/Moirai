use petgraph::graph::DiGraph;

use crate::protocol::crdt::pure_crdt::PureCRDT;
use crate::protocol::event::tag::Tag;
use crate::protocol::event::tagged_op::TaggedOp;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum Graph<V, E> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V, E),
    RemoveArc(V, V, E),
}

impl<V, E> PureCRDT for Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    type Value = DiGraph<V, E>;
    type StableState = Vec<Self>;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(
            new_tagged_op.op(),
            Graph::RemoveVertex(_) | Graph::RemoveArc(_, _, _)
        )
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        // old_op = addVertex, addArc only
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (Graph::AddArc(v1, v2, _), Graph::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
                (Graph::AddArc(v1, v2, e1), Graph::AddArc(v3, v4, e2))
                | (Graph::AddArc(v1, v2, e1), Graph::RemoveArc(v3, v4, e2)) => {
                    v1 == v3 && v2 == v4 && e1 == e2
                }
                (Graph::AddVertex(v1), Graph::AddVertex(v2))
                | (Graph::AddVertex(v1), Graph::RemoveVertex(v2)) => v1 == v2,
                _ => false,
            }
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_tag, is_conc, new_tagged_op)
    }

    fn eval<'a>(
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> Self::Value
    where
        Self: 'a,
    {
        let mut ops: Vec<&Self> = stable.iter().chain(unstable.map(|to| to.op())).collect();
        ops.sort_by(|a, b| match (a, b) {
            (Graph::AddVertex(_), Graph::AddArc(_, _, _)) => Ordering::Less,
            (Graph::AddArc(_, _, _), Graph::AddVertex(_)) => Ordering::Greater,
            _ => Ordering::Equal,
        });
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        let mut edge_index: HashSet<(&V, &V, &E)> = HashSet::new();
        for o in ops {
            match o {
                Graph::AddVertex(v) => {
                    if node_index.contains_key(v) {
                        continue; // Skip if the vertex already exists
                    }
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2, e) => {
                    // if edge_index.contains(&(v1, v2, e)) {
                    //     continue; // Skip if the edge already exists
                    // }
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        graph.add_edge(*a, *b, e.clone());
                        edge_index.insert((v1, v2, e));
                    }
                }
                _ => {}
            }
        }
        graph
    }
}

#[cfg(test)]
mod tests {
    use petgraph::graph::DiGraph;

    use crate::{
        crdt::{
            graph::aw_multidigraph::Graph,
            test_util::{triplet, twins},
        },
        protocol::replica::IsReplica,
    };

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event = replica_a.send(Graph::AddVertex("A"));
        replica_b.receive(event);

        let event = replica_b.send(Graph::AddVertex("B"));
        replica_a.receive(event);

        let event = replica_a.send(Graph::AddArc("B", "A", "arc1"));
        replica_b.receive(event);

        let event = replica_b.send(Graph::RemoveVertex("B"));
        replica_a.receive(event);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query(),)
            .first()
            .is_some());
    }

    #[test]
    fn concurrent_graph_arc() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event = replica_a.send(Graph::AddVertex("A"));
        replica_b.receive(event);

        let event = replica_b.send(Graph::AddVertex("B"));
        replica_a.receive(event);

        let event_b = replica_b.send(Graph::RemoveVertex("B"));
        let event_a = replica_a.send(Graph::AddArc("B", "A", "arc1"));
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query(),)
            .first()
            .is_some());
    }

    #[test]
    fn concurrent_graph_vertex() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event_a = replica_a.send(Graph::AddVertex("A"));
        let event_b = replica_b.send(Graph::AddVertex("A"));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query().node_count(), 1);
        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query(),)
            .first()
            .is_some());
    }

    #[test]
    fn graph_arc_no_vertex() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event = replica_a.send(Graph::AddArc("A", "B", 1));
        replica_b.receive(event);

        assert!(
            vf2::isomorphisms(&replica_a.query(), &DiGraph::<&str, u8>::new(),)
                .first()
                .is_some()
        );
    }

    #[test]
    fn graph_multiple_vertex_same_id() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event_a = replica_a.send(Graph::AddVertex("A"));
        replica_b.receive(event_a);
        let event_b = replica_b.send(Graph::AddVertex("A"));
        replica_a.receive(event_b);
        let event_a = replica_a.send(Graph::AddVertex("A"));
        replica_b.receive(event_a);

        assert_eq!(replica_a.query().node_count(), 1);
    }

    #[test]
    fn revive_arc() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event_a = replica_a.send(Graph::AddVertex("A"));
        replica_b.receive(event_a);
        let event_b = replica_b.send(Graph::AddVertex("B"));
        replica_a.receive(event_b);

        let event_a = replica_a.send(Graph::AddArc("A", "B", 1));
        let event_b = replica_b.send(Graph::RemoveVertex("B"));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query(),)
            .first()
            .is_some());

        assert_eq!(replica_a.query().node_count(), 1);
        assert_eq!(replica_a.query().edge_count(), 0);

        let event_a = replica_a.send(Graph::AddVertex("B"));
        replica_b.receive(event_a);

        println!(
            "{:?}",
            petgraph::dot::Dot::with_config(&replica_a.query(), &[])
        );
        println!(
            "{:?}",
            petgraph::dot::Dot::with_config(&replica_b.query(), &[])
        );

        assert_eq!(replica_a.query().node_count(), 2);
        assert_eq!(replica_a.query().edge_count(), 1);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query(),)
            .first()
            .is_some());
    }

    #[test]
    fn revive_arc_2() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<Graph<&str, u8>>();

        let event_b_1 = replica_b.send(Graph::AddVertex("A"));
        replica_c.receive(event_b_1.clone());
        let event_c_1 = replica_c.send(Graph::AddVertex("B"));
        let event_c_2 = replica_c.send(Graph::AddArc("A", "B", 1));
        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_c_2.clone());

        assert!(vf2::isomorphisms(&replica_b.query(), &replica_c.query(),)
            .first()
            .is_some());

        let event_a_1 = replica_a.send(Graph::RemoveVertex("B"));
        let event_a_2 = replica_a.send(Graph::RemoveArc("A", "B", 1));
        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());

        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);

        replica_a.receive(event_b_1);
        replica_a.receive(event_c_1);
        replica_a.receive(event_c_2);

        assert_eq!(replica_a.query().node_count(), 2);
        assert_eq!(replica_a.query().edge_count(), 1);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());
        assert!(vf2::isomorphisms(&replica_a.query(), &replica_c.query())
            .first()
            .is_some());
        assert!(vf2::isomorphisms(&replica_b.query(), &replica_c.query())
            .first()
            .is_some());
    }

    #[test]
    fn multigraph() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event_a = replica_a.send(Graph::AddVertex("A"));
        replica_b.receive(event_a);
        let event_b = replica_b.send(Graph::AddVertex("B"));
        replica_a.receive(event_b);

        let event_a = replica_a.send(Graph::AddArc("A", "B", 1));
        let event_b = replica_b.send(Graph::AddArc("A", "B", 2));

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        println!(
            "{:?}",
            petgraph::dot::Dot::with_config(&replica_a.query(), &[])
        );
        println!(
            "{:?}",
            petgraph::dot::Dot::with_config(&replica_b.query(), &[])
        );

        assert_eq!(replica_a.query().edge_count(), 2);
        assert_eq!(replica_a.query().node_count(), 2);
        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());
    }

    //     #[cfg(feature = "utils")]
    //     #[test]
    //     fn convergence_check() {
    //         use crate::{
    //             protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
    //         };

    //         let mut graph = DiGraph::new();
    //         let idx_a = graph.add_node("A");
    //         let idx_b = graph.add_node("B");
    //         graph.add_edge(idx_a, idx_b, "arc1");
    //         convergence_checker::<EventGraph<Graph<&str, &str>>>(
    //             &[
    //                 Graph::AddVertex("A"),
    //                 Graph::AddVertex("B"),
    //                 Graph::RemoveVertex("B"),
    //                 Graph::RemoveVertex("A"),
    //                 Graph::AddArc("A", "B", "arc1"),
    //                 Graph::RemoveArc("A", "B", "arc1"),
    //             ],
    //             graph,
    //             |g1, g2| vf2::isomorphisms(g1, g2).first().is_some(),
    //         );
    //     }

    //     #[cfg(feature = "op_weaver")]
    //     #[test]
    //     fn op_weaver_multidigraph() {
    //         use crate::{
    //             protocol::event_graph::EventGraph,
    //             utils::op_weaver::{op_weaver, EventGraphConfig},
    //         };

    //         let alphabet = ['a', 'b', 'c', 'd', 'e', 'f'];
    //         let mut names = Vec::new();

    //         // Generate combinations like "aa", "ab", ..., "ff" (36 total), then "aaa", ...
    //         for &c1 in &alphabet {
    //             for &c2 in &alphabet {
    //                 names.push(format!("{}{}", c1, c2));
    //             }
    //         }
    //         for &c1 in &alphabet {
    //             for &c2 in &alphabet {
    //                 for &c3 in &alphabet {
    //                     names.push(format!("{}{}{}", c1, c2, c3));
    //                 }
    //             }
    //         }

    //         let mut ops: Vec<Graph<String, usize>> = Vec::new();
    //         let mut index = 0;

    //         // AddVertex and RemoveVertex: 15,000 of each
    //         while ops.len() < 15000 {
    //             let name = &names[index % names.len()];
    //             ops.push(Graph::AddVertex(name.clone()));
    //             // ops.push(Graph::RemoveVertex(name.clone()));
    //             index += 1;
    //         }

    //         // AddArc and RemoveArc: 7,500 of each
    //         index = 0;
    //         while ops.len() < 30000 {
    //             let from = &names[index % names.len()];
    //             let to = &names[(index + 1) % names.len()];
    //             let weight1 = (index % 10) + 1;
    //             let weight2 = ((index + 5) % 10) + 1;

    //             ops.push(Graph::AddArc(from.clone(), to.clone(), weight1));
    //             // ops.push(Graph::RemoveArc(from.clone(), to.clone(), weight1));
    //             ops.push(Graph::AddArc(from.clone(), to.clone(), weight2));
    //             // ops.push(Graph::RemoveArc(from.clone(), to.clone(), weight2));

    //             index += 1;
    //         }

    //         let config = EventGraphConfig {
    //             name: "aw_multidigraph",
    //             num_replicas: 4,
    //             num_operations: 100_000,
    //             operations: &ops,
    //             final_sync: true,
    //             churn_rate: 0.4,
    //             reachability: None,
    //             compare: |a: &DiGraph<String, usize>, b: &DiGraph<String, usize>| {
    //                 // vf2::isomorphisms(a, b).first().is_some()
    //                 println!(
    //                     "graph size: nodes -> {}, edges -> {}",
    //                     a.node_count(),
    //                     a.edge_count()
    //                 );
    //                 a.node_count() == b.node_count() && a.edge_count() == b.edge_count()
    //             },
    //             record_results: true,
    //             seed: None,
    //             witness_graph: false,
    //             concurrency_score: false,
    //         };

    //         op_weaver::<EventGraph<Graph<String, usize>>>(config);
    //     }
    // }

    // impl<V, E> Graph<V, E>
    // where
    //     V: Debug + Clone + PartialEq + Eq + Hash,
    //     E: Debug + Clone + PartialEq + Eq + Hash,
    // {
    //     /// Returns true if there is no vertex `v1` and `v2` in the graph.
    //     pub fn lookup_arc(v1: &V, v2: &V, dot: &Dot, state: &EventGraph<Self>) -> bool {
    //         let mut v1_found = false;
    //         let mut v2_found = false;
    //         for op in state.stable.iter() {
    //             match op {
    //                 Graph::AddVertex(v) if v == v1 => v1_found = true,
    //                 Graph::AddVertex(v) if v == v2 => v2_found = true,
    //                 _ => {}
    //             }
    //             if v1_found && v2_found {
    //                 break; // Both vertices found, no need to continue
    //             }
    //         }
    //         if !v1_found || !v2_found {
    //             let predecessors = state.causal_predecessors(dot);
    //             for idx in predecessors.iter() {
    //                 let op = &state.unstable.node_weight(*idx).unwrap().0;
    //                 match op {
    //                     Graph::AddVertex(v) if v == v1 => v1_found = true,
    //                     Graph::AddVertex(v) if v == v2 => v2_found = true,
    //                     _ => {}
    //                 }
    //                 if v1_found && v2_found {
    //                     break; // Both vertices found, no need to continue
    //                 }
    //             }
    //         }
    //         !v1_found || !v2_found
    //     }

    //     /// Returns true if there is already a vertex `v` in the graph.
    //     pub fn lookup_vertex(v: &V, dot: &Dot, state: &EventGraph<Self>) -> bool {
    //         let mut found = false;
    //         for op in state.stable.iter() {
    //             if let Graph::AddVertex(vertex) = op {
    //                 if vertex == v {
    //                     found = true;
    //                     break;
    //                 }
    //             }
    //         }
    //         if !found {
    //             let predecessors = state.causal_predecessors(dot);
    //             for idx in predecessors.iter() {
    //                 let op = &state.unstable.node_weight(*idx).unwrap().0;
    //                 if let Graph::AddVertex(vertex) = op {
    //                     if vertex == v {
    //                         found = true;
    //                         break;
    //                     }
    //                 }
    //             }
    //         }
    //         found
    //     }
}
