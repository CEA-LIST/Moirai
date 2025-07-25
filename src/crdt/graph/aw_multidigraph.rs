use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};

use petgraph::graph::DiGraph;

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum Graph<V, E> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V, E),
    RemoveArc(V, V, E),
}

impl<V, E> Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    /// Returns true if there is no vertex `v1` and `v2` in the graph.
    pub fn lookup_arc(v1: &V, v2: &V, dot: &Dot, state: &EventGraph<Self>) -> bool {
        let mut v1_found = false;
        let mut v2_found = false;
        for op in state.stable.iter() {
            match op {
                Graph::AddVertex(v) if v == v1 => v1_found = true,
                Graph::AddVertex(v) if v == v2 => v2_found = true,
                _ => {}
            }
            if v1_found && v2_found {
                break; // Both vertices found, no need to continue
            }
        }
        if !v1_found || !v2_found {
            let predecessors = state.causal_predecessors(dot);
            for idx in predecessors.iter() {
                let op = &state.unstable.node_weight(*idx).unwrap().0;
                match op {
                    Graph::AddVertex(v) if v == v1 => v1_found = true,
                    Graph::AddVertex(v) if v == v2 => v2_found = true,
                    _ => {}
                }
                if v1_found && v2_found {
                    break; // Both vertices found, no need to continue
                }
            }
        }
        !v1_found || !v2_found
    }

    /// Returns true if there is already a vertex `v` in the graph.
    pub fn lookup_vertex(v: &V, dot: &Dot, state: &EventGraph<Self>) -> bool {
        let mut found = false;
        for op in state.stable.iter() {
            if let Graph::AddVertex(vertex) = op {
                if vertex == v {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            let predecessors = state.causal_predecessors(dot);
            for idx in predecessors.iter() {
                let op = &state.unstable.node_weight(*idx).unwrap().0;
                if let Graph::AddVertex(vertex) = op {
                    if vertex == v {
                        found = true;
                        break;
                    }
                }
            }
        }
        found
    }
}

impl<V, E> PureCRDT for Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    type Value = DiGraph<V, E>;
    // The stable part can be a tuple of HashSet
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, Graph::RemoveVertex(_) | Graph::RemoveArc(_, _, _))
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        _new_dot: &Dot,
    ) -> bool {
        // old_op = addVertex, addArc only
        !is_conc
            && match (old_op, new_op) {
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
        old_dot: Option<&Dot>,
        is_conc: bool,
        new_op: &Self,
        new_dot: &Dot,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_dot, is_conc, new_op, new_dot)
    }

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut ops: Vec<&Self> = stable.iter().chain(unstable.iter()).collect();
        ops.sort_by(|a, b| match (a, b) {
            (Graph::AddVertex(_), Graph::AddArc(_, _, _)) => std::cmp::Ordering::Less,
            (Graph::AddArc(_, _, _), Graph::AddVertex(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
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

    use crate::crdt::{
        graph::aw_multidigraph::Graph,
        test_util::{triplet_graph, twins_graph},
    };

    #[test_log::test]
    fn simple_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, &str>>();

        let event = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(Graph::AddArc("B", "A", "arc1"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        tcsb_a.try_deliver(event);

        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval(),)
            .first()
            .is_some());
    }

    #[test_log::test]
    fn concurrent_graph_arc() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, &str>>();

        let event = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.try_deliver(event);

        let event_b = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        let event_a = tcsb_a.tc_bcast(Graph::AddArc("B", "A", "arc1"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval(),)
            .first()
            .is_some());
    }

    #[test_log::test]
    fn concurrent_graph_vertex() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, &str>>();

        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        let event_b = tcsb_b.tc_bcast(Graph::AddVertex("A"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval().node_count(), 1);
        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval(),)
            .first()
            .is_some());
    }

    #[test_log::test]
    fn graph_arc_no_vertex() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, u8>>();

        let event = tcsb_a.tc_bcast(Graph::AddArc("A", "B", 1));
        tcsb_b.try_deliver(event);

        assert!(
            vf2::isomorphisms(&tcsb_a.eval(), &DiGraph::<&str, u8>::new(),)
                .first()
                .is_some()
        );
    }

    #[test_log::test]
    fn graph_multiple_vertex_same_id() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, u8>>();

        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(Graph::AddVertex("A"));
        tcsb_a.try_deliver(event_b);
        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval().node_count(), 1);
    }

    #[test_log::test]
    fn revive_arc() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, u8>>();

        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.try_deliver(event_b);

        let event_a = tcsb_a.tc_bcast(Graph::AddArc("A", "B", 1));
        let event_b = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval(),)
            .first()
            .is_some());

        assert_eq!(tcsb_a.eval().node_count(), 1);
        assert_eq!(tcsb_a.eval().edge_count(), 0);

        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("B"));
        tcsb_b.try_deliver(event_a);

        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_a.eval(), &[]));
        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_b.eval(), &[]));

        assert_eq!(tcsb_a.eval().node_count(), 2);
        assert_eq!(tcsb_a.eval().edge_count(), 1);

        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval(),)
            .first()
            .is_some());
    }

    #[test_log::test]
    fn revive_arc_2() {
        let (mut tcsb_a, mut tcsb_b, mut tcsb_c) = triplet_graph::<Graph<&str, u8>>();

        let event_b_1 = tcsb_b.tc_bcast(Graph::AddVertex("A"));
        tcsb_c.try_deliver(event_b_1.clone());
        let event_c_1 = tcsb_c.tc_bcast(Graph::AddVertex("B"));
        let event_c_2 = tcsb_c.tc_bcast(Graph::AddArc("A", "B", 1));
        tcsb_b.try_deliver(event_c_1.clone());
        tcsb_b.try_deliver(event_c_2.clone());

        assert!(vf2::isomorphisms(&tcsb_b.eval(), &tcsb_c.eval(),)
            .first()
            .is_some());

        let event_a_1 = tcsb_a.tc_bcast(Graph::RemoveVertex("B"));
        let event_a_2 = tcsb_a.tc_bcast(Graph::RemoveArc("A", "B", 1));
        tcsb_b.try_deliver(event_a_1.clone());
        tcsb_b.try_deliver(event_a_2.clone());

        tcsb_c.try_deliver(event_a_1);
        tcsb_c.try_deliver(event_a_2);

        tcsb_a.try_deliver(event_b_1);
        tcsb_a.try_deliver(event_c_1);
        tcsb_a.try_deliver(event_c_2);

        assert_eq!(tcsb_a.eval().node_count(), 2);
        assert_eq!(tcsb_a.eval().edge_count(), 1);

        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval())
            .first()
            .is_some());
        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_c.eval())
            .first()
            .is_some());
        assert!(vf2::isomorphisms(&tcsb_b.eval(), &tcsb_c.eval())
            .first()
            .is_some());
    }

    #[test_log::test]
    fn multigraph() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, u8>>();

        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.try_deliver(event_b);

        let event_a = tcsb_a.tc_bcast(Graph::AddArc("A", "B", 1));
        let event_b = tcsb_b.tc_bcast(Graph::AddArc("A", "B", 2));

        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_a.eval(), &[]));
        println!("{:?}", petgraph::dot::Dot::with_config(&tcsb_b.eval(), &[]));

        assert_eq!(tcsb_a.eval().edge_count(), 2);
        assert_eq!(tcsb_a.eval().node_count(), 2);
        assert!(vf2::isomorphisms(&tcsb_a.eval(), &tcsb_b.eval())
            .first()
            .is_some());
    }

    #[cfg(feature = "utils")]
    #[test_log::test]
    fn convergence_check() {
        use crate::{
            protocol::event_graph::EventGraph, utils::convergence_checker::convergence_checker,
        };

        let mut graph = DiGraph::new();
        let idx_a = graph.add_node("A");
        let idx_b = graph.add_node("B");
        graph.add_edge(idx_a, idx_b, "arc1");
        convergence_checker::<EventGraph<Graph<&str, &str>>>(
            &[
                Graph::AddVertex("A"),
                Graph::AddVertex("B"),
                Graph::RemoveVertex("B"),
                Graph::RemoveVertex("A"),
                Graph::AddArc("A", "B", "arc1"),
                Graph::RemoveArc("A", "B", "arc1"),
            ],
            graph,
            |g1, g2| vf2::isomorphisms(g1, g2).first().is_some(),
        );
    }

    #[cfg(feature = "op_weaver")]
    #[test_log::test]
    fn op_weaver_multidigraph() {
        use crate::{
            protocol::event_graph::EventGraph,
            utils::op_weaver::{op_weaver, EventGraphConfig},
        };

        let alphabet = ['a', 'b', 'c', 'd', 'e', 'f'];
        let mut names = Vec::new();

        // Generate combinations like "aa", "ab", ..., "ff" (36 total), then "aaa", ...
        for &c1 in &alphabet {
            for &c2 in &alphabet {
                names.push(format!("{}{}", c1, c2));
            }
        }
        for &c1 in &alphabet {
            for &c2 in &alphabet {
                for &c3 in &alphabet {
                    names.push(format!("{}{}{}", c1, c2, c3));
                }
            }
        }

        let mut ops: Vec<Graph<String, usize>> = Vec::new();
        let mut index = 0;

        // AddVertex and RemoveVertex: 15,000 of each
        while ops.len() < 15000 {
            let name = &names[index % names.len()];
            ops.push(Graph::AddVertex(name.clone()));
            ops.push(Graph::RemoveVertex(name.clone()));
            index += 1;
        }

        // AddArc and RemoveArc: 7,500 of each
        index = 0;
        while ops.len() < 30000 {
            let from = &names[index % names.len()];
            let to = &names[(index + 1) % names.len()];
            let weight1 = (index % 10) + 1;
            let weight2 = ((index + 5) % 10) + 1;

            ops.push(Graph::AddArc(from.clone(), to.clone(), weight1));
            ops.push(Graph::RemoveArc(from.clone(), to.clone(), weight1));
            ops.push(Graph::AddArc(from.clone(), to.clone(), weight2));
            ops.push(Graph::RemoveArc(from.clone(), to.clone(), weight2));

            index += 1;
        }

        let config = EventGraphConfig {
            name: "aw_multidigraph",
            num_replicas: 8,
            num_operations: 10_000,
            operations: &ops,
            final_sync: true,
            churn_rate: 0.8,
            reachability: None,
            compare: |a: &DiGraph<String, usize>, b: &DiGraph<String, usize>| {
                vf2::isomorphisms(a, b).first().is_some()
            },
            record_results: false,
            seed: None,
            witness_graph: false,
            concurrency_score: false,
        };

        op_weaver::<EventGraph<Graph<String, usize>>>(config);
    }
}
