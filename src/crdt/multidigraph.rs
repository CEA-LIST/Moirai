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

impl<V, E> PureCRDT for Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    type Value = DiGraph<V, E>;
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
                    if edge_index.contains(&(v1, v2, e)) {
                        continue; // Skip if the edge already exists
                    }
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
    use petgraph::{algo::is_isomorphic, graph::DiGraph};

    use crate::crdt::{multidigraph::Graph, test_util::twins_graph};

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

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
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

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test_log::test]
    fn concurrent_graph_vertex() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, &str>>();

        let event_a = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        let event_b = tcsb_b.tc_bcast(Graph::AddVertex("A"));
        tcsb_a.try_deliver(event_b);
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval().node_count(), 1);
        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test_log::test]
    fn graph_arc_no_vertex() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str, u8>>();

        let event = tcsb_a.tc_bcast(Graph::AddArc("A", "B", 1));
        tcsb_b.try_deliver(event);

        assert!(is_isomorphic(&tcsb_a.eval(), &DiGraph::<&str, ()>::new()));
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
        assert!(petgraph::algo::is_isomorphic(
            &tcsb_a.eval(),
            &tcsb_b.eval()
        ));
    }
}
