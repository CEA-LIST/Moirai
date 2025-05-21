use std::{collections::HashMap, fmt::Debug, hash::Hash};

use petgraph::graph::DiGraph;

use crate::{
    clocks::dependency_clock::DependencyClock,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum Graph<V> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V),
    RemoveArc(V, V),
}

impl<V> PureCRDT for Graph<V>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
{
    type Value = DiGraph<V, ()>;
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self) -> bool {
        matches!(new_op, Graph::RemoveVertex(_) | Graph::RemoveArc(_, _))
    }

    fn redundant_by_when_redundant(old_op: &Self, is_conc: bool, new_op: &Self) -> bool {
        match (&old_op, &new_op) {
            (Graph::AddVertex(v1), Graph::AddVertex(v2)) => v1 == v2,
            (Graph::AddVertex(v1), Graph::RemoveVertex(v2)) => !is_conc && v1 == v2,
            (Graph::AddVertex(_), Graph::AddArc(_, _)) => false,
            (Graph::AddVertex(v1), Graph::RemoveArc(v2, v3)) => !is_conc && (v1 == v2 || v1 == v3),
            (Graph::AddArc(_, _), Graph::AddVertex(_)) => false,
            (Graph::AddArc(v1, v2), Graph::RemoveVertex(v3)) => v3 == v1 || v3 == v2,
            (Graph::AddArc(v1, v2), Graph::AddArc(v3, v4)) => v1 == v3 && v2 == v4,
            (Graph::AddArc(v1, v2), Graph::RemoveArc(v3, v4)) => !is_conc && v1 == v3 && v2 == v4,
            _ => false,
        }
    }

    fn redundant_by_when_not_redundant(old_op: &Self, is_conc: bool, new_op: &Self) -> bool {
        Self::redundant_by_when_redundant(old_op, is_conc, new_op)
    }

    fn stabilize(_metadata: &DependencyClock, _state: &mut EventGraph<Self>) {}

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        for o in stable.iter().chain(unstable.iter()) {
            match o {
                Graph::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        graph.add_edge(*a, *b, ());
                    }
                }
                // No "remove" operation can be in the stable set
                _ => {}
            }
        }
        for o in stable.iter().chain(unstable.iter()) {
            match o {
                Graph::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        let idx = graph.add_edge(*a, *b, ());
                        edge_index.insert((v1, v2), idx);
                    }
                }
                Graph::RemoveVertex(v) => {
                    // the vertex should be already in the node_index map anyway
                    if let Some(idx) = node_index.get(v) {
                        graph.remove_node(*idx);
                    }
                }
                Graph::RemoveArc(v1, v2) => {
                    // the vertex should be already in the node_index map anyway
                    if let Some(idx) = edge_index.get(&(v1, v2)) {
                        graph.remove_edge(*idx);
                    }
                }
            }
        }
        graph
    }
}

#[cfg(test)]
mod tests {
    use petgraph::algo::is_isomorphic;

    use crate::crdt::{aw_graph::Graph, test_util::twins_graph};

    #[test_log::test]
    fn simple_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str>>();

        let event = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(Graph::AddArc("B", "A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        tcsb_a.try_deliver(event);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test_log::test]
    fn concurrent_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<Graph<&str>>();

        let event = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.try_deliver(event);

        let event_b = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        let event_a = tcsb_a.tc_bcast(Graph::AddArc("B", "A"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }
}
