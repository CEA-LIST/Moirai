use std::{collections::HashMap, fmt::Debug, hash::Hash};

use petgraph::graph::DiGraph;

use crate::{
    clocks::dot::Dot,
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

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, Graph::RemoveVertex(_) | Graph::RemoveArc(_, _))
    }

    fn redundant_by_when_redundant(old_op: &Self, _old_dot: Option<&Dot>, is_conc: bool, new_op: &Self, _new_dot: &Dot) -> bool {
        // old_op = addVertex, addArc only
        !is_conc && match (old_op, new_op) {
            (Graph::AddArc(v1, v2), Graph::AddArc(v3, v4)) => 
                v1 == v3 && v2 == v4,
            (Graph::AddArc(_, _), Graph::AddVertex(_)) => false,
            (Graph::AddArc(v1, v2), Graph::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
            (Graph::AddArc(v1, v2), Graph::RemoveArc(v3, v4)) => 
                v1 == v3 && v2 == v4,
            (Graph::AddVertex(v1), Graph::AddVertex(v2)) => v1 == v2,
            (Graph::AddVertex(_), Graph::AddArc(_, _)) => false,
            (Graph::AddVertex(_), Graph::RemoveArc(_, _)) => false,
            (Graph::AddVertex(v1), Graph::RemoveVertex(v2)) => v1 == v2,
            _ => false,
        }
    }

    fn redundant_by_when_not_redundant(old_op: &Self, old_dot: Option<&Dot>, is_conc: bool, new_op: &Self, new_dot: &Dot) -> bool {
        Self::redundant_by_when_redundant(old_op,old_dot, is_conc, new_op, new_dot)
    }

    fn stabilize(_metadata: &Dot, _state: &mut EventGraph<Self>) {}

    fn eval(stable: &Self::Stable, unstable: &[Self]) -> Self::Value {
        let mut ops: Vec<&Self> = stable.iter().chain(unstable.iter()).collect();
        ops.sort_by(|a, b| match (a, b) {
            (Graph::AddVertex(_), Graph::AddArc(_, _)) => std::cmp::Ordering::Less,
            (Graph::AddArc(_, _), Graph::AddVertex(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        });
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        for o in ops {
            match o {
                Graph::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2) => {
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        graph.add_edge(*a, *b, ());
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
