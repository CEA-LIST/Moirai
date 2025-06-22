use std::{collections::HashMap, fmt::Debug, hash::Hash};

use petgraph::graph::DiGraph;

use crate::{
    clocks::dot::Dot,
    protocol::{event_graph::EventGraph, pure_crdt::PureCRDT},
};

#[derive(Clone, Debug)]
pub enum AWGraph<V> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V),
    RemoveArc(V, V),
}

impl<V> PureCRDT for AWGraph<V>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
{
    type Value = DiGraph<V, ()>;
    type Stable = Vec<Self>;

    fn redundant_itself(new_op: &Self, _new_dot: &Dot, _state: &EventGraph<Self>) -> bool {
        matches!(new_op, AWGraph::RemoveVertex(_) | AWGraph::RemoveArc(_, _))
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
                (AWGraph::AddArc(v1, v2), AWGraph::AddArc(v3, v4)) => v1 == v3 && v2 == v4,
                (AWGraph::AddArc(_, _), AWGraph::AddVertex(_)) => false,
                (AWGraph::AddArc(v1, v2), AWGraph::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
                (AWGraph::AddArc(v1, v2), AWGraph::RemoveArc(v3, v4)) => v1 == v3 && v2 == v4,
                (AWGraph::AddVertex(v1), AWGraph::AddVertex(v2)) => {
                    println!("Comparing vertices: {v1:?} and {v2:?}");
                    println!("Are they equal? {}", v1 == v2);
                    v1 == v2
                }
                (AWGraph::AddVertex(_), AWGraph::AddArc(_, _)) => false,
                (AWGraph::AddVertex(_), AWGraph::RemoveArc(_, _)) => false,
                (AWGraph::AddVertex(v1), AWGraph::RemoveVertex(v2)) => v1 == v2,
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
            (AWGraph::AddVertex(_), AWGraph::AddArc(_, _)) => std::cmp::Ordering::Less,
            (AWGraph::AddArc(_, _), AWGraph::AddVertex(_)) => std::cmp::Ordering::Greater,
            _ => std::cmp::Ordering::Equal,
        });
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        for o in ops {
            match o {
                AWGraph::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                AWGraph::AddArc(v1, v2) => {
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
    use petgraph::{algo::is_isomorphic, graph::DiGraph, prelude::StableDiGraph};

    use crate::crdt::{aw_graph::AWGraph, test_util::twins_graph};

    #[test]
    fn simple_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<AWGraph<&str>>();

        let event = tcsb_a.tc_bcast(AWGraph::AddVertex("A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(AWGraph::AddVertex("B"));
        tcsb_a.try_deliver(event);

        let event = tcsb_a.tc_bcast(AWGraph::AddArc("B", "A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(AWGraph::RemoveVertex("B"));
        tcsb_a.try_deliver(event);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test]
    fn concurrent_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<AWGraph<&str>>();

        let event = tcsb_a.tc_bcast(AWGraph::AddVertex("A"));
        tcsb_b.try_deliver(event);

        let event = tcsb_b.tc_bcast(AWGraph::AddVertex("B"));
        tcsb_a.try_deliver(event);

        let event_b = tcsb_b.tc_bcast(AWGraph::RemoveVertex("B"));
        let event_a = tcsb_a.tc_bcast(AWGraph::AddArc("B", "A"));
        tcsb_b.try_deliver(event_a);
        tcsb_a.try_deliver(event_b);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test]
    fn graph_arc_no_vertex() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<AWGraph<&str>>();

        let event = tcsb_a.tc_bcast(AWGraph::AddArc("A", "B"));
        tcsb_b.try_deliver(event);

        assert!(is_isomorphic(&tcsb_a.eval(), &DiGraph::<&str, ()>::new()));
    }

    #[test]
    fn graph_multiple_vertex_same_id() {
        let (mut tcsb_a, mut tcsb_b) = twins_graph::<AWGraph<&str>>();

        let event_a = tcsb_a.tc_bcast(AWGraph::AddVertex("A"));
        tcsb_b.try_deliver(event_a);
        let event_b = tcsb_b.tc_bcast(AWGraph::AddVertex("A"));
        tcsb_a.try_deliver(event_b);
        let event_a = tcsb_a.tc_bcast(AWGraph::AddVertex("A"));
        tcsb_b.try_deliver(event_a);

        assert_eq!(tcsb_a.eval().node_count(), 1);
    }

    #[test]
    fn test_stable_graph() {
        let mut stable_graph = StableDiGraph::<String, String>::new();
        stable_graph.add_node("Node0".to_string());
        stable_graph.add_node("Node0".to_string());
        // let test = stable_graph.add_edge(1.into(), 0.into(), "Edge1".to_string());
        // stable_graph.add_edge(1.into(), 0.into(), "Edge2".to_string());

        println!("edge index: {:?}", stable_graph);

        println!("{:?}", petgraph::dot::Dot::with_config(&stable_graph, &[]));
    }
}
