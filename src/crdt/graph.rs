use std::{collections::HashMap, fmt::Debug, hash::Hash};

use petgraph::graph::DiGraph;

use crate::trcb::{Event, OpRules};

#[derive(Clone, Debug)]
pub enum Operation<V> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V),
    RemoveArc(V, V),
}

impl<V> OpRules<&str, u32> for Operation<V>
where
    V: Debug + Clone + Eq + Hash,
{
    type Value = DiGraph<V, ()>;

    // Add-wins policy
    fn obsolete(is_obsolete: &Event<&str, u32, Self>, other: &Event<&str, u32, Self>) -> bool {
        match (&is_obsolete.op, &other.op) {
            (Operation::AddVertex(v1), Operation::AddVertex(v2))
            | (Operation::AddVertex(v1), Operation::RemoveVertex(v2)) => {
                is_obsolete.vc < other.vc && v1 == v2
            }
            (Operation::AddVertex(_), Operation::AddArc(_, _))
            | (Operation::AddArc(_, _), Operation::AddVertex(_))
            | (Operation::AddVertex(_), Operation::RemoveArc(_, _)) => false,
            (Operation::RemoveVertex(_), _) | (Operation::RemoveArc(_, _), _) => true,
            (Operation::AddArc(v1, v2), Operation::AddArc(v3, v4))
            | (Operation::AddArc(v1, v2), Operation::RemoveArc(v3, v4)) => {
                is_obsolete.vc < other.vc && v1 == v3 && v2 == v4
            }
            (Operation::AddArc(v1, v2), Operation::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
        }
    }

    fn eval(unstable_events: &[Event<&str, u32, Self>], stable_events: &[Self]) -> Self::Value {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        for event in stable_events {
            match event {
                Operation::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Operation::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    graph.add_edge(
                        *node_index.get(v1).unwrap(),
                        *node_index.get(v2).unwrap(),
                        (),
                    );
                }
                // No "remove" operation can be in the stable set
                _ => {}
            }
        }
        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        for event in unstable_events {
            match &event.op {
                Operation::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Operation::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    let idx = graph.add_edge(
                        *node_index.get(v1).unwrap(),
                        *node_index.get(v2).unwrap(),
                        (),
                    );
                    edge_index.insert((v1, v2), idx);
                }
                Operation::RemoveVertex(v) => {
                    // the vertex should be already in the node_index map anyway
                    if let Some(idx) = node_index.get(v) {
                        graph.remove_node(*idx);
                    }
                }
                Operation::RemoveArc(v1, v2) => {
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
    use crate::{crdt::graph::Operation, trcb::Trcb};
    use petgraph::algo::is_isomorphic;

    #[cfg(feature = "dhat-heap")]
    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    #[test]
    fn test_graph() {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        let mut trcb_a = Trcb::<&str, u32, Operation<&str>>::new("A");
        let mut trcb_b = Trcb::<&str, u32, Operation<&str>>::new("B");

        trcb_a.new_peer(&"B");
        trcb_b.new_peer(&"A");

        let event = trcb_a.tc_bcast(Operation::AddVertex("A"));
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Operation::AddVertex("B"));
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Operation::AddArc("A", "B"));
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Operation::RemoveVertex("A"));
        trcb_b.tc_deliver(event);

        let event = trcb_b.tc_bcast(Operation::RemoveArc("A", "B"));
        trcb_a.tc_deliver(event);

        assert!(is_isomorphic(&trcb_a.eval(), &trcb_b.eval()));
    }
}
