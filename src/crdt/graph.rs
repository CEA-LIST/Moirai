use petgraph::graph::DiGraph;

use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::po_log::POLog;
use crate::protocol::pure_crdt::PureCRDT;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Clone, Debug)]
pub enum Graph<V> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V),
    RemoveArc(V, V),
}

impl<V> PureCRDT for Graph<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = DiGraph<V, ()>;

    fn r(new_event: &Event<Self>, _old_event: &Event<Self>) -> bool {
        match &new_event.op {
            Graph::AddVertex(_) => false,
            Graph::RemoveVertex(_) => true,
            Graph::AddArc(_, _) => false,
            Graph::RemoveArc(_, _) => true,
        }
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        match (&old_event.op, &new_event.op) {
            (Graph::AddVertex(v1), Graph::AddVertex(v2)) => {
                matches!(
                    old_event
                        .metadata
                        .clock
                        .partial_cmp(&new_event.metadata.clock),
                    None | Some(Ordering::Less)
                ) && v1 == v2
            }
            (Graph::AddVertex(v1), Graph::RemoveVertex(v2)) => {
                old_event.metadata.clock < new_event.metadata.clock && v1 == v2
            }
            (Graph::AddVertex(_), Graph::AddArc(_, _)) => false,
            (Graph::AddVertex(v1), Graph::RemoveArc(v2, v3)) => {
                old_event.metadata.clock < new_event.metadata.clock && (v1 == v2 || v1 == v3)
            }
            (Graph::AddArc(_, _), Graph::AddVertex(_)) => false,
            (Graph::AddArc(v1, v2), Graph::RemoveVertex(v3)) => {
                matches!(
                    old_event
                        .metadata
                        .clock
                        .partial_cmp(&new_event.metadata.clock),
                    None | Some(Ordering::Less)
                ) && (v3 == v1 || v3 == v2)
            }
            (Graph::AddArc(v1, v2), Graph::AddArc(v3, v4)) => {
                matches!(
                    old_event
                        .metadata
                        .clock
                        .partial_cmp(&new_event.metadata.clock),
                    None | Some(Ordering::Less)
                ) && v1 == v3
                    && v2 == v4
            }
            (Graph::AddArc(v1, v2), Graph::RemoveArc(v3, v4)) => {
                old_event.metadata.clock < new_event.metadata.clock && v1 == v3 && v2 == v4
            }
            _ => false,
        }
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(_metadata: &Metadata, _state: &mut POLog<Self>) {}

    fn eval(ops: &[Self]) -> Self::Value {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        for o in ops {
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
        for o in ops {
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

    use crate::crdt::{graph::Graph, test_util::twins_po};

    #[test_log::test]
    fn simple_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins_po::<Graph<&str>>();

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
        let (mut tcsb_a, mut tcsb_b) = twins_po::<Graph<&str>>();

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
