use petgraph::graph::DiGraph;

use crate::protocol::event::Event;
use crate::protocol::metadata::Metadata;
use crate::protocol::pure_crdt::PureCRDT;
use crate::protocol::tcsb::POLog;
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

    fn r(event: &Event<Self>, state: &POLog<Self>) -> bool {
        match &event.op {
            Graph::AddVertex(_) => false,
            Graph::RemoveVertex(_) => true,
            Graph::AddArc(v1, v2) => Self::lookup(v1, v2, state),
            Graph::RemoveArc(_, _) => true,
        }
    }

    fn r_zero(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        match (&old_event.op, &new_event.op) {
            (Graph::AddVertex(v1), Graph::AddVertex(v2)) => {
                matches!(
                    old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
                    None | Some(Ordering::Less)
                ) && v1 == v2
            }
            (Graph::AddVertex(v1), Graph::RemoveVertex(v2)) => {
                old_event.metadata.vc < new_event.metadata.vc && v1 == v2
            }
            (Graph::AddVertex(_), Graph::AddArc(_, _)) => false,
            (Graph::AddVertex(v1), Graph::RemoveArc(v2, v3)) => {
                old_event.metadata.vc < new_event.metadata.vc && (v1 == v2 || v1 == v3)
            }
            (Graph::AddArc(_, _), Graph::AddVertex(_)) => false,
            (Graph::AddArc(v1, v2), Graph::RemoveVertex(v3)) => {
                matches!(
                    old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
                    None | Some(Ordering::Less)
                ) && (v3 == v1 || v3 == v2)
            }
            (Graph::AddArc(v1, v2), Graph::AddArc(v3, v4)) => {
                matches!(
                    old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
                    None | Some(Ordering::Less)
                ) && v1 == v3
                    && v2 == v4
            }
            (Graph::AddArc(v1, v2), Graph::RemoveArc(v3, v4)) => {
                old_event.metadata.vc < new_event.metadata.vc && v1 == v3 && v2 == v4
            }
            _ => false,
        }
    }

    fn r_one(old_event: &Event<Self>, new_event: &Event<Self>) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize(_: &Metadata, _: &mut POLog<Self>) {}

    fn eval(state: &POLog<Self>) -> Self::Value {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        for o in &state.0 {
            match o {
                Graph::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    graph.add_edge(
                        *node_index.get(&v1).unwrap(),
                        *node_index.get(&v2).unwrap(),
                        (),
                    );
                }
                // No "remove" operation can be in the stable set
                _ => {}
            }
        }
        for o in state.1.values() {
            match o {
                Graph::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    let idx = graph.add_edge(
                        *node_index.get(v1).unwrap(),
                        *node_index.get(v2).unwrap(),
                        (),
                    );
                    edge_index.insert((v1, v2), idx);
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

impl<V> Graph<V>
where
    V: Debug + Clone + Hash + Eq,
{
    fn lookup(v1: &V, v2: &V, state: &POLog<Self>) -> bool {
        let mut found_v1 = false;
        let mut found_v2 = false;

        for o in state.0.iter() {
            if found_v1 && found_v2 {
                break;
            }
            if let Graph::AddVertex(v) = o {
                if v == v1 {
                    found_v1 = true;
                }
                if v == v2 {
                    found_v2 = true;
                }
            }
        }
        for o in state.1.values() {
            if found_v1 && found_v2 {
                break;
            }
            if let Graph::AddVertex(v) = o {
                if v == v1 {
                    found_v1 = true;
                }
                if v == v2 {
                    found_v2 = true;
                }
            }
        }
        !found_v1 || !found_v2
    }
}

#[cfg(test)]
mod tests {
    use petgraph::algo::is_isomorphic;

    use crate::crdt::{graph::Graph, test_util::twins};

    #[test_log::test]
    fn simple_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Graph<&str>>();

        let event = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.tc_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.tc_deliver(event);

        let event = tcsb_a.tc_bcast(Graph::AddArc("B", "A"));
        tcsb_b.tc_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        tcsb_a.tc_deliver(event);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }

    #[test_log::test]
    fn concurrent_graph() {
        let (mut tcsb_a, mut tcsb_b) = twins::<Graph<&str>>();

        let event = tcsb_a.tc_bcast(Graph::AddVertex("A"));
        tcsb_b.tc_deliver(event);

        let event = tcsb_b.tc_bcast(Graph::AddVertex("B"));
        tcsb_a.tc_deliver(event);

        let event_b = tcsb_b.tc_bcast(Graph::RemoveVertex("B"));
        let event_a = tcsb_a.tc_bcast(Graph::AddArc("B", "A"));
        tcsb_b.tc_deliver(event_a);
        tcsb_a.tc_deliver(event_b);

        assert!(is_isomorphic(&tcsb_a.eval(), &tcsb_b.eval()));
    }
}
