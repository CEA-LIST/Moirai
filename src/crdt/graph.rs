use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign},
};

use petgraph::graph::DiGraph;
use serde::Serialize;

use crate::trcb::{Event, Message, OpRules};

#[derive(Clone, Debug)]
pub enum Op<V> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V),
    RemoveArc(V, V),
}

impl<V> OpRules for Op<V>
where
    V: Debug + Clone + Eq + Hash + Serialize,
{
    type Value = DiGraph<V, ()>;

    // Add-wins policy
    fn obsolete<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        is_obsolete: &Event<K, C, Self>,
        other: &Event<K, C, Self>,
    ) -> bool {
        match (&is_obsolete.message, &other.message) {
            (_, Message::Signal(_)) => false,
            (Message::Signal(_), _) => false,
            (Message::Op(Op::AddVertex(v1)), Message::Op(Op::AddVertex(v2)))
            | (Message::Op(Op::AddVertex(v1)), Message::Op(Op::RemoveVertex(v2))) => {
                is_obsolete.vc < other.vc && v1 == v2
            }
            (Message::Op(Op::AddVertex(_)), Message::Op(Op::AddArc(_, _)))
            | (Message::Op(Op::AddArc(_, _)), Message::Op(Op::AddVertex(_)))
            | (Message::Op(Op::AddVertex(_)), Message::Op(Op::RemoveArc(_, _))) => false,
            (Message::Op(Op::RemoveVertex(_)), _) | (Message::Op(Op::RemoveArc(_, _)), _) => true,
            (Message::Op(Op::AddArc(v1, v2)), Message::Op(Op::AddArc(v3, v4)))
            | (Message::Op(Op::AddArc(v1, v2)), Message::Op(Op::RemoveArc(v3, v4))) => {
                is_obsolete.vc < other.vc && v1 == v3 && v2 == v4
            }
            (Message::Op(Op::AddArc(v1, v2)), Message::Op(Op::RemoveVertex(v3))) => {
                v1 == v3 || v2 == v3
            }
        }
    }

    fn eval<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        unstable_events: &[Event<K, C, Self>],
        stable_events: &[Self],
    ) -> Self::Value {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        for event in stable_events {
            match event {
                Op::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Op::AddArc(v1, v2) => {
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
            match &event.message {
                Message::Op(Op::AddVertex(v)) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Message::Op(Op::AddArc(v1, v2)) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    let idx = graph.add_edge(
                        *node_index.get(v1).unwrap(),
                        *node_index.get(v2).unwrap(),
                        (),
                    );
                    edge_index.insert((v1, v2), idx);
                }
                Message::Op(Op::RemoveVertex(v)) => {
                    // the vertex should be already in the node_index map anyway
                    if let Some(idx) = node_index.get(v) {
                        graph.remove_node(*idx);
                    }
                }
                Message::Op(Op::RemoveArc(v1, v2)) => {
                    // the vertex should be already in the node_index map anyway
                    if let Some(idx) = edge_index.get(&(v1, v2)) {
                        graph.remove_edge(*idx);
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
    use crate::{
        crdt::graph::Op,
        trcb::{Message, Signal, Trcb},
    };
    use petgraph::algo::is_isomorphic;
    use uuid::Uuid;

    #[cfg(feature = "dhat-heap")]
    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    #[test_log::test]
    fn test_graph() {
        #[cfg(feature = "dhat-heap")]
        let _profiler = dhat::Profiler::new_heap();

        let id_a = Uuid::new_v4().to_string();
        let id_b = Uuid::new_v4().to_string();

        let mut trcb_a = Trcb::<&str, u32, Op<&str>>::new(id_a.as_str());
        let mut trcb_b = Trcb::<&str, u32, Op<&str>>::new(id_b.as_str());

        let event_a = trcb_a.tc_bcast(Message::Signal(Signal::Join));
        trcb_b.tc_deliver(event_a);

        let event_b = trcb_b.tc_bcast(Message::Signal(Signal::Join));
        trcb_a.tc_deliver(event_b);

        let event = trcb_a.tc_bcast(Message::Op(Op::AddVertex("A")));
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Message::Op(Op::AddVertex("B")));
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Message::Op(Op::AddArc("A", "B")));
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Message::Op(Op::RemoveVertex("A")));
        trcb_b.tc_deliver(event);

        let event = trcb_b.tc_bcast(Message::Op(Op::RemoveArc("A", "B")));
        trcb_a.tc_deliver(event);

        assert!(is_isomorphic(&trcb_a.eval(), &trcb_b.eval()));
    }
}
