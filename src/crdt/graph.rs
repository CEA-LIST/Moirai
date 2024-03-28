use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::{Add, AddAssign},
};

use petgraph::graph::DiGraph;
use serde::{Deserialize, Serialize};

use crate::protocol::{event::OpEvent, op_rules::OpRules};

#[derive(Deserialize, Serialize, Clone, Debug)]
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
        is_obsolete: &OpEvent<K, C, Self>,
        other: &OpEvent<K, C, Self>,
    ) -> bool {
        match (&is_obsolete.op, &other.op) {
            (Op::AddVertex(v1), Op::AddVertex(v2)) | (Op::AddVertex(v1), Op::RemoveVertex(v2)) => {
                is_obsolete.metadata.vc < other.metadata.vc && v1 == v2
            }
            (Op::AddVertex(_), Op::AddArc(_, _))
            | (Op::AddArc(_, _), Op::AddVertex(_))
            | (Op::AddVertex(_), Op::RemoveArc(_, _)) => false,
            (Op::RemoveVertex(_), _) | (Op::RemoveArc(_, _), _) => true,
            (Op::AddArc(v1, v2), Op::AddArc(v3, v4))
            | (Op::AddArc(v1, v2), Op::RemoveArc(v3, v4)) => {
                is_obsolete.metadata.vc < other.metadata.vc && v1 == v3 && v2 == v4
            }
            (Op::AddArc(v1, v2), Op::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
        }
    }

    fn eval<
        K: PartialOrd + Hash + Eq + Clone + Debug,
        C: Add<C, Output = C> + AddAssign<C> + From<u8> + Ord + Default + Clone + Debug,
    >(
        unstable_events: &[&OpEvent<K, C, Self>],
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
            match &event.op {
                Op::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Op::AddArc(v1, v2) => {
                    // probably safe to unwrap because node and edges are inserted in order
                    // (i.e. node before edge)
                    let idx = graph.add_edge(
                        *node_index.get(v1).unwrap(),
                        *node_index.get(v2).unwrap(),
                        (),
                    );
                    edge_index.insert((v1, v2), idx);
                }
                Op::RemoveVertex(v) => {
                    // the vertex should be already in the node_index map anyway
                    if let Some(idx) = node_index.get(v) {
                        graph.remove_node(*idx);
                    }
                }
                Op::RemoveArc(v1, v2) => {
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
    use crate::{
        crdt::graph::Op,
        protocol::{
            event::{Message, ProtocolCmd},
            trcb::Trcb,
        },
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

        let event_a = trcb_a.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
        trcb_b.tc_deliver(event_a.clone());

        let event_b = trcb_b.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
        trcb_a.tc_deliver(event_b.clone());

        let event_a_1 = trcb_a.tc_bcast(Message::Op(Op::AddVertex("A")));
        trcb_b.tc_deliver(event_a_1.clone());

        let event_a_2 = trcb_a.tc_bcast(Message::Op(Op::AddVertex("B")));
        trcb_b.tc_deliver(event_a_2.clone());

        let id_c = Uuid::new_v4().to_string();
        let mut trcb_c = Trcb::<&str, u32, Op<&str>>::new(id_c.as_str());
        let event = trcb_c.tc_bcast(Message::ProtocolCmd(ProtocolCmd::Join));
        trcb_b.tc_deliver(event.clone());
        trcb_a.tc_deliver(event);

        trcb_c.tc_deliver(event_a);
        trcb_c.tc_deliver(event_b);

        trcb_c.tc_deliver(event_a_1);
        trcb_c.tc_deliver(event_a_2);

        let event = trcb_c.tc_bcast(Message::Op(Op::AddArc("A", "B")));
        trcb_c.tc_deliver(event.clone());
        trcb_b.tc_deliver(event);

        let event = trcb_a.tc_bcast(Message::Op(Op::RemoveVertex("A")));
        trcb_b.tc_deliver(event.clone());
        trcb_c.tc_deliver(event);

        let event = trcb_b.tc_bcast(Message::Op(Op::RemoveArc("A", "B")));
        trcb_a.tc_deliver(event.clone());
        trcb_c.tc_deliver(event);

        assert!(is_isomorphic(&trcb_a.eval(), &trcb_b.eval()));
        assert!(is_isomorphic(&trcb_a.eval(), &trcb_c.eval()));
    }
}
