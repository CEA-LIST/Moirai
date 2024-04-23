use petgraph::graph::DiGraph;

use crate::clocks::vector_clock::VectorClock;
use crate::protocol::event::{Message, OpEvent};
use crate::protocol::pure_crdt::PureCRDT;
use crate::protocol::tcsb::POLog;
use crate::protocol::utils::{Incrementable, Keyable};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

/// Add-wins policy
#[derive(Clone, Debug)]
pub enum Op<V> {
    AddVertex(V),
    RemoveVertex(V),
    AddArc(V, V),
    RemoveArc(V, V),
}

impl<V> PureCRDT for Op<V>
where
    V: Debug + Clone + Hash + Eq,
{
    type Value = DiGraph<V, ()>;

    fn r<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        event: &OpEvent<K, C, Self>,
        state: &POLog<K, C, Self>,
    ) -> bool {
        match &event.op {
            Op::AddVertex(_) => false,
            Op::RemoveVertex(_) => true,
            Op::AddArc(v1, v2) => {
                let mut found_v1 = false;
                let mut found_v2 = false;

                for op in state.0.iter() {
                    if found_v1 && found_v2 {
                        break;
                    }
                    if let Op::AddVertex(v) = op {
                        if v == v1 {
                            found_v1 = true;
                        }
                        if v == v2 {
                            found_v2 = true;
                        }
                    }
                }
                for message in state.1.values() {
                    if found_v1 && found_v2 {
                        break;
                    }

                    if let Message::Op(Op::AddVertex(v)) = message {
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
            Op::RemoveArc(_, _) => true,
        }
    }

    fn r_zero<K, C>(old_event: &OpEvent<K, C, Self>, new_event: &OpEvent<K, C, Self>) -> bool
    where
        K: Keyable + Clone + Debug,
        C: Incrementable<C> + Clone + Debug,
    {
        match (&old_event.op, &new_event.op) {
            (Op::AddVertex(v1), Op::AddVertex(v2)) => {
                matches!(
                    old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
                    None | Some(Ordering::Less)
                ) && v1 == v2
            }
            (Op::AddVertex(v1), Op::RemoveVertex(v2)) => {
                old_event.metadata.vc < new_event.metadata.vc && v1 == v2
            }
            (Op::AddVertex(_), Op::AddArc(_, _)) => false,
            (Op::AddVertex(v1), Op::RemoveArc(v2, v3)) => {
                old_event.metadata.vc < new_event.metadata.vc && (v1 == v2 || v1 == v3)
            }
            (Op::AddArc(_, _), Op::AddVertex(_)) => false,
            (Op::AddArc(v1, v2), Op::RemoveVertex(v3)) => {
                matches!(
                    old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
                    None | Some(Ordering::Less)
                ) && v3 == v1
                    || v3 == v2
            }
            (Op::AddArc(v1, v2), Op::AddArc(v3, v4)) => {
                matches!(
                    old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
                    None | Some(Ordering::Less)
                ) && v1 == v3
                    && v2 == v4
            }
            (Op::AddArc(v1, v2), Op::RemoveArc(v3, v4)) => {
                old_event.metadata.vc < new_event.metadata.vc && v1 == v3 && v2 == v4
            }
            // (Op::RemoveArc(v1, v2), Op::AddVertex(v3)) => v3 == v1 || v3 == v2,
            // (Op::RemoveArc(v1, v2), Op::RemoveVertex(v3)) => false,
            // (Op::RemoveArc(v1, v2), Op::AddArc(v3, v4)) => v1 == v2 && v3 == v4,
            // (Op::RemoveArc(v1, v2), Op::RemoveArc(v3, v4)) => v1 == v2 && v3 == v4,
            // (Op::RemoveVertex(v1), Op::AddVertex(v2)) => v1 == v2,
            // (Op::RemoveVertex(v1), Op::RemoveVertex(v2)) => {
            //     matches!(
            //         old_event.metadata.vc.partial_cmp(&new_event.metadata.vc),
            //         None | Some(Ordering::Less)
            //     ) && v1 == v2
            // }
            // (Op::RemoveVertex(v1), Op::AddArc(v2, v3)) => {
            //     v1 == v2 || v1 == v3
            // }
            // (Op::RemoveVertex(v1), Op::RemoveArc(v2, v3)) => false,
            _ => false,
        }
    }

    fn r_one<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        old_event: &OpEvent<K, C, Self>,
        new_event: &OpEvent<K, C, Self>,
    ) -> bool {
        Self::r_zero(old_event, new_event)
    }

    fn stabilize<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        _: &VectorClock<K, C>,
        _state: &mut POLog<K, C, Self>,
    ) {
    }

    fn eval<K: Keyable + Clone + Debug, C: Incrementable<C> + Clone + Debug>(
        state: &POLog<K, C, Self>,
    ) -> Self::Value {
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();
        let mut edge_index = HashMap::new();
        for op in &state.0 {
            match op {
                Op::AddVertex(v) => {
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Op::AddArc(v1, v2) => {
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
        for message in state.1.values() {
            match &message {
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
                _ => (),
            }
        }
        graph
    }
}

#[cfg(test)]
mod tests {
    use petgraph::algo::is_isomorphic;

    use crate::{
        crdt::graph::Op,
        protocol::{event::Message, tcsb::Tcsb},
    };

    #[test_log::test]
    fn simple_graph() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::AddVertex("A")));
        tscb_b.tc_deliver(event);

        let event = tscb_b.tc_bcast(Message::Op(Op::AddVertex("B")));
        tscb_a.tc_deliver(event);

        let event = tscb_a.tc_bcast(Message::Op(Op::AddArc("B", "A")));
        tscb_b.tc_deliver(event);

        let event = tscb_b.tc_bcast(Message::Op(Op::RemoveVertex("B")));
        tscb_a.tc_deliver(event);

        assert!(is_isomorphic(&tscb_a.eval(), &tscb_b.eval()));
    }

    #[test_log::test]
    fn concurrent_graph() {
        let mut tscb_a = Tcsb::<&str, u64, Op<&str>>::new("a");
        let mut tscb_b = Tcsb::<&str, u64, Op<&str>>::new("b");

        let event = tscb_a.tc_bcast(Message::Op(Op::AddVertex("A")));
        tscb_b.tc_deliver(event);

        let event = tscb_b.tc_bcast(Message::Op(Op::AddVertex("B")));
        tscb_a.tc_deliver(event);

        let event_b = tscb_b.tc_bcast(Message::Op(Op::RemoveVertex("B")));
        let event_a = tscb_a.tc_bcast(Message::Op(Op::AddArc("B", "A")));
        tscb_b.tc_deliver(event_a);
        tscb_a.tc_deliver(event_b);

        assert!(is_isomorphic(&tscb_a.eval(), &tscb_b.eval()));
    }
}
