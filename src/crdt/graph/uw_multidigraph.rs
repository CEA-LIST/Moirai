use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
};

use petgraph::graph::DiGraph;

use crate::protocol::{clock::version_vector::Version, event::Event, state::log::IsLog};

#[derive(Clone, Debug)]
pub enum UWGraph<V, E, No, Lo> {
    UpdateVertex(V, No),
    RemoveVertex(V),
    UpdateArc(V, V, E, Lo),
    RemoveArc(V, V, E),
}

#[derive(Clone, Debug)]
pub struct UWGraphLog<V, E, Nl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    arc_content: HashMap<(V, V, E), El>,
    vertex_content: HashMap<V, Nl>,
}

impl<V, E, Nl, El> IsLog for UWGraphLog<V, E, Nl, El>
where
    Nl: IsLog,
    El: IsLog,
    V: Clone + Debug + Ord + PartialOrd + Hash + Eq + Default + Display,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    type Op = UWGraph<V, E, Nl::Op, El::Op>;
    type Value = DiGraph<Content<V, Nl::Value>, Content<(V, V, E), El::Value>>;

    fn new() -> Self {
        Self::default()
    }

    fn effect(&mut self, event: Event<Self::Op>) {
        match event.op().clone() {
            UWGraph::UpdateVertex(v, op) => {
                let child_op = Event::unfold(event, op);
                self.vertex_content.entry(v).or_default().effect(child_op);
            }
            UWGraph::RemoveVertex(v) => {
                if let Some(child) = self.vertex_content.get_mut(&v) {
                    child.redundant_by_parent(event.version(), true);
                }
                let arcs_to_remove: Vec<(V, V, E)> = self
                    .arc_content
                    .keys()
                    .filter(|(v1, v2, _)| v1 == &v || v2 == &v)
                    .cloned()
                    .collect();
                for arc in arcs_to_remove {
                    if let Some(child) = self.arc_content.get_mut(&arc) {
                        child.redundant_by_parent(event.version(), true);
                    }
                }
            }
            UWGraph::UpdateArc(v1, v2, e, op) => {
                let child_op = Event::unfold(event, op);
                self.arc_content
                    .entry((v1, v2, e))
                    .or_default()
                    .effect(child_op);
            }
            UWGraph::RemoveArc(v1, v2, e) => {
                if let Some(child) = self.arc_content.get_mut(&(v1, v2, e)) {
                    child.redundant_by_parent(event.version(), true);
                }
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        for v in self.arc_content.values_mut() {
            v.stabilize(version);
        }

        for v in self.vertex_content.values_mut() {
            v.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for v in self.arc_content.values_mut() {
            v.redundant_by_parent(version, conservative);
        }

        for v in self.vertex_content.values_mut() {
            v.redundant_by_parent(version, conservative);
        }
    }

    fn len(&self) -> usize {
        self.vertex_content.values().map(|v| v.len()).sum::<usize>()
            + self.arc_content.values().map(|e| e.len()).sum::<usize>()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn eval(&self) -> Self::Value {
        let mut graph = Self::Value::new();
        let mut node_idx = HashMap::new();
        for (v, child) in self.vertex_content.iter() {
            // TODO: skip empty nodes
            if child.is_empty() {
                continue;
            }
            let idx = graph.add_node(Content::new(v.clone(), child.eval()));
            node_idx.insert(v.clone(), idx);
        }
        for ((v1, v2, e), child) in self.arc_content.iter() {
            if child.is_empty() {
                continue;
            }
            let idx1 = node_idx.get(v1);
            let idx2 = node_idx.get(v2);
            match (idx1, idx2) {
                (Some(i1), Some(i2)) => {
                    graph.add_edge(
                        *i1,
                        *i2,
                        Content::new((v1.clone(), v2.clone(), e.clone()), child.eval()),
                    );
                }
                _ => {
                    continue;
                }
            }
        }
        graph
    }
}

impl<V, E, Nl, El> Default for UWGraphLog<V, E, Nl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    fn default() -> Self {
        Self {
            arc_content: HashMap::new(),
            vertex_content: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Content<Id, Val> {
    pub id: Id,
    pub val: Val,
}

impl<Id, Val> Content<Id, Val> {
    pub fn new(id: Id, val: Val) -> Self {
        Self { id, val }
    }
}

impl<Id, Val> Display for Content<Id, Val>
where
    Val: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.val)
    }
}

#[cfg(test)]
mod tests {
    use petgraph::graph::DiGraph;

    use crate::{
        crdt::{
            counter::resettable_counter::Counter,
            graph::uw_multidigraph::{UWGraph, UWGraphLog},
            register::lww_register::LWWRegister,
            test_util::{triplet_log, twins_log},
        },
        protocol::{replica::IsReplica, state::po_log::VecLog},
    };

    type Lww = VecLog<LWWRegister<i32>>;
    type Cntr = VecLog<Counter<i32>>;

    #[test]
    fn nested_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        replica_b.receive(event);

        let event = replica_b.send(UWGraph::UpdateVertex("B", LWWRegister::Write(2)));
        replica_a.receive(event);

        let event_a = replica_a.send(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        let event_b = replica_b.send(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(5)));
        let event_b_2 = replica_b.send(UWGraph::UpdateArc("A", "B", 2, Counter::Dec(9)));

        replica_b.receive(event_a);
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);

        let event_a = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(5)));
        let event_b = replica_b.send(UWGraph::UpdateVertex("A", LWWRegister::Write(10)));
        let event_b_2 = replica_b.send(UWGraph::UpdateVertex("A", LWWRegister::Write(8)));

        replica_b.receive(event_a);

        let event_b_3 = replica_b.send(UWGraph::RemoveArc("A", "B", 1));
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_a.receive(event_b_3);

        let event = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(3)));
        replica_b.receive(event);

        let event = replica_b.send(UWGraph::UpdateVertex("B", LWWRegister::Write(4)));
        replica_a.receive(event);

        let event = replica_a.send(UWGraph::UpdateArc("B", "A", 1, Counter::Inc(3)));
        replica_b.receive(event);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());
    }

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        let event_b = replica_b.send(UWGraph::UpdateVertex("A", LWWRegister::Write(2)));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut graph: DiGraph<i32, i32> = DiGraph::new();
        graph.add_node(2);

        assert!(petgraph::algo::is_isomorphic(&replica_a.query(), &graph));
        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(),
            &replica_b.query()
        ));
    }

    #[test]
    fn remove_vertex() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        replica_b.receive(event_a);
        let event_b = replica_b.send(UWGraph::RemoveVertex("A"));
        replica_a.receive(event_b);

        assert_eq!(replica_a.query().node_count(), 0);
        assert_eq!(replica_b.query().node_count(), 0);
    }

    #[test]
    fn revive_arc() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        replica_b.receive(event_a);
        let event_b = replica_b.send(UWGraph::UpdateVertex("B", LWWRegister::Write(2)));
        replica_a.receive(event_b);

        let event_a = replica_a.send(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        let event_b = replica_b.send(UWGraph::RemoveVertex("B"));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());

        assert_eq!(replica_a.query().node_count(), 1);
        assert_eq!(replica_a.query().edge_count(), 0);

        let event_a = replica_a.send(UWGraph::UpdateVertex("B", LWWRegister::Write(3)));
        replica_b.receive(event_a);

        assert_eq!(replica_a.query().node_count(), 2);
        assert_eq!(replica_a.query().edge_count(), 1);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());
    }

    #[test]
    fn revive_arc_2() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_b_1 = replica_b.send(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        replica_c.receive(event_b_1.clone());
        let event_c_1 = replica_c.send(UWGraph::UpdateVertex("B", LWWRegister::Write(1)));
        let event_c_2 = replica_c.send(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(2)));
        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_c_2.clone());

        assert!(petgraph::algo::is_isomorphic(
            &replica_b.query(),
            &replica_c.query()
        ));

        let event_a_1 = replica_a.send(UWGraph::RemoveVertex("B"));
        let event_a_2 = replica_a.send(UWGraph::RemoveArc("A", "B", 1));
        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());

        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);

        replica_a.receive(event_b_1);
        replica_a.receive(event_c_1);
        replica_a.receive(event_c_2);

        assert_eq!(replica_a.query().node_count(), 2);
        assert_eq!(replica_a.query().edge_count(), 1);

        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(),
            &replica_b.query()
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(),
            &replica_c.query()
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_b.query(),
            &replica_c.query()
        ));
    }

    #[test]
    fn revive_arc_3() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a.send(UWGraph::UpdateVertex("A", LWWRegister::Write(1)));
        replica_b.receive(event_a);
        let event_b = replica_b.send(UWGraph::UpdateVertex("B", LWWRegister::Write(2)));
        replica_a.receive(event_b);

        let event_a = replica_a.send(UWGraph::UpdateArc("A", "B", 1, Counter::Inc(7)));
        replica_b.receive(event_a);

        let event_a = replica_a.send(UWGraph::UpdateArc("B", "A", 1, Counter::Inc(8)));
        let event_b = replica_b.send(UWGraph::RemoveVertex("B"));
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());

        assert_eq!(replica_a.query().node_count(), 1);
        assert_eq!(replica_a.query().edge_count(), 0);

        let event_a = replica_a.send(UWGraph::UpdateVertex("B", LWWRegister::Write(3)));
        replica_b.receive(event_a);

        assert_eq!(replica_a.query().node_count(), 2);
        assert_eq!(replica_a.query().edge_count(), 1);

        assert!(vf2::isomorphisms(&replica_a.query(), &replica_b.query())
            .first()
            .is_some());
    }

    // #[cfg(feature = "op_weaver")]
    // #[test]
    // fn op_weaver_uw_multidigraph() {
    //     use crate::{
    //         crdt::graph::uw_multidigraph::Content,
    //         utils::op_weaver::{op_weaver, EventGraphConfig},
    //     };

    //     let ops = vec![
    //         UWGraph::UpdateVertex("a", LWWRegister::Write("vertex_a")),
    //         UWGraph::UpdateVertex("b", LWWRegister::Write("vertex_b")),
    //         UWGraph::UpdateVertex("c", LWWRegister::Write("vertex_c")),
    //         UWGraph::UpdateVertex("d", LWWRegister::Write("vertex_d")),
    //         UWGraph::UpdateVertex("e", LWWRegister::Write("vertex_e")),
    //         UWGraph::RemoveVertex("a"),
    //         UWGraph::RemoveVertex("b"),
    //         UWGraph::RemoveVertex("c"),
    //         UWGraph::RemoveVertex("d"),
    //         UWGraph::RemoveVertex("e"),
    //         UWGraph::UpdateArc("a", "b", 1, Counter::Inc(1)),
    //         UWGraph::UpdateArc("a", "a", 1, Counter::Inc(13)),
    //         UWGraph::UpdateArc("a", "a", 1, Counter::Dec(3)),
    //         UWGraph::UpdateArc("a", "b", 2, Counter::Dec(2)),
    //         UWGraph::UpdateArc("a", "b", 2, Counter::Inc(7)),
    //         UWGraph::UpdateArc("b", "c", 1, Counter::Dec(5)),
    //         UWGraph::UpdateArc("c", "d", 1, Counter::Inc(3)),
    //         UWGraph::UpdateArc("d", "e", 1, Counter::Dec(2)),
    //         UWGraph::UpdateArc("e", "a", 1, Counter::Inc(4)),
    //         UWGraph::RemoveArc("a", "b", 1),
    //         UWGraph::RemoveArc("a", "b", 2),
    //         UWGraph::RemoveArc("b", "c", 1),
    //         UWGraph::RemoveArc("c", "d", 1),
    //         UWGraph::RemoveArc("d", "e", 1),
    //         UWGraph::RemoveArc("e", "a", 1),
    //     ];

    //     type GraphValue<'a> =
    //         DiGraph<Content<&'a str, &'a str>, Content<(&'a str, &'a str, u8), i32>>;

    //     let config = EventGraphConfig {
    //         name: "multidigraph",
    //         num_replicas: 8,
    //         num_operations: 10_000,
    //         operations: &ops,
    //         final_sync: true,
    //         churn_rate: 0.3,
    //         reachability: None,
    //         compare: |a: &GraphValue, b: &GraphValue| vf2::isomorphisms(a, b).first().is_some(),
    //         record_results: true,
    //         seed: None,
    //         witness_graph: false,
    //         concurrency_score: false,
    //     };

    //     op_weaver::<UWGraphLog<&str, u8, EventGraph<LWWRegister<&str>>, EventGraph<Counter<i32>>>>(
    //         config,
    //     );
    // }
}
