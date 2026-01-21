use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

use petgraph::graph::DiGraph;

use crate::{
    protocol::{
        clock::version_vector::Version,
        crdt::{
            eval::EvalNested,
            query::{QueryOperation, Read},
        },
        event::Event,
        state::log::IsLog,
    },
    HashMap,
};

#[derive(Clone, Debug)]
pub enum UWGraph<V, E, No, Lo> {
    UpdateVertex {
        id: V,
        child: No,
    },
    RemoveVertex {
        id: V,
    },
    UpdateArc {
        source: V,
        target: V,
        id: E,
        child: Lo,
    },
    RemoveArc {
        source: V,
        target: V,
        id: E,
    },
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
            // Update the child at vertex `v`
            UWGraph::UpdateVertex { id: v, child: op } => {
                let child_op = Event::unfold(event, op);
                self.vertex_content.entry(v).or_default().effect(child_op);
            }
            // Remove the vertex `v`, all its incident arcs, and reset its child
            UWGraph::RemoveVertex { id: v } => {
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
            // Update the child at arc `(v1, v2, e)`
            UWGraph::UpdateArc {
                source: v1,
                target: v2,
                id: e,
                child: op,
            } => {
                let child_op = Event::unfold(event, op);
                self.arc_content
                    .entry((v1, v2, e))
                    .or_default()
                    .effect(child_op);
            }
            // Remove the arc `(v1, v2, e)` and reset its child
            UWGraph::RemoveArc {
                source: v1,
                target: v2,
                id: e,
            } => {
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

    fn is_default(&self) -> bool {
        self.arc_content.is_empty() && self.vertex_content.is_empty()
    }

    fn is_enabled(&self, op: &Self::Op) -> bool {
        match op {
            UWGraph::UpdateVertex { .. } => true,
            UWGraph::RemoveVertex { id: v } => {
                if let Some(child) = self.vertex_content.get(v) {
                    !child.is_default()
                } else {
                    false
                }
            }
            UWGraph::UpdateArc {
                source: v1,
                target: v2,
                ..
            } => {
                if let (Some(child1), Some(child2)) =
                    (self.vertex_content.get(v1), self.vertex_content.get(v2))
                {
                    if child1.is_default() || child2.is_default() {
                        return false;
                    }
                    true
                } else {
                    false
                }
            }
            UWGraph::RemoveArc {
                source: v1,
                target: v2,
                id: e,
            } => {
                if let Some(child) = self.arc_content.get(&(v1.clone(), v2.clone(), e.clone())) {
                    !child.is_default()
                } else {
                    false
                }
            }
        }
    }
}

impl<V, E, Nl, El> Default for UWGraphLog<V, E, Nl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    fn default() -> Self {
        Self {
            arc_content: HashMap::default(),
            vertex_content: HashMap::default(),
        }
    }
}

impl<V, E, Nl, El> EvalNested<Read<<Self as IsLog>::Value>> for UWGraphLog<V, E, Nl, El>
where
    Nl: IsLog + EvalNested<Read<<Nl as IsLog>::Value>>,
    El: IsLog + EvalNested<Read<<El as IsLog>::Value>>,
    V: Clone + Debug + Ord + PartialOrd + Hash + Eq + Default + Display,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    fn execute_query(
        &self,
        _q: Read<Self::Value>,
    ) -> <Read<Self::Value> as QueryOperation>::Response {
        let mut graph = Self::Value::new();
        let mut node_idx = HashMap::default();
        for (v, child) in self.vertex_content.iter() {
            if child.is_default() {
                continue;
            }
            let idx = graph.add_node(Content::new(v.clone(), child.execute_query(Read::new())));
            node_idx.insert(v.clone(), idx);
        }
        for ((v1, v2, e), child) in self.arc_content.iter() {
            if child.is_default() {
                continue;
            }
            let idx1 = node_idx.get(v1);
            let idx2 = node_idx.get(v2);
            match (idx1, idx2) {
                (Some(i1), Some(i2)) => {
                    graph.add_edge(
                        *i1,
                        *i2,
                        Content::new(
                            (v1.clone(), v2.clone(), e.clone()),
                            child.execute_query(Read::new()),
                        ),
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
        protocol::{crdt::query::Read, replica::IsReplica, state::po_log::VecLog},
    };

    type Lww = VecLog<LWWRegister<i32>>;
    type Cntr = VecLog<Counter<i32>>;

    #[test]
    fn nested_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        replica_b.receive(event);

        let event = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(2),
            })
            .unwrap();
        replica_a.receive(event);

        let event_a = replica_a
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: Counter::Inc(2),
            })
            .unwrap();
        let event_b = replica_b
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: Counter::Inc(5),
            })
            .unwrap();
        let event_b_2 = replica_b
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 2,
                child: Counter::Dec(9),
            })
            .unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(5),
            })
            .unwrap();
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(10),
            })
            .unwrap();
        let event_b_2 = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(8),
            })
            .unwrap();

        replica_b.receive(event_a);

        let event_b_3 = replica_b
            .send(UWGraph::RemoveArc {
                source: "A",
                target: "B",
                id: 1,
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_a.receive(event_b_3);

        let event = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(3),
            })
            .unwrap();
        replica_b.receive(event);

        let event = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(4),
            })
            .unwrap();
        replica_a.receive(event);

        let event = replica_a
            .send(UWGraph::UpdateArc {
                source: "B",
                target: "A",
                id: 1,
                child: Counter::Inc(3),
            })
            .unwrap();
        replica_b.receive(event);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(2),
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut graph: DiGraph<i32, i32> = DiGraph::new();
        graph.add_node(2);

        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(Read::new()),
            &graph
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(Read::new()),
            &replica_b.query(Read::new())
        ));
    }

    #[test]
    fn remove_vertex() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(UWGraph::RemoveVertex { id: "A" }).unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(Read::new()).node_count(), 0);
        assert_eq!(replica_b.query(Read::new()).node_count(), 0);
    }

    #[test]
    fn revive_arc() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(2),
            })
            .unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: Counter::Inc(2),
            })
            .unwrap();
        let event_b = replica_b.send(UWGraph::RemoveVertex { id: "B" }).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 0);

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(3),
            })
            .unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 1);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[test]
    fn revive_arc_2() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a_1 = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(4),
            })
            .unwrap();
        let event_a_2 = replica_a
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(3),
            })
            .unwrap();
        let event_a_3 = replica_a
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: Counter::Inc(51),
            })
            .unwrap();

        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());
        replica_b.receive(event_a_3.clone());
        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);
        replica_c.receive(event_a_3);

        let event_b_1 = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        replica_c.receive(event_b_1.clone());
        let event_c_1 = replica_c
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        let event_c_2 = replica_c
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: Counter::Inc(2),
            })
            .unwrap();
        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_c_2.clone());

        assert!(petgraph::algo::is_isomorphic(
            &replica_b.query(Read::new()),
            &replica_c.query(Read::new())
        ));

        let event_a_1 = replica_a
            .send(UWGraph::RemoveArc {
                source: "A",
                target: "B",
                id: 1,
            })
            .unwrap();
        let event_a_2 = replica_a.send(UWGraph::RemoveVertex { id: "B" }).unwrap();
        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());

        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);

        replica_a.receive(event_b_1);
        replica_a.receive(event_c_1);
        replica_a.receive(event_c_2);

        assert_eq!(replica_a.query(Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 1);

        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(Read::new()),
            &replica_b.query(Read::new())
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(Read::new()),
            &replica_c.query(Read::new())
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_b.query(Read::new()),
            &replica_c.query(Read::new())
        ));
    }

    #[test]
    fn revive_arc_3() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: LWWRegister::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(2),
            })
            .unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a
            .send(UWGraph::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: Counter::Inc(7),
            })
            .unwrap();
        replica_b.receive(event_a);

        let event_a = replica_a
            .send(UWGraph::UpdateArc {
                source: "B",
                target: "A",
                id: 1,
                child: Counter::Inc(8),
            })
            .unwrap();
        let event_b = replica_b.send(UWGraph::RemoveVertex { id: "B" }).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 0);

        let event_a = replica_a
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: LWWRegister::Write(3),
            })
            .unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 1);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    // #[cfg(feature = "fuzz")]
    // #[test]
    // fn fuzz_uw_multidigraph() {
    //     use crate::{
    //         // crdt::test_util::init_tracing,
    //         fuzz::{
    //             config::{FuzzerConfig, RunConfig},
    //             fuzzer,
    //         },
    //     };

    //     // init_tracing();

    //     let ops: OpConfig<UWGraph<&str, u8, LWWRegister<&str>, Counter<i32>>> =
    //         OpConfig::Uniform(&[
    //             UWGraph::UpdateVertex("a", LWWRegister::Write("vertex_a")),
    //             UWGraph::UpdateVertex("b", LWWRegister::Write("vertex_b")),
    //             UWGraph::UpdateVertex("c", LWWRegister::Write("vertex_c")),
    //             UWGraph::UpdateVertex("d", LWWRegister::Write("vertex_d")),
    //             UWGraph::UpdateVertex("e", LWWRegister::Write("vertex_e")),
    //             UWGraph::RemoveVertex("a"),
    //             UWGraph::RemoveVertex("b"),
    //             UWGraph::RemoveVertex("c"),
    //             UWGraph::RemoveVertex("d"),
    //             UWGraph::RemoveVertex("e"),
    //             UWGraph::UpdateArc("a", "b", 1, Counter::Inc(1)),
    //             UWGraph::UpdateArc("a", "a", 1, Counter::Inc(13)),
    //             UWGraph::UpdateArc("a", "a", 1, Counter::Dec(3)),
    //             UWGraph::UpdateArc("a", "b", 2, Counter::Dec(2)),
    //             UWGraph::UpdateArc("a", "b", 2, Counter::Inc(7)),
    //             UWGraph::UpdateArc("b", "c", 1, Counter::Dec(5)),
    //             UWGraph::UpdateArc("c", "d", 1, Counter::Inc(3)),
    //             UWGraph::UpdateArc("d", "e", 1, Counter::Dec(2)),
    //             UWGraph::UpdateArc("e", "a", 1, Counter::Inc(4)),
    //             UWGraph::RemoveArc("a", "b", 1),
    //             UWGraph::RemoveArc("a", "b", 2),
    //             UWGraph::RemoveArc("b", "c", 1),
    //             UWGraph::RemoveArc("c", "d", 1),
    //             UWGraph::RemoveArc("d", "e", 1),
    //             UWGraph::RemoveArc("e", "a", 1),
    //         ]);

    //     let run = RunConfig::new(0.4, 8, 100_000, None, None);
    //     let runs = vec![run.clone(); 1];

    //     let config = FuzzerConfig::<
    //         UWGraphLog<&str, u8, VecLog<LWWRegister<&str>>, VecLog<Counter<i32>>>,
    //     >::new(
    //         "uw_multidigraph",
    //         runs,
    //         ops,
    //         true,
    //         |a, b| vf2::isomorphisms(a, b).first().is_some(),
    //         None,
    //     );

    //     fuzzer::<UWGraphLog<&str, u8, VecLog<LWWRegister<&str>>, VecLog<Counter<i32>>>>(config);
    // }
}
