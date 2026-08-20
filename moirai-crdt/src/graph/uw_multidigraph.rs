use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

#[cfg(feature = "fuzz")]
use moirai_fuzz::{op_generator::OpGeneratorNested, value_generator::ValueGenerator};
use moirai_protocol::{
    clock::version_vector::Version,
    crdt::{
        eval::EvalNested,
        query::{QueryOperation, Read},
    },
    event::Event,
    state::{effect_context::EffectContext, log::IsLog},
};
use petgraph::graph::DiGraph;
#[cfg(feature = "fuzz")]
use rand::{Rng, seq::IteratorRandom};

use crate::HashMap;

type LabeledMultidigraph<V, E, Vl, El> =
    DiGraph<Content<V, <Vl as IsLog>::Value>, Content<(V, V, E), <El as IsLog>::Value>>;

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
pub struct UWGraphLog<V, E, Vl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
    Vl: IsLog,
    El: IsLog,
{
    arcs: HashMap<(V, V, E), El>,
    vertices: HashMap<V, Vl>,
}

#[derive(Clone, Debug)]
pub enum LabelledGraphRejection<V, E, Vl, El>
where
    Vl: IsLog,
    El: IsLog,
    V: Debug,
    E: Debug,
{
    VertexNotFound(V),
    ArcNotFound(V, V, E),
    VertexDisabled(Vl::Rejection),
    ArcDisabled(El::Rejection),
}

impl<V, E, Vl, El> Display for LabelledGraphRejection<V, E, Vl, El>
where
    Vl: IsLog,
    El: IsLog,
    V: Debug,
    E: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LabelledGraphRejection::VertexNotFound(v) => write!(f, "Vertex not found: {:?}", v),
            LabelledGraphRejection::ArcNotFound(v1, v2, e) => {
                write!(f, "Arc not found: {:?} -> {:?} (id: {:?})", v1, v2, e)
            }
            LabelledGraphRejection::VertexDisabled(r) => {
                write!(f, "Vertex operation disabled in child log: {}", r)
            }
            LabelledGraphRejection::ArcDisabled(r) => {
                write!(f, "Arc operation disabled in child log: {}", r)
            }
        }
    }
}

impl<V, E, Vl, El> IsLog for UWGraphLog<V, E, Vl, El>
where
    Vl: IsLog,
    El: IsLog,
    V: Clone + Debug + Hash + Eq,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    type Command = UWGraph<V, E, Vl::Op, El::Op>;
    type Op = UWGraph<V, E, Vl::Op, El::Op>;
    type Value = LabeledMultidigraph<V, E, Vl, El>;
    type Rejection = LabelledGraphRejection<V, E, Vl, El>;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Op {
        cmd
    }

    // TODO: add sink when vertex/arc creation/destruction
    fn effect(&mut self, event: Event<Self::Op>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            // Update the child at vertex `v`
            UWGraph::UpdateVertex { id: v, child: op } => {
                let child_op = Event::unfold(event, op);
                self.vertices.entry(v).or_default().effect(child_op, ctx);
            }
            // Remove the vertex `v`, all its incident arcs, and reset its child
            UWGraph::RemoveVertex { id: v } => {
                if let Some(child) = self.vertices.get_mut(&v) {
                    child.redundant_by_parent(event.version(), true);
                }
                let arcs_to_remove: Vec<(V, V, E)> = self
                    .arcs
                    .keys()
                    .filter(|(v1, v2, _)| v1 == &v || v2 == &v)
                    .cloned()
                    .collect();
                for arc in arcs_to_remove {
                    if let Some(child) = self.arcs.get_mut(&arc) {
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
                self.arcs
                    .entry((v1, v2, e))
                    .or_default()
                    .effect(child_op, ctx);
            }
            // Remove the arc `(v1, v2, e)` and reset its child
            UWGraph::RemoveArc {
                source: v1,
                target: v2,
                id: e,
            } => {
                if let Some(child) = self.arcs.get_mut(&(v1, v2, e)) {
                    child.redundant_by_parent(event.version(), true);
                }
            }
        }
    }

    fn stabilize(&mut self, version: &Version) {
        for v in self.arcs.values_mut() {
            v.stabilize(version);
        }

        for v in self.vertices.values_mut() {
            v.stabilize(version);
        }
    }

    fn redundant_by_parent(&mut self, version: &Version, conservative: bool) {
        for v in self.arcs.values_mut() {
            v.redundant_by_parent(version, conservative);
        }

        for v in self.vertices.values_mut() {
            v.redundant_by_parent(version, conservative);
        }
    }

    fn is_default(&self) -> bool {
        self.arcs.is_empty() && self.vertices.is_empty()
    }

    fn is_enabled(&self, op: &Self::Op) -> Result<(), LabelledGraphRejection<V, E, Vl, El>> {
        match op {
            UWGraph::UpdateVertex { id, child } => {
                if let Some(log) = self.vertices.get(id) {
                    log.is_enabled(child)
                        .map_err(|e| LabelledGraphRejection::VertexDisabled(e))
                } else {
                    Vl::default()
                        .is_enabled(child)
                        .map_err(|e| LabelledGraphRejection::VertexDisabled(e))
                }
            }
            UWGraph::RemoveVertex { id: v } => {
                if let Some(child) = self.vertices.get(v)
                    && !child.is_default()
                {
                    Ok(())
                } else {
                    Err(LabelledGraphRejection::VertexNotFound(v.clone()))
                }
            }
            UWGraph::UpdateArc {
                source,
                target,
                id,
                child,
            } => {
                if let (Some(child1), Some(child2)) =
                    (self.vertices.get(source), self.vertices.get(target))
                {
                    if child1.is_default() || child2.is_default() {
                        return Err(LabelledGraphRejection::ArcNotFound(
                            source.clone(),
                            target.clone(),
                            id.clone(),
                        ));
                    }
                    El::default()
                        .is_enabled(child)
                        .map_err(|e| LabelledGraphRejection::ArcDisabled(e))
                } else {
                    Err(LabelledGraphRejection::ArcNotFound(
                        source.clone(),
                        target.clone(),
                        id.clone(),
                    ))
                }
            }
            UWGraph::RemoveArc { source, target, id } => {
                if let Some(child) = self.arcs.get(&(source.clone(), target.clone(), id.clone()))
                    && !child.is_default()
                {
                    Ok(())
                } else {
                    Err(LabelledGraphRejection::ArcNotFound(
                        source.clone(),
                        target.clone(),
                        id.clone(),
                    ))
                }
            }
        }
    }
}

impl<V, E, Vl, El> Default for UWGraphLog<V, E, Vl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
    Vl: IsLog,
    El: IsLog,
{
    fn default() -> Self {
        Self {
            arcs: HashMap::default(),
            vertices: HashMap::default(),
        }
    }
}

impl<V, E, Vl, El> EvalNested<Read<<Self as IsLog>::Value>> for UWGraphLog<V, E, Vl, El>
where
    Vl: IsLog + EvalNested<Read<<Vl as IsLog>::Value>>,
    El: IsLog + EvalNested<Read<<El as IsLog>::Value>>,
    V: Clone + Debug + Hash + Eq,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    fn execute_query(
        &self,
        _q: Read<Self::Value>,
    ) -> <Read<Self::Value> as QueryOperation>::Response {
        let mut graph = <Self as IsLog>::Value::new();
        let mut node_idx = HashMap::default();
        for (v, child) in self.vertices.iter() {
            if child.is_default() {
                continue;
            }
            let idx = graph.add_node(Content::new(v.clone(), child.execute_query(Read::new())));
            node_idx.insert(v.clone(), idx);
        }
        for ((v1, v2, e), child) in self.arcs.iter() {
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

#[cfg(feature = "fuzz")]
impl<V, E, Vl, El> OpGeneratorNested for UWGraphLog<V, E, Vl, El>
where
    V: ValueGenerator + Clone + Hash + Debug + Eq,
    E: ValueGenerator + Clone + Hash + Debug + Eq,
    Vl: OpGeneratorNested + EvalNested<Read<<Vl as IsLog>::Value>>,
    El: OpGeneratorNested + EvalNested<Read<<El as IsLog>::Value>>,
{
    fn generate(&self, rng: &mut impl Rng) -> Self::Op {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            UpdateVertex,
            RemoveVertex,
            UpdateArc,
            RemoveArc,
        }

        let graph = self.execute_query(Read::new());

        let choice = if graph.node_count() < 2 {
            &Choice::UpdateVertex
        } else if graph.edge_count() == 0 {
            let dist = WeightedIndex::new([2, 1, 3]).unwrap();
            &[
                Choice::UpdateVertex,
                Choice::RemoveVertex,
                Choice::UpdateArc,
            ][dist.sample(rng)]
        } else {
            let dist = WeightedIndex::new([2, 1, 2, 1]).unwrap();
            &[
                Choice::UpdateVertex,
                Choice::RemoveVertex,
                Choice::UpdateArc,
                Choice::RemoveArc,
            ][dist.sample(rng)]
        };

        match choice {
            Choice::UpdateVertex => {
                let v = V::generate(rng, &<V as ValueGenerator>::Config::default());
                let child_op = if let Some(child) = self.vertices.get(&v) {
                    child.generate(rng)
                } else {
                    Vl::new().generate(rng)
                };
                UWGraph::UpdateVertex {
                    id: v,
                    child: child_op,
                }
            }
            Choice::RemoveVertex => {
                let idx = graph.node_indices().choose(rng).unwrap();
                let v = graph.node_weight(idx).unwrap();
                UWGraph::RemoveVertex { id: v.id.clone() }
            }
            Choice::UpdateArc => {
                let idx1 = graph.node_indices().choose(rng).unwrap();
                let idx2 = graph.node_indices().choose(rng).unwrap();
                let v1 = graph.node_weight(idx1).unwrap();
                let v2 = graph.node_weight(idx2).unwrap();
                let edge = E::generate(rng, &<E as ValueGenerator>::Config::default());

                let child_op = if let Some(child) =
                    self.arcs.get(&(v1.id.clone(), v2.id.clone(), edge.clone()))
                {
                    child.generate(rng)
                } else {
                    El::new().generate(rng)
                };

                UWGraph::UpdateArc {
                    source: v1.id.clone(),
                    target: v2.id.clone(),
                    id: edge,
                    child: child_op,
                }
            }
            Choice::RemoveArc => {
                let edge = graph.edge_references().choose(rng).unwrap();
                let e = edge.weight();
                UWGraph::RemoveArc {
                    source: e.id.0.clone(),
                    target: e.id.1.clone(),
                    id: e.id.2.clone(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{
        crdt::{policy::LwwPolicy, query::Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };
    use petgraph::graph::DiGraph;

    #[cfg(feature = "fuzz")]
    use crate::{
        counter::resettable_counter::Counter,
        graph::uw_multidigraph::{UWGraph, UWGraphLog},
        register::unique_register::Register,
        utils::membership::{triplet_log, twins_log},
    };

    type Lww = VecLog<Register<i32, LwwPolicy>>;
    type Cntr = VecLog<Counter<i32>>;

    #[test]
    fn nested_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event = replica_a
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: Register::Write(1),
            })
            .unwrap();
        replica_b.receive(event);

        let event = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: Register::Write(2),
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
                child: Register::Write(5),
            })
            .unwrap();
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: Register::Write(10),
            })
            .unwrap();
        let event_b_2 = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: Register::Write(8),
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
                child: Register::Write(3),
            })
            .unwrap();
        replica_b.receive(event);

        let event = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: Register::Write(4),
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
                child: Register::Write(1),
            })
            .unwrap();
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "A",
                child: Register::Write(2),
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
                child: Register::Write(1),
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
                child: Register::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: Register::Write(2),
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
                child: Register::Write(3),
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
                child: Register::Write(4),
            })
            .unwrap();
        let event_a_2 = replica_a
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: Register::Write(3),
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
                child: Register::Write(1),
            })
            .unwrap();
        replica_c.receive(event_b_1.clone());
        let event_c_1 = replica_c
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: Register::Write(1),
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
                child: Register::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(UWGraph::UpdateVertex {
                id: "B",
                child: Register::Write(2),
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
                child: Register::Write(3),
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

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_uw_graph() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let runs = vec![RunConfig::new(0.4, 8, 1_000, None, None, false, false)];
        let config = FuzzerConfig::<UWGraphLog<usize, usize, Lww, Cntr>>::new(
            "uw_graph",
            runs,
            true,
            |a, b| a.node_count() == b.node_count() && a.edge_count() == b.edge_count(),
            false,
        );
        fuzzer::<UWGraphLog<usize, usize, Lww, Cntr>>(config);
    }
}
