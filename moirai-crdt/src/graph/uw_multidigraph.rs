/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//  
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
// 
//  SPDX-License-Identifier: Apache-2.0
//  
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

use std::{
    fmt::{Debug, Display},
    hash::Hash,
};

#[cfg(feature = "fuzz")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::{
    generator::{command_generator::CommandGenerator, value_generator::ValueGenerator},
    observers::footprint::{FootprintNode, LogFootprint},
};
use moirai_protocol::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    data_type::query::{QueryOperation, Read},
    event::Event,
    state::{
        effect_context::EffectContext,
        log::{EvalNested, IsLog},
    },
};
use petgraph::graph::DiGraph;
#[cfg(feature = "fuzz")]
use rand::{Rng, seq::IteratorRandom};

use crate::{FxHashMap, graph::update::LabelledMultidigraphUpdate};

type LabeledMultidigraph<V, E, Vl, El> = DiGraph<Content<V, Vl>, Content<(V, V, E), El>>;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct UWGraphLog<V, E, Vl, El>
where
    V: Clone + Debug + Eq + PartialEq + Hash,
    E: Clone + Debug + Eq + PartialEq + Hash,
    Vl: IsLog,
    El: IsLog,
{
    vertices: FxHashMap<V, Vl>,
    arcs: FxHashMap<(V, V, E), El>,
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
    Vl::Rejection: Display,
    El: IsLog,
    El::Rejection: Display,
    V: Debug,
    E: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::VertexNotFound(v) => write!(f, "Vertex not found: {:?}", v),
            Self::ArcNotFound(v1, v2, e) => {
                write!(f, "Arc not found: {:?} -> {:?} (id: {:?})", v1, v2, e)
            }
            Self::VertexDisabled(r) => {
                write!(f, "Vertex operation disabled in child log: {}", r)
            }
            Self::ArcDisabled(r) => {
                write!(f, "Arc operation disabled in child log: {}", r)
            }
        }
    }
}

impl<V, E, Vl, El> IsLog for UWGraphLog<V, E, Vl, El>
where
    Vl: IsLog + Debug,
    El: IsLog + Debug,
    V: Clone + Debug + Hash + Eq,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    type Command = LabelledMultidigraphUpdate<V, E, Vl::Update, El::Update>;
    type Update = LabelledMultidigraphUpdate<V, E, Vl::Update, El::Update>;
    type Rejection = LabelledGraphRejection<V, E, Vl, El>;

    fn new() -> Self {
        Self::default()
    }

    fn prepare(&self, cmd: Self::Command) -> Self::Update {
        cmd
    }

    // TODO: add sink when vertex/arc creation/destruction
    fn effect(&mut self, event: Event<Self::Update>, ctx: &mut EffectContext<'_>) {
        match event.op().clone() {
            // Update the child at vertex `v`
            LabelledMultidigraphUpdate::UpdateVertex { id: v, child: op } => {
                let child_op = Event::unfold(event, op);
                self.vertices.entry(v).or_default().effect(child_op, ctx);
            }
            // Remove the vertex `v`, all its incident arcs, and reset its child
            LabelledMultidigraphUpdate::RemoveVertex { id: v } => {
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
            LabelledMultidigraphUpdate::UpdateArc {
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
            LabelledMultidigraphUpdate::RemoveArc {
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

    fn stabilize(&mut self, frontier: &CausalFrontier) {
        for v in self.arcs.values_mut() {
            v.stabilize(frontier);
        }

        for v in self.vertices.values_mut() {
            v.stabilize(frontier);
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

    fn is_enabled(&self, op: &Self::Update) -> Result<(), LabelledGraphRejection<V, E, Vl, El>> {
        match op {
            LabelledMultidigraphUpdate::UpdateVertex { id, child } => {
                if let Some(log) = self.vertices.get(id) {
                    log.is_enabled(child)
                        .map_err(|e| LabelledGraphRejection::VertexDisabled(e))
                } else {
                    Vl::default()
                        .is_enabled(child)
                        .map_err(|e| LabelledGraphRejection::VertexDisabled(e))
                }
            }
            LabelledMultidigraphUpdate::RemoveVertex { id: v } => {
                if let Some(child) = self.vertices.get(v)
                    && !child.is_default()
                {
                    Ok(())
                } else {
                    Err(LabelledGraphRejection::VertexNotFound(v.clone()))
                }
            }
            LabelledMultidigraphUpdate::UpdateArc {
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
            LabelledMultidigraphUpdate::RemoveArc { source, target, id } => {
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
            arcs: FxHashMap::default(),
            vertices: FxHashMap::default(),
        }
    }
}

impl<V, E, Vl, VLabel, El, ELabel> EvalNested<Read<LabeledMultidigraph<V, E, VLabel, ELabel>>>
    for UWGraphLog<V, E, Vl, El>
where
    Vl: IsLog + Debug + EvalNested<Read<VLabel>>,
    El: IsLog + Debug + EvalNested<Read<ELabel>>,
    V: Clone + Debug + Hash + Eq,
    E: Clone + Debug + Eq + PartialEq + Hash,
{
    fn execute_query(
        &self,
        _q: &Read<LabeledMultidigraph<V, E, VLabel, ELabel>>,
    ) -> <Read<LabeledMultidigraph<V, E, VLabel, ELabel>> as QueryOperation>::Response {
        let mut graph = LabeledMultidigraph::new();
        let mut node_idx = FxHashMap::default();
        for (v, child) in self.vertices.iter() {
            if child.is_default() {
                continue;
            }
            let idx = graph.add_node(Content::new(v.clone(), child.execute_query(&Read::new())));
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
                            child.execute_query(&Read::new()),
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
    pub const fn new(id: Id, val: Val) -> Self {
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
impl<V, E, Vl, El> CommandGenerator for UWGraphLog<V, E, Vl, El>
where
    V: ValueGenerator + Clone + Hash + Debug + Eq,
    E: ValueGenerator + Clone + Hash + Debug + Eq,
    Vl: CommandGenerator + IsLog<Command = <Vl as IsLog>::Update> + Debug,
    El: CommandGenerator + IsLog<Command = <El as IsLog>::Update> + Debug,
{
    fn generate_command(&self, rng: &mut impl Rng) -> Self::Command {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            UpdateVertex,
            RemoveVertex,
            UpdateArc,
            RemoveArc,
        }

        let vertices: Vec<&V> = self
            .vertices
            .iter()
            .filter_map(|(id, child)| (!child.is_default()).then_some(id))
            .collect();
        let arcs: Vec<&(V, V, E)> = self
            .arcs
            .iter()
            .filter_map(|(id, child)| {
                (!child.is_default() && vertices.contains(&&id.0) && vertices.contains(&&id.1))
                    .then_some(id)
            })
            .collect();

        let choice = if vertices.len() < 2 {
            &Choice::UpdateVertex
        } else if arcs.is_empty() {
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
                    child.generate_command(rng)
                } else {
                    Vl::new().generate_command(rng)
                };
                LabelledMultidigraphUpdate::UpdateVertex {
                    id: v,
                    child: child_op,
                }
            }
            Choice::RemoveVertex => {
                let v = vertices.iter().choose(rng).unwrap();
                LabelledMultidigraphUpdate::RemoveVertex { id: (*v).clone() }
            }
            Choice::UpdateArc => {
                let v1 = vertices.iter().choose(rng).unwrap();
                let v2 = vertices.iter().choose(rng).unwrap();
                let edge = E::generate(rng, &<E as ValueGenerator>::Config::default());

                let child_op = if let Some(child) =
                    self.arcs
                        .get(&((**v1).clone(), (**v2).clone(), edge.clone()))
                {
                    child.generate_command(rng)
                } else {
                    El::new().generate_command(rng)
                };

                LabelledMultidigraphUpdate::UpdateArc {
                    source: (**v1).clone(),
                    target: (**v2).clone(),
                    id: edge,
                    child: child_op,
                }
            }
            Choice::RemoveArc => {
                let (source, target, id) = arcs.iter().choose(rng).unwrap();
                LabelledMultidigraphUpdate::RemoveArc {
                    source: source.clone(),
                    target: target.clone(),
                    id: id.clone(),
                }
            }
        }
    }
}

#[cfg(feature = "fuzz")]
impl<V, E, Vl, El> LogFootprint for UWGraphLog<V, E, Vl, El>
where
    V: Clone + Hash + Debug + Eq + DeepSizeOf,
    E: Clone + Hash + Debug + Eq + DeepSizeOf,
    Vl: IsLog + LogFootprint + DeepSizeOf,
    El: IsLog + LogFootprint + DeepSizeOf,
{
    fn footprint(&self) -> FootprintNode {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "fuzz")]
    use moirai_fuzz::config::Predicate;
    use moirai_protocol::{
        data_type::{policy::LwwPolicy, query::Read},
        replica::IsReplica,
        state::po_log::VecLog,
    };
    use petgraph::graph::DiGraph;

    #[cfg(feature = "fuzz")]
    use crate::graph::uw_multidigraph::LabeledMultidigraph;
    use crate::{
        counter::{Counter, update::CounterUpdate},
        graph::{update::LabelledMultidigraphUpdate, uw_multidigraph::UWGraphLog},
        register::{unique_register::Register, update::RegisterUpdate},
        utils::bootstrap::{triplet_log, twins_log},
    };

    type Lww = VecLog<Register<i32, LwwPolicy>>;
    type Cntr = VecLog<Counter<i32>>;

    #[test]
    fn nested_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        replica_b.receive(event);

        let event = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(2),
            })
            .unwrap();
        replica_a.receive(event);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: CounterUpdate::Inc(2),
            })
            .unwrap();
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: CounterUpdate::Inc(5),
            })
            .unwrap();
        let event_b_2 = replica_b
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 2,
                child: CounterUpdate::Dec(9),
            })
            .unwrap();

        replica_b.receive(event_a);
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(5),
            })
            .unwrap();
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(10),
            })
            .unwrap();
        let event_b_2 = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(8),
            })
            .unwrap();

        replica_b.receive(event_a);

        let event_b_3 = replica_b
            .send(LabelledMultidigraphUpdate::RemoveArc {
                source: "A",
                target: "B",
                id: 1,
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_a.receive(event_b_2);
        replica_a.receive(event_b_3);

        let event = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(3),
            })
            .unwrap();
        replica_b.receive(event);

        let event = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(4),
            })
            .unwrap();
        replica_a.receive(event);

        let event = replica_a
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "B",
                target: "A",
                id: 1,
                child: CounterUpdate::Inc(3),
            })
            .unwrap();
        replica_b.receive(event);

        assert!(
            vf2::isomorphisms(
                &replica_a.query(&Read::new()),
                &replica_b.query(&Read::new())
            )
            .first()
            .is_some()
        );
    }

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(2),
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        let mut graph: DiGraph<i32, i32> = DiGraph::new();
        graph.add_node(2);

        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(&Read::new()),
            &graph
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(&Read::new()),
            &replica_b.query(&Read::new())
        ));
    }

    #[test]
    fn remove_vertex() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::RemoveVertex { id: "A" })
            .unwrap();
        replica_a.receive(event_b);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 0);
        assert_eq!(replica_b.query(&Read::new()).node_count(), 0);
    }

    #[test]
    fn revive_arc() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(2),
            })
            .unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: CounterUpdate::Inc(2),
            })
            .unwrap();
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::RemoveVertex { id: "B" })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(
            vf2::isomorphisms(
                &replica_a.query(&Read::new()),
                &replica_b.query(&Read::new())
            )
            .first()
            .is_some()
        );

        assert_eq!(replica_a.query(&Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(&Read::new()).edge_count(), 0);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(3),
            })
            .unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(&Read::new()).edge_count(), 1);

        assert!(
            vf2::isomorphisms(
                &replica_a.query(&Read::new()),
                &replica_b.query(&Read::new())
            )
            .first()
            .is_some()
        );
    }

    #[test]
    fn revive_arc_2() {
        let (mut replica_a, mut replica_b, mut replica_c) =
            triplet_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a_1 = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(4),
            })
            .unwrap();
        let event_a_2 = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(3),
            })
            .unwrap();
        let event_a_3 = replica_a
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: CounterUpdate::Inc(51),
            })
            .unwrap();

        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());
        replica_b.receive(event_a_3.clone());
        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);
        replica_c.receive(event_a_3);

        let event_b_1 = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        replica_c.receive(event_b_1.clone());
        let event_c_1 = replica_c
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        let event_c_2 = replica_c
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: CounterUpdate::Inc(2),
            })
            .unwrap();
        replica_b.receive(event_c_1.clone());
        replica_b.receive(event_c_2.clone());

        assert!(petgraph::algo::is_isomorphic(
            &replica_b.query(&Read::new()),
            &replica_c.query(&Read::new())
        ));

        let event_a_1 = replica_a
            .send(LabelledMultidigraphUpdate::RemoveArc {
                source: "A",
                target: "B",
                id: 1,
            })
            .unwrap();
        let event_a_2 = replica_a
            .send(LabelledMultidigraphUpdate::RemoveVertex { id: "B" })
            .unwrap();
        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());

        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);

        replica_a.receive(event_b_1);
        replica_a.receive(event_c_1);
        replica_a.receive(event_c_2);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(&Read::new()).edge_count(), 1);

        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(&Read::new()),
            &replica_b.query(&Read::new())
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_a.query(&Read::new()),
            &replica_c.query(&Read::new())
        ));
        assert!(petgraph::algo::is_isomorphic(
            &replica_b.query(&Read::new()),
            &replica_c.query(&Read::new())
        ));
    }

    #[test]
    fn revive_arc_3() {
        let (mut replica_a, mut replica_b) = twins_log::<UWGraphLog<&str, u8, Lww, Cntr>>();

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "A",
                child: RegisterUpdate::Write(1),
            })
            .unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(2),
            })
            .unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "A",
                target: "B",
                id: 1,
                child: CounterUpdate::Inc(7),
            })
            .unwrap();
        replica_b.receive(event_a);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateArc {
                source: "B",
                target: "A",
                id: 1,
                child: CounterUpdate::Inc(8),
            })
            .unwrap();
        let event_b = replica_b
            .send(LabelledMultidigraphUpdate::RemoveVertex { id: "B" })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(
            vf2::isomorphisms(
                &replica_a.query(&Read::new()),
                &replica_b.query(&Read::new())
            )
            .first()
            .is_some()
        );

        assert_eq!(replica_a.query(&Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(&Read::new()).edge_count(), 0);

        let event_a = replica_a
            .send(LabelledMultidigraphUpdate::UpdateVertex {
                id: "B",
                child: RegisterUpdate::Write(3),
            })
            .unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(&Read::new()).edge_count(), 1);

        assert!(
            vf2::isomorphisms(
                &replica_a.query(&Read::new()),
                &replica_b.query(&Read::new())
            )
            .first()
            .is_some()
        );
    }

    #[cfg(feature = "fuzz")]
    #[test]
    #[ignore]
    fn fuzz_uw_graph() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };

        let runs = vec![RunConfig::new(
            ChurnConfig::new(0.4, 0.6),
            8,
            1_000,
            None,
            None,
        )];
        let config = FuzzerConfig::<
            UWGraphLog<usize, usize, Lww, Cntr>,
            Read<LabeledMultidigraph<usize, usize, Option<i32>, i32>>,
        >::new(
            "uw_graph",
            runs,
            Predicate::new(Read::new(), |a, b| {
                a.node_count() == b.node_count() && a.edge_count() == b.edge_count()
            }),
        );
        fuzzer::<
            UWGraphLog<usize, usize, Lww, Cntr>,
            Read<LabeledMultidigraph<usize, usize, Option<i32>, i32>>,
        >(config);
    }
}
