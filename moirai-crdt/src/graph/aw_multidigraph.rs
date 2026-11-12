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
    cmp::Ordering,
    fmt::{Debug, Display},
    hash::Hash,
    marker::PhantomData,
};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
#[cfg(feature = "fuzz")]
use moirai_fuzz::generator::op_generator::OpGenerator;
#[cfg(feature = "fuzz")]
use moirai_fuzz::generator::value_generator::ValueGenerator;
use moirai_protocol::{
    data_type::{
        query::{QueryOperation, Read},
        replicated::{
            CausalCompaction, CausalReset, CausalStabilize, Eval, Precondition, RedundancyRelation,
            ReplicatedDataType,
        },
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableCore,
};
use petgraph::graph::DiGraph;
#[cfg(feature = "fuzz")]
use petgraph::visit::EdgeRef;
#[cfg(feature = "fuzz")]
use rand::seq::IteratorRandom;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::{FxHashMap, FxHashSet, graph::update::GraphUpdate};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AWMultidigraph<V, E>(PhantomData<(V, E)>);

#[derive(Clone, Debug)]
pub enum GraphRejection<V, E> {
    ArcDoesNotExist(V, V, E),
    VertexDoesNotExist(V),
}

impl<V, E> Display for GraphRejection<V, E>
where
    V: Debug,
    E: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ArcDoesNotExist(v1, v2, e) => write!(
                f,
                "Arc from {:?} to {:?} with edge {:?} does not exist",
                v1, v2, e
            ),
            Self::VertexDoesNotExist(v) => write!(f, "Vertex {:?} does not exist", v),
        }
    }
}

impl<V, E> ReplicatedDataType for AWMultidigraph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    // TODO: We can use a more efficient representation of the stable state
    // TODO: garbage collect dangling arcs
    type Update = GraphUpdate<V, E>;
    type Stable = Vec<Self::Update>;
    type Rejection = GraphRejection<V, E>;
}

impl<V, E, U> Precondition<U> for AWMultidigraph<V, E>
where
    V: Debug + Clone + Eq + Hash,
    E: Debug + Clone + Eq + Hash,
    U: IsUnstableCore<Self::Update>,
{
    fn is_enabled(
        stable: &Self::Stable,
        unstable: &U,
        op: &Self::Update,
    ) -> Result<(), Self::Rejection> {
        let state = Self::execute_query(stable, unstable, &Read::new());

        let vertex_exists = |v| state.node_weights().any(|node| node == v);
        let vertex_index = |v| {
            state
                .node_indices()
                .find(|&idx| state.node_weight(idx) == Some(v))
        };
        let arc_exists = |v1, v2, e| {
            vertex_index(v1)
                .zip(vertex_index(v2))
                .is_some_and(|(i1, i2)| {
                    state
                        .edges_connecting(i1, i2)
                        .any(|edge| edge.weight() == e)
                })
        };

        match op {
            GraphUpdate::AddVertex(_) => Ok(()),
            GraphUpdate::RemoveVertex(v) => {
                if vertex_exists(v) {
                    Ok(())
                } else {
                    Err(GraphRejection::VertexDoesNotExist(v.clone()))
                }
            }
            GraphUpdate::AddArc(v1, v2, _) => {
                if !vertex_exists(v1) {
                    Err(GraphRejection::VertexDoesNotExist(v1.clone()))
                } else if !vertex_exists(v2) {
                    Err(GraphRejection::VertexDoesNotExist(v2.clone()))
                } else {
                    Ok(())
                }
            }
            GraphUpdate::RemoveArc(v1, v2, e) => {
                if arc_exists(v1, v2, e) {
                    Ok(())
                } else {
                    Err(GraphRejection::ArcDoesNotExist(
                        v1.clone(),
                        v2.clone(),
                        e.clone(),
                    ))
                }
            }
        }
    }
}

impl<V, E, U> CausalStabilize<U> for AWMultidigraph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
}

impl<V, E, U> CausalReset<U> for AWMultidigraph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
}

impl<V, E, U> CausalCompaction<U> for AWMultidigraph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
    U: IsUnstableCore<Self::Update>,
{
    fn compact_stable(
        stable: &mut Self::Stable,
        rdnt: RedundancyRelation<Self::Update>,
        new: &TaggedOp<Self::Update>,
    ) {
        stable.retain(|old| !rdnt(old, None, false, new));
    }

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Self::Stable,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(
            new_tagged_op.op(),
            GraphUpdate::RemoveVertex(_) | GraphUpdate::RemoveArc(_, _, _)
        )
    }

    fn redundant_by_when_redundant(
        old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        // old_op = addVertex, addArc only
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (GraphUpdate::AddArc(v1, v2, _), GraphUpdate::RemoveVertex(v3)) => {
                    v1 == v3 || v2 == v3
                }
                (GraphUpdate::AddArc(v1, v2, e1), GraphUpdate::AddArc(v3, v4, e2))
                | (GraphUpdate::AddArc(v1, v2, e1), GraphUpdate::RemoveArc(v3, v4, e2)) => {
                    v1 == v3 && v2 == v4 && e1 == e2
                }
                (GraphUpdate::AddVertex(v1), GraphUpdate::AddVertex(v2))
                | (GraphUpdate::AddVertex(v1), GraphUpdate::RemoveVertex(v2)) => v1 == v2,
                _ => false,
            }
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self::Update,
        old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        <Self as CausalCompaction<U>>::redundant_by_when_redundant(
            old_op,
            old_tag,
            is_conc,
            new_tagged_op,
        )
    }
}

impl<V, E, U> Eval<U, Read<DiGraph<V, E>>> for AWMultidigraph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
    U: IsUnstableCore<<Self as ReplicatedDataType>::Update>,
{
    fn execute_query(
        stable: &Vec<<Self as ReplicatedDataType>::Update>,
        unstable: &U,
        _q: &Read<DiGraph<V, E>>,
    ) -> <Read<DiGraph<V, E>> as QueryOperation>::Response {
        let mut ops: Vec<&<Self as ReplicatedDataType>::Update> = stable
            .iter()
            .chain(unstable.iter().map(|t| t.op()))
            .collect();
        // TODO: Not needed if we are using a sorted unstable! e.g., VecLog
        ops.sort_by(|a, b| match (a, b) {
            (GraphUpdate::AddVertex(_), GraphUpdate::AddArc(_, _, _)) => Ordering::Less,
            (GraphUpdate::AddArc(_, _, _), GraphUpdate::AddVertex(_)) => Ordering::Greater,
            _ => Ordering::Equal,
        });
        let mut graph = DiGraph::new();
        let mut node_index = FxHashMap::default();
        let mut edge_index: FxHashSet<(&V, &V, &E)> = FxHashSet::default();
        for o in ops {
            match o {
                GraphUpdate::AddVertex(v) => {
                    if node_index.contains_key(v) {
                        continue; // Skip if the vertex already exists
                    }
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                GraphUpdate::AddArc(v1, v2, e) => {
                    if edge_index.contains(&(v1, v2, e)) {
                        continue; // Skip if the edge already exists
                    }
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        graph.add_edge(*a, *b, e.clone());
                        edge_index.insert((v1, v2, e));
                    }
                }
                GraphUpdate::RemoveVertex(_) | GraphUpdate::RemoveArc(_, _, _) => {
                    unreachable!()
                }
            }
        }
        graph
    }
}

#[cfg(feature = "fuzz")]
impl<V, E, U> OpGenerator<U> for AWMultidigraph<V, E>
where
    U: IsUnstableCore<Self::Update>,
    V: ValueGenerator + Debug + Clone + PartialEq + Eq + Hash,
    E: ValueGenerator + Debug + Clone + PartialEq + Eq + Hash,
{
    type Config = ();

    fn generate(
        rng: &mut impl rand::Rng,
        _config: &Self::Config,
        stable: &Self::Stable,
        unstable: &U,
    ) -> Self::Update {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            AddVertex,
            RemoveVertex,
            AddArc,
            RemoveArc,
        }

        let graph = Self::execute_query(stable, unstable, &Read::new());

        let choice = if graph.node_count() < 2 {
            &Choice::AddVertex
        } else if graph.edge_count() == 0 {
            let dist = WeightedIndex::new([2, 1, 3]).unwrap();
            &[Choice::AddVertex, Choice::RemoveVertex, Choice::AddArc][dist.sample(rng)]
        } else {
            let dist = WeightedIndex::new([2, 1, 2, 1]).unwrap();
            &[
                Choice::AddVertex,
                Choice::RemoveVertex,
                Choice::AddArc,
                Choice::RemoveArc,
            ][dist.sample(rng)]
        };

        match choice {
            Choice::AddVertex => {
                use moirai_fuzz::generator::value_generator::ValueGenerator;

                let v = V::generate(rng, &<V as ValueGenerator>::Config::default());
                GraphUpdate::AddVertex(v)
            }
            Choice::RemoveVertex => {
                let idx = graph.node_indices().choose(rng).unwrap();
                let v = graph.node_weight(idx).unwrap().clone();
                GraphUpdate::RemoveVertex(v)
            }
            Choice::AddArc => {
                let idx1 = graph.node_indices().choose(rng).unwrap();
                let idx2 = graph.node_indices().choose(rng).unwrap();
                let v1 = graph.node_weight(idx1).unwrap().clone();
                let v2 = graph.node_weight(idx2).unwrap().clone();
                let e = E::generate(rng, &<E as ValueGenerator>::Config::default());
                GraphUpdate::AddArc(v1, v2, e)
            }
            Choice::RemoveArc => {
                let edge = graph.edge_references().choose(rng).unwrap();
                let v1 = graph.node_weight(edge.source()).unwrap().clone();
                let v2 = graph.node_weight(edge.target()).unwrap().clone();
                let e = edge.weight().clone();
                GraphUpdate::RemoveArc(v1, v2, e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{data_type::query::Read, replica::IsReplica};

    use crate::{
        graph::{aw_multidigraph::AWMultidigraph, update::GraphUpdate},
        utils::bootstrap::{triplet, twins},
    };

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, &str>>();

        let event = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(GraphUpdate::AddVertex("B")).unwrap();
        replica_a.receive(event);

        let event = replica_a
            .send(GraphUpdate::AddArc("B", "A", "arc1"))
            .unwrap();
        replica_b.receive(event);

        let event = replica_b.send(GraphUpdate::RemoveVertex("B")).unwrap();
        replica_a.receive(event);

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
    fn concurrent_add_remove_vertex() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, &str>>();

        let event = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event);

        let event_b = replica_b.send(GraphUpdate::RemoveVertex("A")).unwrap();
        let event_a = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();

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
    }

    #[test]
    fn concurrent_graph_arc() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, &str>>();

        let event = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(GraphUpdate::AddVertex("B")).unwrap();
        replica_a.receive(event);

        let event_b = replica_b.send(GraphUpdate::RemoveVertex("B")).unwrap();
        let event_a = replica_a
            .send(GraphUpdate::AddArc("B", "A", "arc1"))
            .unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

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
    fn concurrent_graph_vertex() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, &str>>();

        let event_a = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        let event_b = replica_b.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 1);
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
    fn graph_multiple_vertex_same_id() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, u8>>();

        let event_a = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_a.receive(event_b);
        let event_a = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 1);
    }

    #[test]
    fn concurrent_revive_arc() {
        let (mut replica_a, mut replica_b, mut replica_c) = triplet::<AWMultidigraph<&str, u8>>();

        let event_a_1 = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        let event_a_2 = replica_a.send(GraphUpdate::AddVertex("B")).unwrap();

        replica_b.receive(event_a_1.clone());
        replica_b.receive(event_a_2.clone());
        replica_c.receive(event_a_1);
        replica_c.receive(event_a_2);

        let event_b = replica_b.send(GraphUpdate::RemoveVertex("B")).unwrap();
        replica_c.receive(event_b.clone());
        let event_c = replica_c.send(GraphUpdate::AddVertex("B")).unwrap();

        let event_a = replica_a.send(GraphUpdate::AddArc("A", "B", 1)).unwrap();

        replica_a.receive(event_b);
        replica_a.receive(event_c.clone());
        replica_b.receive(event_a.clone());
        replica_b.receive(event_c);
        replica_c.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(&Read::new()).edge_count(), 1);
        assert_eq!(replica_b.query(&Read::new()).node_count(), 2);
        assert_eq!(replica_b.query(&Read::new()).edge_count(), 1);
        assert_eq!(replica_c.query(&Read::new()).node_count(), 2);
        assert_eq!(replica_c.query(&Read::new()).edge_count(), 1);
    }

    #[test]
    fn revive_arc() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, u8>>();

        let event_a = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(GraphUpdate::AddVertex("B")).unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a.send(GraphUpdate::AddArc("A", "B", 1)).unwrap();
        let event_b = replica_b.send(GraphUpdate::RemoveVertex("B")).unwrap();
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

        let event_a = replica_a.send(GraphUpdate::AddVertex("B")).unwrap();
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
    fn multigraph() {
        let (mut replica_a, mut replica_b) = twins::<AWMultidigraph<&str, u8>>();

        let event_a = replica_a.send(GraphUpdate::AddVertex("A")).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(GraphUpdate::AddVertex("B")).unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a.send(GraphUpdate::AddArc("A", "B", 1)).unwrap();
        let event_b = replica_b.send(GraphUpdate::AddArc("A", "B", 2)).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(&Read::new()).edge_count(), 2);
        assert_eq!(replica_a.query(&Read::new()).node_count(), 2);
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
    fn fuzz_aw_graph() {
        use moirai_fuzz::{
            config::{ChurnConfig, FuzzerConfig, Predicate, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;
        use petgraph::graph::DiGraph;

        let run = RunConfig::new(ChurnConfig::new(0.4, 0.6), 4, 1_000, None, None);
        let runs = vec![run; 1];

        let config =
            FuzzerConfig::<VecLog<AWMultidigraph<String, u8>>, Read<DiGraph<String, u8>>>::new(
                "aw_graph",
                runs,
                // |a, b| vf2::isomorphisms(a, b).first().is_some(),
                Predicate::new(Read::new(), |a, b| {
                    a.node_count() == b.node_count() && a.edge_count() == b.edge_count()
                }),
            );

        fuzzer::<VecLog<AWMultidigraph<String, u8>>, Read<DiGraph<String, u8>>>(config);
    }
}
