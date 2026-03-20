use std::{cmp::Ordering, fmt::Debug, hash::Hash};

#[cfg(feature = "fuzz")]
use moirai_fuzz::op_generator::OpGenerator;
#[cfg(feature = "fuzz")]
use moirai_fuzz::value_generator::ValueGenerator;
use moirai_protocol::{
    crdt::{
        eval::Eval,
        pure_crdt::PureCRDT,
        query::{QueryOperation, Read},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    replica::ReplicaIdx,
    state::unstable_state::IsUnstableState,
    utils::{intern_str::Interner, translate_ids::TranslateIds},
};
use petgraph::graph::DiGraph;
#[cfg(feature = "fuzz")]
use petgraph::visit::EdgeRef;
#[cfg(feature = "fuzz")]
use rand::seq::IteratorRandom;

use crate::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub enum Graph<V, E> {
    /// Add a vertex with identifier `V`. `V` must be unique.
    AddVertex(V),
    /// Remove a vertex with identifier `V`. All arcs connected to this vertex are also removed.
    RemoveVertex(V),
    /// Add an arc from vertex `V` to vertex `V'` with edge identifier `E`. Both vertices must already exist. The triple `(V, V', E)` must be unique.
    AddArc(V, V, E),
    /// Remove an arc from vertex `V` to vertex `V'` with edge identifier `E`. The triple `(V, V', E)` must already exist.
    RemoveArc(V, V, E),
}

impl<V, E> TranslateIds for Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    fn translate_ids(&self, _from: ReplicaIdx, _interner: &Interner) -> Self {
        self.clone()
    }
}

impl<V, E> PureCRDT for Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    type Value = DiGraph<V, E>;
    type StableState = Vec<Self>;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        matches!(
            new_tagged_op.op(),
            Graph::RemoveVertex(_) | Graph::RemoveArc(_, _, _)
        )
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        // old_op = addVertex, addArc only
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (Graph::AddArc(v1, v2, _), Graph::RemoveVertex(v3)) => v1 == v3 || v2 == v3,
                (Graph::AddArc(v1, v2, e1), Graph::AddArc(v3, v4, e2))
                | (Graph::AddArc(v1, v2, e1), Graph::RemoveArc(v3, v4, e2)) => {
                    v1 == v3 && v2 == v4 && e1 == e2
                }
                (Graph::AddVertex(v1), Graph::AddVertex(v2))
                | (Graph::AddVertex(v1), Graph::RemoveVertex(v2)) => v1 == v2,
                _ => false,
            }
    }

    fn redundant_by_when_not_redundant(
        old_op: &Self,
        old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        Self::redundant_by_when_redundant(old_op, old_tag, is_conc, new_tagged_op)
    }

    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> bool {
        let state = Self::execute_query(Read::new(), stable, unstable);
        match op {
            Graph::AddVertex(_) => true,
            // The vertex must exist to be removed.
            Graph::RemoveVertex(v) => state.node_weights().any(|node| node == v),
            // Both vertices must exist to add an arc.
            Graph::AddArc(v1, v2, _) => {
                state.node_weights().any(|node| node == v1)
                    && state.node_weights().any(|node| node == v2)
            }
            Graph::RemoveArc(v1, v2, e) => {
                let idx_1 = state
                    .node_indices()
                    .find(|&idx| state.node_weight(idx) == Some(v1));
                let idx_2 = state
                    .node_indices()
                    .find(|&idx| state.node_weight(idx) == Some(v2));
                if let (Some(i1), Some(i2)) = (idx_1, idx_2) {
                    state
                        .edges_connecting(i1, i2)
                        .any(|edge| edge.weight() == e)
                } else {
                    false
                }
            }
        }
    }
}

impl<V, E> Eval<Read<<Self as PureCRDT>::Value>> for Graph<V, E>
where
    V: Debug + Clone + PartialEq + Eq + Hash,
    E: Debug + Clone + PartialEq + Eq + Hash,
{
    fn execute_query(
        _q: Read<<Self as PureCRDT>::Value>,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> <Read<<Self as PureCRDT>::Value> as QueryOperation>::Response {
        let mut ops: Vec<&Self> = stable
            .iter()
            .chain(unstable.iter().map(|t| t.op()))
            .collect();
        // TODO: Not needed if we are using a sorted unstable! e.g., VecLog
        ops.sort_by(|a, b| match (a, b) {
            (Graph::AddVertex(_), Graph::AddArc(_, _, _)) => Ordering::Less,
            (Graph::AddArc(_, _, _), Graph::AddVertex(_)) => Ordering::Greater,
            _ => Ordering::Equal,
        });
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::default();
        let mut edge_index: HashSet<(&V, &V, &E)> = HashSet::default();
        for o in ops {
            match o {
                Graph::AddVertex(v) => {
                    if node_index.contains_key(v) {
                        continue; // Skip if the vertex already exists
                    }
                    let idx = graph.add_node(v.clone());
                    node_index.insert(v, idx);
                }
                Graph::AddArc(v1, v2, e) => {
                    if edge_index.contains(&(v1, v2, e)) {
                        continue; // Skip if the edge already exists
                    }
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        graph.add_edge(*a, *b, e.clone());
                        edge_index.insert((v1, v2, e));
                    }
                }
                Graph::RemoveVertex(_) | Graph::RemoveArc(_, _, _) => unreachable!(),
            }
        }
        graph
    }
}

#[cfg(feature = "fuzz")]
impl<V, E> OpGenerator for Graph<V, E>
where
    V: ValueGenerator + Debug + Clone + PartialEq + Eq + Hash,
    E: ValueGenerator + Debug + Clone + PartialEq + Eq + Hash,
{
    type Config = ();

    fn generate(
        rng: &mut impl rand::Rng,
        _config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        use rand::distr::{Distribution, weighted::WeightedIndex};

        enum Choice {
            AddVertex,
            RemoveVertex,
            AddArc,
            RemoveArc,
        }

        let graph = Self::execute_query(Read::new(), stable, unstable);

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
                use moirai_fuzz::value_generator::ValueGenerator;

                let v = V::generate(rng, &<V as ValueGenerator>::Config::default());
                Graph::AddVertex(v)
            }
            Choice::RemoveVertex => {
                let idx = graph.node_indices().choose(rng).unwrap();
                let v = graph.node_weight(idx).unwrap().clone();
                Graph::RemoveVertex(v)
            }
            Choice::AddArc => {
                let idx1 = graph.node_indices().choose(rng).unwrap();
                let idx2 = graph.node_indices().choose(rng).unwrap();
                let v1 = graph.node_weight(idx1).unwrap().clone();
                let v2 = graph.node_weight(idx2).unwrap().clone();
                let e = E::generate(rng, &<E as ValueGenerator>::Config::default());
                Graph::AddArc(v1, v2, e)
            }
            Choice::RemoveArc => {
                let edge = graph.edge_references().choose(rng).unwrap();
                let v1 = graph.node_weight(edge.source()).unwrap().clone();
                let v2 = graph.node_weight(edge.target()).unwrap().clone();
                let e = edge.weight().clone();
                Graph::RemoveArc(v1, v2, e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use moirai_protocol::{crdt::query::Read, replica::IsReplica};

    use crate::{graph::aw_multidigraph::Graph, utils::membership::twins};

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(Graph::AddVertex("B")).unwrap();
        replica_a.receive(event);

        let event = replica_a.send(Graph::AddArc("B", "A", "arc1")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(Graph::RemoveVertex("B")).unwrap();
        replica_a.receive(event);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[test]
    fn concurrent_add_remove_vertex() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event);

        let event_b = replica_b.send(Graph::RemoveVertex("A")).unwrap();
        let event_a = replica_a.send(Graph::AddVertex("A")).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
    }

    #[test]
    fn concurrent_graph_arc() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event);

        let event = replica_b.send(Graph::AddVertex("B")).unwrap();
        replica_a.receive(event);

        let event_b = replica_b.send(Graph::RemoveVertex("B")).unwrap();
        let event_a = replica_a.send(Graph::AddArc("B", "A", "arc1")).unwrap();
        replica_b.receive(event_a);
        replica_a.receive(event_b);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[test]
    fn concurrent_graph_vertex() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, &str>>();

        let event_a = replica_a.send(Graph::AddVertex("A")).unwrap();
        let event_b = replica_b.send(Graph::AddVertex("A")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[test]
    fn graph_multiple_vertex_same_id() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event_a = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(Graph::AddVertex("A")).unwrap();
        replica_a.receive(event_b);
        let event_a = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
    }

    #[test]
    fn revive_arc() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event_a = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(Graph::AddVertex("B")).unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a.send(Graph::AddArc("A", "B", 1)).unwrap();
        let event_b = replica_b.send(Graph::RemoveVertex("B")).unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 0);

        let event_a = replica_a.send(Graph::AddVertex("B")).unwrap();
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
    fn multigraph() {
        let (mut replica_a, mut replica_b) = twins::<Graph<&str, u8>>();

        let event_a = replica_a.send(Graph::AddVertex("A")).unwrap();
        replica_b.receive(event_a);
        let event_b = replica_b.send(Graph::AddVertex("B")).unwrap();
        replica_a.receive(event_b);

        let event_a = replica_a.send(Graph::AddArc("A", "B", 1)).unwrap();
        let event_b = replica_b.send(Graph::AddArc("A", "B", 2)).unwrap();

        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).edge_count(), 2);
        assert_eq!(replica_a.query(Read::new()).node_count(), 2);
        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_aw_graph() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run = RunConfig::new(0.4, 8, 100, None, None, false, false);
        let runs = vec![run.clone(); 1];

        let config = FuzzerConfig::<VecLog<Graph<String, u8>>>::new(
            "aw_graph",
            runs,
            true,
            // |a, b| vf2::isomorphisms(a, b).first().is_some(),
            |a, b| a.node_count() == b.node_count() && a.edge_count() == b.edge_count(),
            false,
        );

        fuzzer::<VecLog<Graph<String, u8>>>(config);
    }
}
