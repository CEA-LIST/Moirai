use std::{cmp::Ordering, fmt::Debug, hash::Hash};

use petgraph::graph::DiGraph;

use crate::{
    protocol::{
        crdt::{
            eval::Eval,
            pure_crdt::PureCRDT,
            query::{QueryOperation, Read},
        },
        event::{tag::Tag, tagged_op::TaggedOp},
        state::unstable_state::IsUnstableState,
    },
    HashMap, HashSet,
};

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
                    // if edge_index.contains(&(v1, v2, e)) {
                    //     continue; // Skip if the edge already exists
                    // }
                    if let (Some(a), Some(b)) = (node_index.get(v1), node_index.get(v2)) {
                        graph.add_edge(*a, *b, e.clone());
                        edge_index.insert((v1, v2, e));
                    }
                }
                Graph::RemoveVertex(_) => unreachable!(),
                Graph::RemoveArc(_, _, _) => unreachable!(),
            }
        }
        graph
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        crdt::{graph::aw_multidigraph::Graph, test_util::twins},
        protocol::{crdt::query::Read, replica::IsReplica},
    };

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

    // TODO: Fuzzer test
}
