use moirai_protocol::{
    crdt::{
        eval::Eval,
        pure_crdt::PureCRDT,
        query::{QueryOperation, Read},
    },
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
};
use petgraph::graph::DiGraph;
use std::hash::Hash;
use std::{cmp::Ordering, fmt::Debug};

use crate::{
    HashMap, HashSet,
    policy::{LwwPolicy, Policy},
};

pub trait Connectable<Target, Edge> {
    const MIN: usize;
    const MAX: usize;

    fn min(&self) -> usize {
        Self::MIN
    }

    fn max(&self) -> usize {
        Self::MAX
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct User(u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Server(u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkConnection;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Database(u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbConnection;

impl Connectable<Server, NetworkConnection> for User {
    const MIN: usize = 1;
    const MAX: usize = 1;
}

impl Connectable<Database, DbConnection> for User {
    const MIN: usize = 0;
    const MAX: usize = 1;
}

impl Connectable<Database, DbConnection> for Server {
    const MIN: usize = 0;
    const MAX: usize = 1;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arc<S, T, E>
where
    S: Connectable<T, E>,
{
    pub source: S,
    pub target: T,
    pub kind: E,
}

impl<S, T, E> Arc<S, T, E>
where
    S: Connectable<T, E>,
{
    pub fn min(&self) -> usize {
        S::MIN
    }

    pub fn max(&self) -> usize {
        S::MAX
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Vertex<S>
where
    S: Debug + Clone + PartialEq + Eq + Hash,
{
    AddVertex { id: S },
    RemoveVertex { id: S },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraph {
    AddVertex { id: MyTypedGraphVertex },
    RemoveVertex { id: MyTypedGraphVertex },
    AddArc(MyTypedGraphArcs),
    RemoveArc(MyTypedGraphArcs),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraphArcs {
    UserToServer(Arc<User, Server, NetworkConnection>),
    ServerToDb(Arc<Server, Database, DbConnection>),
}

impl MyTypedGraphArcs {
    pub fn source(&self) -> MyTypedGraphVertex {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => MyTypedGraphVertex::User(arc.source.clone()),
            MyTypedGraphArcs::ServerToDb(arc) => MyTypedGraphVertex::Server(arc.source.clone()),
        }
    }

    pub fn target(&self) -> MyTypedGraphVertex {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => MyTypedGraphVertex::Server(arc.target.clone()),
            MyTypedGraphArcs::ServerToDb(arc) => MyTypedGraphVertex::Database(arc.target.clone()),
        }
    }

    pub fn kind(&self) -> MyTypedGraphEdge {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => MyTypedGraphEdge::UserToServer(arc.kind.clone()),
            MyTypedGraphArcs::ServerToDb(arc) => MyTypedGraphEdge::ServerToDb(arc.kind.clone()),
        }
    }

    pub fn max(&self) -> usize {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => arc.max(),
            MyTypedGraphArcs::ServerToDb(arc) => arc.max(),
        }
    }

    pub fn min(&self) -> usize {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => arc.min(),
            MyTypedGraphArcs::ServerToDb(arc) => arc.min(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraphVertex {
    User(User),
    Server(Server),
    Database(Database),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraphEdge {
    UserToServer(NetworkConnection),
    ServerToDb(DbConnection),
}

impl PureCRDT for MyTypedGraph {
    type Value = DiGraph<MyTypedGraphVertex, MyTypedGraphEdge>;
    type StableState = Vec<Self>;

    const DISABLE_R_WHEN_R: bool = false;
    const DISABLE_R_WHEN_NOT_R: bool = false;
    const DISABLE_STABILIZE: bool = false;

    fn redundant_itself<'a>(
        new_tagged_op: &TaggedOp<Self>,
        stable: &Self::StableState,
        unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        match new_tagged_op.op() {
            MyTypedGraph::AddVertex { .. } => false,
            MyTypedGraph::RemoveVertex { .. } | MyTypedGraph::RemoveArc(_) => true,
            MyTypedGraph::AddArc(arc) => {
                let count_stable = stable
                    .iter()
                    .filter(|op| match op {
                        MyTypedGraph::AddArc(a) => {
                            a.source() == arc.source() && a.kind() == arc.kind()
                        }
                        _ => false,
                    })
                    .count();
                let unstable_ops: Vec<_> = unstable.collect();
                let count_unstable = unstable_ops
                    .iter()
                    .filter(|op| match op.op() {
                        MyTypedGraph::AddArc(a) => {
                            a.source() == arc.source() && a.kind() == arc.kind()
                        }
                        _ => false,
                    })
                    .count();

                if count_stable + count_unstable < arc.max() {
                    false
                } else {
                    // The new arc can't loose against a stable one
                    unstable_ops
                        .iter()
                        .any(|old_tagged_op| match old_tagged_op.op() {
                            MyTypedGraph::AddArc(a) => {
                                a.source() == arc.source()
                                    && a.kind() == arc.kind()
                                    && LwwPolicy::compare(new_tagged_op.tag(), old_tagged_op.tag())
                                        == std::cmp::Ordering::Less
                            }
                            _ => false,
                        })
                }
            }
        }
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
                (MyTypedGraph::AddArc(arc), MyTypedGraph::RemoveVertex { id: v3 }) => {
                    arc.source() == *v3 || arc.target() == *v3
                }
                (MyTypedGraph::AddArc(arc1), MyTypedGraph::AddArc(arc2))
                | (MyTypedGraph::AddArc(arc1), MyTypedGraph::RemoveArc(arc2)) => {
                    arc1.source() == arc2.source()
                        && arc1.target() == arc2.target()
                        && arc1.kind() == arc2.kind()
                }
                (MyTypedGraph::AddVertex { id: v1 }, MyTypedGraph::AddVertex { id: v2 })
                | (MyTypedGraph::AddVertex { id: v1 }, MyTypedGraph::RemoveVertex { id: v2 }) => {
                    v1 == v2
                }
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

    fn stabilize(
        _tagged_op: &TaggedOp<Self>,
        _stable: &mut Self::StableState,
        _unstable: &mut impl IsUnstableState<Self>,
    ) {
    }

    fn eval<Q>(
        q: Q,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<Q>,
    {
        Self::execute_query(q, stable, unstable)
    }

    fn is_enabled(
        op: &Self,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> bool {
        let graph = Self::execute_query(Read::new(), stable, unstable);
        match op {
            MyTypedGraph::AddVertex { .. } => true,
            MyTypedGraph::RemoveVertex { id } => graph.node_weights().any(|node| node == id),
            MyTypedGraph::RemoveArc(arc) => {
                let source = arc.source();
                let target = arc.target();
                let kind = arc.kind();

                let idx_1 = graph
                    .node_indices()
                    .find(|&idx| graph.node_weight(idx) == Some(&source));
                let idx_2 = graph
                    .node_indices()
                    .find(|&idx| graph.node_weight(idx) == Some(&target));
                if let (Some(i1), Some(i2)) = (idx_1, idx_2)
                    && !graph
                        .edges_connecting(i1, i2)
                        .any(|edge| edge.weight() == &kind)
                {
                    return false;
                }

                let count = graph
                    .edges_directed(
                        graph.node_indices().find(|&i| graph[i] == source).unwrap(),
                        petgraph::Direction::Outgoing,
                    )
                    .filter(|edge| edge.weight() == &kind)
                    .count();

                count > arc.min()
            }
            MyTypedGraph::AddArc(arc) => {
                let source = arc.source();
                let target = arc.target();
                let kind = arc.kind();

                if !graph.node_weights().any(|node| node == &source)
                    || !graph.node_weights().any(|node| node == &target)
                {
                    return false;
                }

                // `kind` must be unique per source-target pair, so we only need to check the count of existing edges with the same source and kind
                let count = graph
                    .edges_directed(
                        graph.node_indices().find(|&i| graph[i] == source).unwrap(),
                        petgraph::Direction::Outgoing,
                    )
                    .filter(|edge| edge.weight() == &kind)
                    .count();

                count < arc.max()
            }
        }
    }
}

impl Eval<Read<<Self as PureCRDT>::Value>> for MyTypedGraph {
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
            (
                MyTypedGraph::AddVertex { .. } | MyTypedGraph::RemoveVertex { .. },
                MyTypedGraph::AddArc(_) | MyTypedGraph::RemoveArc(_),
            ) => Ordering::Less,
            (
                MyTypedGraph::AddArc(_) | MyTypedGraph::RemoveArc(_),
                MyTypedGraph::AddVertex { .. } | MyTypedGraph::RemoveVertex { .. },
            ) => Ordering::Greater,
            _ => Ordering::Equal,
        });
        let mut graph = DiGraph::new();
        let mut node_index = HashMap::default();
        let mut edge_index: HashSet<(MyTypedGraphVertex, MyTypedGraphVertex, MyTypedGraphEdge)> =
            HashSet::default();
        for o in ops {
            match o {
                MyTypedGraph::AddVertex { id } => {
                    if node_index.contains_key(id) {
                        continue; // Skip if the vertex already exists
                    }
                    let idx = graph.add_node(id.clone());
                    node_index.insert(id.clone(), idx);
                }
                MyTypedGraph::AddArc(arcs) => {
                    let v1 = arcs.source();
                    let v2 = arcs.target();
                    let e = match arcs {
                        MyTypedGraphArcs::UserToServer(arc) => {
                            MyTypedGraphEdge::UserToServer(arc.kind.clone())
                        }
                        MyTypedGraphArcs::ServerToDb(arc) => {
                            MyTypedGraphEdge::ServerToDb(arc.kind.clone())
                        }
                    };
                    let tuple = (v1, v2, e);
                    if edge_index.contains(&tuple) {
                        continue; // Skip if the edge already exists
                    }
                    let (v1, v2, e) = tuple;
                    if let (Some(a), Some(b)) = (node_index.get(&v1), node_index.get(&v2)) {
                        graph.add_edge(*a, *b, e.clone());
                        edge_index.insert((v1, v2, e));
                    }
                }
                MyTypedGraph::RemoveVertex { .. } | MyTypedGraph::RemoveArc(_) => unreachable!(),
            }
        }
        graph
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::typed_graph::{Arc, MyTypedGraphArcs};
    use crate::utils::membership::twins;
    use moirai_protocol::replica::IsReplica;

    use super::*;

    #[test]
    fn my_application() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph>();

        let event_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();

        let event_a_2 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();

        let event_a_3 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToServer(Arc {
                source: User(1),
                target: Server(1),
                kind: NetworkConnection,
            })))
            .unwrap();

        let event_a_4 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(2)),
            })
            .unwrap();

        let event_a_5 = replica_a.send(MyTypedGraph::RemoveArc(MyTypedGraphArcs::UserToServer(
            Arc {
                source: User(1),
                target: Server(1),
                kind: NetworkConnection,
            },
        )));
        assert!(event_a_5.is_none());

        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);
        replica_b.receive(event_a_4);

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }

    #[test]
    fn arcs() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph>();

        let event_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();

        let event_a_2 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();

        let event_a_3 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(2)),
            })
            .unwrap();

        replica_b.receive(event_a_1);
        replica_b.receive(event_a_2);
        replica_b.receive(event_a_3);

        let event_a_4 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToServer(Arc {
                source: User(1),
                target: Server(1),
                kind: NetworkConnection,
            })))
            .unwrap();

        let event_b_1 = replica_b
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToServer(Arc {
                source: User(1),
                target: Server(2),
                kind: NetworkConnection,
            })))
            .unwrap();

        replica_a.receive(event_b_1);
        replica_b.receive(event_a_4);

        println!(
            "Graph A: {:?}",
            petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[])
        );
        println!(
            "Graph B: {:?}",
            petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[])
        );

        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some()
        );
    }
}
