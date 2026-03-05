use moirai_fuzz::{
    op_generator::OpGenerator,
    value_generator::{NumberConfig, ValueGenerator},
};
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
use petgraph::visit::EdgeRef;
use rand::Rng;
use std::{cmp::Ordering, fmt::Debug};
use std::{hash::Hash, marker::PhantomData};

use crate::{HashMap, HashSet, policy::Policy};

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

impl ValueGenerator for User {
    type Config = ();

    fn generate(rng: &mut impl rand::RngCore, _config: &Self::Config) -> Self {
        User(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Server(u32);

impl ValueGenerator for Server {
    type Config = ();

    fn generate(rng: &mut impl rand::RngCore, _config: &Self::Config) -> Self {
        Server(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NetworkConnection;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Database(u32);

impl ValueGenerator for Database {
    type Config = ();

    fn generate(rng: &mut impl rand::RngCore, _config: &Self::Config) -> Self {
        Database(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoadBalancer(u32);

impl ValueGenerator for LoadBalancer {
    type Config = ();

    fn generate(rng: &mut impl rand::RngCore, _config: &Self::Config) -> Self {
        LoadBalancer(<u32 as ValueGenerator>::generate(
            rng,
            &NumberConfig::new(0, 20).unwrap(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DbConnection;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProxyConnection;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserToDbConnection;

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

impl Connectable<Server, ProxyConnection> for LoadBalancer {
    const MIN: usize = 0;
    const MAX: usize = 3;
}

impl Connectable<Database, UserToDbConnection> for User {
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
pub enum MyTypedGraph<P> {
    AddVertex { id: MyTypedGraphVertex },
    RemoveVertex { id: MyTypedGraphVertex },
    AddArc(MyTypedGraphArcs),
    RemoveArc(MyTypedGraphArcs),
    __Marker(std::convert::Infallible, PhantomData<P>),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraphArcs {
    UserToServer(Arc<User, Server, NetworkConnection>),
    ServerToDb(Arc<Server, Database, DbConnection>),
    LoadBalancerToServer(Arc<LoadBalancer, Server, ProxyConnection>),
    UserToDb(Arc<User, Database, UserToDbConnection>),
}

impl MyTypedGraphArcs {
    pub fn source(&self) -> MyTypedGraphVertex {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => MyTypedGraphVertex::User(arc.source.clone()),
            MyTypedGraphArcs::UserToDb(arc) => MyTypedGraphVertex::User(arc.source.clone()),
            MyTypedGraphArcs::ServerToDb(arc) => MyTypedGraphVertex::Server(arc.source.clone()),
            MyTypedGraphArcs::LoadBalancerToServer(arc) => {
                MyTypedGraphVertex::LoadBalancer(arc.source.clone())
            }
        }
    }

    pub fn target(&self) -> MyTypedGraphVertex {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => MyTypedGraphVertex::Server(arc.target.clone()),
            MyTypedGraphArcs::UserToDb(arc) => MyTypedGraphVertex::Database(arc.target.clone()),
            MyTypedGraphArcs::ServerToDb(arc) => MyTypedGraphVertex::Database(arc.target.clone()),
            MyTypedGraphArcs::LoadBalancerToServer(arc) => {
                MyTypedGraphVertex::Server(arc.target.clone())
            }
        }
    }

    pub fn kind(&self) -> MyTypedGraphEdge {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => MyTypedGraphEdge::UserToServer(arc.kind.clone()),
            MyTypedGraphArcs::UserToDb(arc) => MyTypedGraphEdge::UserToDb(arc.kind.clone()),
            MyTypedGraphArcs::ServerToDb(arc) => MyTypedGraphEdge::ServerToDb(arc.kind.clone()),
            MyTypedGraphArcs::LoadBalancerToServer(arc) => {
                MyTypedGraphEdge::LoadBalancerToServer(arc.kind.clone())
            }
        }
    }

    pub fn max(&self) -> usize {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => arc.max(),
            MyTypedGraphArcs::UserToDb(arc) => arc.max(),
            MyTypedGraphArcs::ServerToDb(arc) => arc.max(),
            MyTypedGraphArcs::LoadBalancerToServer(arc) => arc.max(),
        }
    }

    pub fn min(&self) -> usize {
        match self {
            MyTypedGraphArcs::UserToServer(arc) => arc.min(),
            MyTypedGraphArcs::UserToDb(arc) => arc.min(),
            MyTypedGraphArcs::ServerToDb(arc) => arc.min(),
            MyTypedGraphArcs::LoadBalancerToServer(arc) => arc.min(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraphVertex {
    User(User),
    Server(Server),
    Database(Database),
    LoadBalancer(LoadBalancer),
}

impl ValueGenerator for MyTypedGraphVertex {
    type Config = ();

    fn generate(rng: &mut impl rand::RngCore, _config: &Self::Config) -> Self {
        let choice = rng.random_range(0..4);
        match choice {
            0 => MyTypedGraphVertex::User(User::generate(rng, &())),
            1 => MyTypedGraphVertex::Server(Server::generate(rng, &())),
            2 => MyTypedGraphVertex::Database(Database::generate(rng, &())),
            _ => MyTypedGraphVertex::LoadBalancer(LoadBalancer::generate(rng, &())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MyTypedGraphEdge {
    UserToServer(NetworkConnection),
    UserToDb(UserToDbConnection),
    ServerToDb(DbConnection),
    LoadBalancerToServer(ProxyConnection),
}

/// Results of analyzing arc constraints on a typed graph.
///
/// Given the current state of a typed graph, this struct describes which arcs
/// can still be **added** without violating any `MAX` constraint, and which
/// existing arcs can be **removed** without violating any `MIN` constraint.
#[derive(Debug, Clone)]
pub struct ArcConstraints {
    /// Arcs that can be added: both endpoints exist, the specific
    /// `(source, target, kind)` triple is not already present, and the
    /// source vertex has not yet reached its `MAX` for this arc kind.
    pub addable: Vec<MyTypedGraphArcs>,
    /// Existing arcs whose removal would *not* violate the `MIN` constraint
    /// for that `(source, kind)` group.
    pub removable: Vec<MyTypedGraphArcs>,
}

/// Given a typed graph (as produced by `execute_query(Read::new(), …)`),
/// compute which arcs can be added without violating `MAX` and which
/// existing arcs can be removed without violating `MIN`.
pub fn compute_arc_constraints(
    graph: &DiGraph<MyTypedGraphVertex, MyTypedGraphEdge>,
) -> ArcConstraints {
    let mut addable = Vec::new();
    let mut removable = Vec::new();

    // Build a set of existing (source, target, kind) triples for O(1) lookup.
    let existing_edges: HashSet<(MyTypedGraphVertex, MyTypedGraphVertex, MyTypedGraphEdge)> = graph
        .edge_indices()
        .filter_map(|ei| {
            let (si, ti) = graph.edge_endpoints(ei)?;
            Some((graph[si].clone(), graph[ti].clone(), graph[ei].clone()))
        })
        .collect();

    for source_idx in graph.node_indices() {
        let source = &graph[source_idx];

        // Count outgoing edges grouped by edge kind.
        let mut outgoing_by_kind: HashMap<MyTypedGraphEdge, usize> = HashMap::default();
        for edge in graph.edges_directed(source_idx, petgraph::Direction::Outgoing) {
            *outgoing_by_kind.entry(edge.weight().clone()).or_insert(0) += 1;
        }

        // ── Addable arcs ───────────────────────────────────────────────
        // For every other vertex in the graph, check whether an arc of
        // each valid type can still be created from `source`.
        for target_idx in graph.node_indices() {
            if source_idx == target_idx {
                continue;
            }
            let target = &graph[target_idx];

            for candidate in possible_arcs_between(source, target) {
                let kind = candidate.kind();
                let count = outgoing_by_kind.get(&kind).copied().unwrap_or(0);
                if count < candidate.max()
                    && !existing_edges.contains(&(source.clone(), target.clone(), kind))
                {
                    addable.push(candidate);
                }
            }
        }

        // ── Removable arcs ─────────────────────────────────────────────
        // For every outgoing edge, check whether removing it would still
        // leave at least `MIN` edges of the same kind from `source`.
        for edge in graph.edges_directed(source_idx, petgraph::Direction::Outgoing) {
            let target = &graph[edge.target()];
            let kind = edge.weight();

            if let Some(arc) = arc_from_vertices_and_edge(source, target, kind) {
                let count = outgoing_by_kind.get(kind).copied().unwrap_or(0);
                if count > arc.min() {
                    removable.push(arc);
                }
            }
        }
    }

    ArcConstraints { addable, removable }
}

/// A single schema violation found in a typed graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaViolation {
    /// An edge connects two vertex types that have no `Connectable` relationship,
    /// or the edge kind does not match the expected arc type.
    InvalidEdge {
        source: MyTypedGraphVertex,
        target: MyTypedGraphVertex,
        edge: MyTypedGraphEdge,
    },
    /// A source vertex has more outgoing edges of a given kind than the `MAX`
    /// constraint allows.
    ExceedsMax {
        source: MyTypedGraphVertex,
        edge_kind: MyTypedGraphEdge,
        count: usize,
        max: usize,
    },
    /// A source vertex has fewer outgoing edges of a given kind than the `MIN`
    /// constraint requires.
    BelowMin {
        source: MyTypedGraphVertex,
        edge_kind: MyTypedGraphEdge,
        count: usize,
        min: usize,
    },
}

impl std::fmt::Display for SchemaViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaViolation::InvalidEdge {
                source,
                target,
                edge,
            } => write!(
                f,
                "Invalid edge {:?} between {:?} and {:?}",
                edge, source, target
            ),
            SchemaViolation::ExceedsMax {
                source,
                edge_kind,
                count,
                max,
            } => write!(
                f,
                "Vertex {:?} has {} outgoing {:?} edges, exceeding max of {}",
                source, count, edge_kind, max
            ),
            SchemaViolation::BelowMin {
                source,
                edge_kind,
                count,
                min,
            } => write!(
                f,
                "Vertex {:?} has {} outgoing {:?} edges, below min of {}",
                source, count, edge_kind, min
            ),
        }
    }
}

/// Returns the `(MIN, MAX)` cardinality constraints and the expected
/// `MyTypedGraphEdge` for a given `(source_vertex, target_vertex)` pair,
/// or `None` if no `Connectable` relationship exists between them.
fn edge_constraints_for(
    source: &MyTypedGraphVertex,
    target: &MyTypedGraphVertex,
    edge: &MyTypedGraphEdge,
) -> Option<(usize, usize)> {
    match (source, target, edge) {
        (
            MyTypedGraphVertex::User(_),
            MyTypedGraphVertex::Server(_),
            MyTypedGraphEdge::UserToServer(_),
        ) => Some((
            <User as Connectable<Server, NetworkConnection>>::MIN,
            <User as Connectable<Server, NetworkConnection>>::MAX,
        )),
        (
            MyTypedGraphVertex::User(_),
            MyTypedGraphVertex::Database(_),
            MyTypedGraphEdge::ServerToDb(_),
        ) => Some((
            <User as Connectable<Database, DbConnection>>::MIN,
            <User as Connectable<Database, DbConnection>>::MAX,
        )),
        (
            MyTypedGraphVertex::Server(_),
            MyTypedGraphVertex::Database(_),
            MyTypedGraphEdge::ServerToDb(_),
        ) => Some((
            <Server as Connectable<Database, DbConnection>>::MIN,
            <Server as Connectable<Database, DbConnection>>::MAX,
        )),
        (
            MyTypedGraphVertex::LoadBalancer(_),
            MyTypedGraphVertex::Server(_),
            MyTypedGraphEdge::LoadBalancerToServer(_),
        ) => Some((
            <LoadBalancer as Connectable<Server, ProxyConnection>>::MIN,
            <LoadBalancer as Connectable<Server, ProxyConnection>>::MAX,
        )),
        (
            MyTypedGraphVertex::User(_),
            MyTypedGraphVertex::Database(_),
            MyTypedGraphEdge::UserToDb(_),
        ) => Some((
            <User as Connectable<Database, UserToDbConnection>>::MIN,
            <User as Connectable<Database, UserToDbConnection>>::MAX,
        )),
        _ => None,
    }
}

/// Returns the required `(edge_kind, MIN, MAX)` constraints that apply
/// to a given vertex type as a source.
fn required_constraints_for(vertex: &MyTypedGraphVertex) -> Vec<(MyTypedGraphEdge, usize, usize)> {
    match vertex {
        MyTypedGraphVertex::User(_) => vec![
            (
                MyTypedGraphEdge::UserToServer(NetworkConnection),
                <User as Connectable<Server, NetworkConnection>>::MIN,
                <User as Connectable<Server, NetworkConnection>>::MAX,
            ),
            (
                MyTypedGraphEdge::ServerToDb(DbConnection),
                <User as Connectable<Database, DbConnection>>::MIN,
                <User as Connectable<Database, DbConnection>>::MAX,
            ),
            (
                MyTypedGraphEdge::UserToDb(UserToDbConnection),
                <User as Connectable<Database, UserToDbConnection>>::MIN,
                <User as Connectable<Database, UserToDbConnection>>::MAX,
            ),
        ],
        MyTypedGraphVertex::Server(_) => vec![(
            MyTypedGraphEdge::ServerToDb(DbConnection),
            <Server as Connectable<Database, DbConnection>>::MIN,
            <Server as Connectable<Database, DbConnection>>::MAX,
        )],
        MyTypedGraphVertex::LoadBalancer(_) => vec![(
            MyTypedGraphEdge::LoadBalancerToServer(ProxyConnection),
            <LoadBalancer as Connectable<Server, ProxyConnection>>::MIN,
            <LoadBalancer as Connectable<Server, ProxyConnection>>::MAX,
        )],
        MyTypedGraphVertex::Database(_) => vec![],
    }
}

/// Verify that a typed graph respects its schema.
///
/// This checks three things for every vertex and edge in the graph:
///
/// 1. **Valid edges** – every edge connects vertex types that have a
///    `Connectable` relationship, with a matching edge kind.
/// 2. **MAX constraint** – no source vertex has more outgoing edges of a
///    given kind than the `MAX` defined by its `Connectable` impl.
/// 3. **MIN constraint** – every source vertex has at least `MIN` outgoing
///    edges of each kind that is required by its `Connectable` impls.
///
/// Returns `Ok(())` if the graph is valid, or `Err` with the list of all
/// violations found.
pub fn validate_schema(
    graph: &DiGraph<MyTypedGraphVertex, MyTypedGraphEdge>,
) -> Result<(), Vec<SchemaViolation>> {
    let mut violations = Vec::new();

    // ── 1. Validate every edge ─────────────────────────────────────────
    for edge_idx in graph.edge_indices() {
        if let Some((si, ti)) = graph.edge_endpoints(edge_idx) {
            let source = &graph[si];
            let target = &graph[ti];
            let edge = &graph[edge_idx];

            if edge_constraints_for(source, target, edge).is_none() {
                violations.push(SchemaViolation::InvalidEdge {
                    source: source.clone(),
                    target: target.clone(),
                    edge: edge.clone(),
                });
            }
        }
    }

    // ── 2 & 3. Validate cardinality for each source vertex ─────────────
    for node_idx in graph.node_indices() {
        let source = &graph[node_idx];

        // Count outgoing edges by kind.
        let mut outgoing_by_kind: HashMap<MyTypedGraphEdge, usize> = HashMap::default();
        for edge in graph.edges_directed(node_idx, petgraph::Direction::Outgoing) {
            *outgoing_by_kind.entry(edge.weight().clone()).or_insert(0) += 1;
        }

        // Check MAX for each edge kind that actually appears.
        for (kind, count) in &outgoing_by_kind {
            // Find the max by inspecting any target with this edge kind.
            let max = graph
                .edges_directed(node_idx, petgraph::Direction::Outgoing)
                .find_map(|e| {
                    if e.weight() == kind {
                        let target = &graph[e.target()];
                        edge_constraints_for(source, target, kind).map(|(_, m)| m)
                    } else {
                        None
                    }
                });

            if let Some(max) = max {
                if *count > max {
                    violations.push(SchemaViolation::ExceedsMax {
                        source: source.clone(),
                        edge_kind: kind.clone(),
                        count: *count,
                        max,
                    });
                }
            }
        }

        // Check MIN for every required edge kind.
        for (edge_kind, min, _max) in required_constraints_for(source) {
            if min > 0 {
                let count = outgoing_by_kind.get(&edge_kind).copied().unwrap_or(0);
                if count < min {
                    violations.push(SchemaViolation::BelowMin {
                        source: source.clone(),
                        edge_kind,
                        count,
                        min,
                    });
                }
            }
        }
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

/// Returns all valid arc types that can exist between two vertex types.
fn possible_arcs_between(
    source: &MyTypedGraphVertex,
    target: &MyTypedGraphVertex,
) -> Vec<MyTypedGraphArcs> {
    match (source, target) {
        (MyTypedGraphVertex::User(u), MyTypedGraphVertex::Server(s)) => {
            vec![MyTypedGraphArcs::UserToServer(Arc {
                source: u.clone(),
                target: s.clone(),
                kind: NetworkConnection,
            })]
        }
        (MyTypedGraphVertex::User(u), MyTypedGraphVertex::Database(d)) => {
            vec![MyTypedGraphArcs::UserToDb(Arc {
                source: u.clone(),
                target: d.clone(),
                kind: UserToDbConnection,
            })]
        }
        (MyTypedGraphVertex::Server(s), MyTypedGraphVertex::Database(d)) => {
            vec![MyTypedGraphArcs::ServerToDb(Arc {
                source: s.clone(),
                target: d.clone(),
                kind: DbConnection,
            })]
        }
        (MyTypedGraphVertex::LoadBalancer(lb), MyTypedGraphVertex::Server(s)) => {
            vec![MyTypedGraphArcs::LoadBalancerToServer(Arc {
                source: lb.clone(),
                target: s.clone(),
                kind: ProxyConnection,
            })]
        }
        _ => vec![],
    }
}

/// Reconstructs a `MyTypedGraphArcs` value from its constituent source
/// vertex, target vertex, and edge weight.
fn arc_from_vertices_and_edge(
    source: &MyTypedGraphVertex,
    target: &MyTypedGraphVertex,
    edge: &MyTypedGraphEdge,
) -> Option<MyTypedGraphArcs> {
    match (source, target, edge) {
        (
            MyTypedGraphVertex::User(u),
            MyTypedGraphVertex::Server(s),
            MyTypedGraphEdge::UserToServer(nc),
        ) => Some(MyTypedGraphArcs::UserToServer(Arc {
            source: u.clone(),
            target: s.clone(),
            kind: nc.clone(),
        })),
        (
            MyTypedGraphVertex::User(u),
            MyTypedGraphVertex::Database(d),
            MyTypedGraphEdge::UserToDb(c),
        ) => Some(MyTypedGraphArcs::UserToDb(Arc {
            source: u.clone(),
            target: d.clone(),
            kind: c.clone(),
        })),
        (
            MyTypedGraphVertex::Server(s),
            MyTypedGraphVertex::Database(d),
            MyTypedGraphEdge::ServerToDb(c),
        ) => Some(MyTypedGraphArcs::ServerToDb(Arc {
            source: s.clone(),
            target: d.clone(),
            kind: c.clone(),
        })),
        (
            MyTypedGraphVertex::LoadBalancer(lb),
            MyTypedGraphVertex::Server(s),
            MyTypedGraphEdge::LoadBalancerToServer(c),
        ) => Some(MyTypedGraphArcs::LoadBalancerToServer(Arc {
            source: lb.clone(),
            target: s.clone(),
            kind: c.clone(),
        })),
        _ => None,
    }
}

impl<P> PureCRDT for MyTypedGraph<P>
where
    P: Policy,
{
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
            MyTypedGraph::AddVertex { .. } | MyTypedGraph::AddArc(_) => false,
            MyTypedGraph::RemoveVertex { .. } | MyTypedGraph::RemoveArc(_) => true,
            // MyTypedGraph::AddArc(arc) => {
            //     let count_stable = stable
            //         .iter()
            //         .filter(|op| match op {
            //             MyTypedGraph::AddArc(a) => {
            //                 a.source() == arc.source() && a.kind() == arc.kind()
            //             }
            //             _ => false,
            //         })
            //         .count();
            //     let unstable_ops: Vec<_> = unstable.collect();
            //     let count_unstable = unstable_ops
            //         .iter()
            //         .filter(|op| match op.op() {
            //             MyTypedGraph::AddArc(a) => {
            //                 a.source() == arc.source() && a.kind() == arc.kind()
            //             }
            //             _ => false,
            //         })
            //         .count();

            //     if count_stable + count_unstable < arc.max() {
            //         false
            //     } else {
            //         unstable_ops
            //             .iter()
            //             .any(|old_tagged_op| match old_tagged_op.op() {
            //                 MyTypedGraph::AddArc(a) => {
            //                     a.source() == arc.source()
            //                         && a.kind() == arc.kind()
            //                         && P::compare(new_tagged_op.tag(), old_tagged_op.tag())
            //                             == std::cmp::Ordering::Less
            //                 }
            //                 _ => false,
            //             })
            //     }
            // }
            MyTypedGraph::__Marker(_, _) => unreachable!(),
        }
    }

    fn redundant_by_when_redundant(
        old_op: &Self,
        _old_tag: Option<&Tag>,
        is_conc: bool,
        new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        !is_conc
            && match (old_op, new_tagged_op.op()) {
                (MyTypedGraph::AddArc(arc), MyTypedGraph::RemoveVertex { id: v3 }) => {
                    arc.source() == *v3 || arc.target() == *v3
                }
                (MyTypedGraph::AddArc(arc1), MyTypedGraph::AddArc(arc2))
                | (MyTypedGraph::AddArc(arc1), MyTypedGraph::RemoveArc(arc2)) => {
                    arc1.source() == arc2.source()
                        && arc1.kind() == arc2.kind()
                        && arc1.target() == arc2.target()
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

    fn post_effect(
        new_tagged_op: &TaggedOp<Self>,
        stable: &mut Self::StableState,
        unstable: &mut impl IsUnstableState<Self>,
    ) {
        if let MyTypedGraph::AddArc(arc) = new_tagged_op.op() {
            let source = arc.source();
            let kind = arc.kind();
            let target = arc.target();
            let max = arc.max();

            // Count how many existing arcs of the same (source, kind) we have in stable and unstable.
            // Do not account for duplicates because if the same arc already exists, the new one is redundant and will be pruned later.

            println!("Stable state before pruning:");
            for op in stable.iter() {
                println!("  {:?}", op);
            }
            println!("Unstable state before pruning:");
            for op in unstable.iter() {
                println!("  {}", op);
            }

            let graph = Self::execute_query(Read::new(), stable, unstable);
            if let Some(idx) = graph.node_indices().find(|&i| graph[i] == source) {
                let count = graph
                    .edges_directed(idx, petgraph::Direction::Outgoing)
                    .filter(|edge| edge.weight() == &kind && graph[edge.target()] != target)
                    .count()
                    + 1;

                // let mut stable_candidates = Vec::new();
                // for op in stable.iter() {
                //     if let MyTypedGraph::AddArc(a) = op {
                //         if a.source() == source && a.kind() == kind {
                //             stable_candidates.push(a);
                //         }
                //     }
                // }
                // let unique_stable_candidates: HashSet<_> = stable_candidates.iter().collect();

                // let mut unstable_candidates: Vec<_> = Vec::new();
                // for op in unstable.iter() {
                //     if let MyTypedGraph::AddArc(a) = op.op() {
                //         if a.source() == source && a.kind() == kind {
                //             unstable_candidates.push(op);
                //         }
                //     }
                // }
                // let unique_unstable_candidates: Vec<_> = Vec::new();
                // for candidate in unstable_candidates.iter() {
                //     if unique_unstable_candidates.
                // }

                // let total = unique_stable_candidates.len() + unique_unstable_candidates.len();
                // println!("       Total count is currently {}, max is {}", total, max);
                // println!(
                //     "      Candidates for pruning: {:?}",
                //     unique_unstable_candidates
                // );
                // +1 because the count does not include the new arc being added, which will also contribute to the count and potentially cause it to exceed the max.
                if count > max {
                    let mut candidates = Vec::new();
                    // Collect all candidate arcs for pruning
                    for op in unstable.iter() {
                        if let MyTypedGraph::AddArc(a) = op.op() {
                            if a.source() == source && a.kind() == kind {
                                candidates.push(op.clone());
                            }
                        }
                    }

                    candidates.sort_by(|a, b| P::compare(a.tag(), b.tag()));

                    let to_remove = count - max;
                    for loser in candidates.iter().take(to_remove) {
                        // TODO: not removing if exactly the same arc because idempotent
                        // println!(
                        //     "       Pruning redundant op {} due to new op {}",
                        //     loser, new_tagged_op
                        // );
                        unstable.remove(loser.id());
                    }
                }

                // println!(
                //     "       Final graph:\n{:?}",
                //     Self::execute_query(Read::new(), stable, unstable)
                // );
            }
        }
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

                let count = graph
                    .edges_directed(
                        graph.node_indices().find(|&i| graph[i] == source).unwrap(),
                        petgraph::Direction::Outgoing,
                    )
                    .filter(|edge| edge.weight() == &kind)
                    .count();

                // println!(
                //     "Current graph:\n{:?}",
                //     petgraph::dot::Dot::with_config(&graph, &[])
                // );

                // println!(
                //     "Checking if can add arc {:?} -> {:?} of kind {:?}: currently {} outgoing, max is {}",
                //     source,
                //     target,
                //     kind,
                //     count,
                //     arc.max()
                // );

                count < arc.max()
            }
            MyTypedGraph::__Marker(_, _) => unreachable!(),
        }
    }
}

impl<P> Eval<Read<<Self as PureCRDT>::Value>> for MyTypedGraph<P>
where
    P: Policy,
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
                        continue;
                    }
                    let idx = graph.add_node(id.clone());
                    node_index.insert(id.clone(), idx);
                }
                MyTypedGraph::AddArc(arcs) => {
                    let v1 = arcs.source();
                    let v2 = arcs.target();
                    let e = arcs.kind();
                    let tuple = (v1, v2, e);
                    if edge_index.contains(&tuple) {
                        continue;
                    }
                    let (v1, v2, e) = tuple;
                    if let (Some(a), Some(b)) = (node_index.get(&v1), node_index.get(&v2)) {
                        graph.add_edge(*a, *b, e.clone());
                        edge_index.insert((v1, v2, e));
                    }
                }
                MyTypedGraph::RemoveVertex { .. }
                | MyTypedGraph::RemoveArc(_)
                | MyTypedGraph::__Marker(_, _) => unreachable!(),
            }
        }
        graph
    }
}

#[cfg(feature = "fuzz")]
impl<P> OpGenerator for MyTypedGraph<P>
where
    P: Policy,
{
    type Config = ();

    fn generate(
        rng: &mut impl rand::RngCore,
        _config: &Self::Config,
        stable: &Self::StableState,
        unstable: &impl IsUnstableState<Self>,
    ) -> Self {
        use rand::distr::{Distribution, weighted::WeightedIndex};
        use rand::seq::IndexedRandom;

        enum Choice {
            AddVertex,
            RemoveVertex,
            AddArc,
            RemoveArc,
        }

        let graph = Self::execute_query(Read::new(), stable, unstable);
        let constraints = compute_arc_constraints(&graph);
        let existing_vertices: Vec<_> = graph.node_weights().cloned().collect();

        // choice: if node < 2 -> add vertex
        // if edge = 0 -> add vertex, remove vertex, add arc (if addable nonempty)
        // else (node >= 2 && edge > 0) -> add vertex, remove vertex, add arc (if addable nonempty), remove arc (if removable nonempty)

        let choice = if graph.node_count() < 2 {
            &Choice::AddVertex
        } else if graph.edge_count() == 0 {
            if constraints.addable.is_empty() {
                let dist = WeightedIndex::new([2, 1]).unwrap();
                &[Choice::AddVertex, Choice::RemoveVertex][dist.sample(rng)]
            } else {
                let dist = WeightedIndex::new([2, 1, 3]).unwrap();
                &[Choice::AddVertex, Choice::RemoveVertex, Choice::AddArc][dist.sample(rng)]
            }
        } else if constraints.removable.is_empty() && constraints.addable.is_empty() {
            let dist = WeightedIndex::new([2, 1]).unwrap();
            &[Choice::AddVertex, Choice::RemoveVertex][dist.sample(rng)]
        } else if !constraints.removable.is_empty() && constraints.addable.is_empty() {
            let dist = WeightedIndex::new([2, 1]).unwrap();
            &[Choice::AddVertex, Choice::RemoveVertex, Choice::RemoveArc][dist.sample(rng)]
        } else if constraints.removable.is_empty() && !constraints.addable.is_empty() {
            let dist = WeightedIndex::new([2, 1, 3]).unwrap();
            &[Choice::AddVertex, Choice::RemoveVertex, Choice::AddArc][dist.sample(rng)]
        } else {
            let dist = WeightedIndex::new([2, 1, 3, 2]).unwrap();
            &[
                Choice::AddVertex,
                Choice::RemoveVertex,
                Choice::AddArc,
                Choice::RemoveArc,
            ][dist.sample(rng)]
        };

        match choice {
            Choice::AddVertex => MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::generate(rng, &()),
            },
            Choice::RemoveVertex => {
                use rand::seq::IndexedRandom;

                let vertex = existing_vertices.choose(rng).unwrap().clone();
                MyTypedGraph::RemoveVertex { id: vertex }
            }
            Choice::AddArc => {
                MyTypedGraph::AddArc(constraints.addable.choose(rng).unwrap().clone())
            }
            Choice::RemoveArc => {
                MyTypedGraph::RemoveArc(constraints.removable.choose(rng).unwrap().clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::graph::typed_graph::{Arc, MyTypedGraphArcs};
    use crate::policy::LwwPolicy;
    use crate::utils::membership::twins;
    use moirai_protocol::replica::IsReplica;
    use moirai_protocol::state::po_log::VecLog;
    use petgraph::graph;

    use super::*;

    fn assert_convergence<R: IsReplica<VecLog<MyTypedGraph<LwwPolicy>>>>(
        replica_a: &R,
        replica_b: &R,
    ) {
        assert!(
            vf2::isomorphisms(&replica_a.query(Read::new()), &replica_b.query(Read::new()))
                .first()
                .is_some(),
            "Replicas did not converge!\nA: {:?}\nB: {:?}",
            petgraph::dot::Dot::with_config(&replica_a.query(Read::new()), &[]),
            petgraph::dot::Dot::with_config(&replica_b.query(Read::new()), &[]),
        );
    }

    #[test]
    fn simple_graph() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();
        replica_b.receive(e1);

        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let e3 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToServer(Arc {
                source: User(1),
                target: Server(1),
                kind: NetworkConnection,
            })))
            .unwrap();
        replica_b.receive(e3);

        assert_convergence(&replica_a, &replica_b);
        assert_eq!(replica_a.query(Read::new()).node_count(), 2);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 1);
    }

    #[test]
    fn concurrent_add_same_vertex() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let event_a = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();
        let event_b = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn multiple_adds_same_vertex() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(1)),
            })
            .unwrap();
        replica_b.receive(e1);
        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(1)),
            })
            .unwrap();
        replica_a.receive(e2);
        let e3 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(1)),
            })
            .unwrap();
        replica_b.receive(e3);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn remove_vertex_cascades_arcs() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();
        replica_b.receive(e1);
        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let e3 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToServer(Arc {
                source: User(1),
                target: Server(1),
                kind: NetworkConnection,
            })))
            .unwrap();
        replica_b.receive(e3);

        let e4 = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();
        replica_a.receive(e4);

        assert_eq!(replica_a.query(Read::new()).node_count(), 1);
        assert_eq!(replica_a.query(Read::new()).edge_count(), 0);
        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn concurrent_add_arc_vs_remove_vertex() {
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(1)),
            })
            .unwrap();
        replica_b.receive(e1);
        let e2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();
        replica_a.receive(e2);

        let event_a = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToServer(Arc {
                source: User(1),
                target: Server(1),
                kind: NetworkConnection,
            })))
            .unwrap();
        let event_b = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyTypedGraphVertex::Server(Server(1)),
            })
            .unwrap();
        replica_a.receive(event_b);
        replica_b.receive(event_a);

        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn error_1() {
        // this test reproduce this trace:
        // digraph {  0 [ label="[AddVertex { id: User(User(13)) }@(1:1)]"]  1 [ label="[AddVertex { id: User(User(19)) }@(1:2)]"]  2 [ label="[AddVertex { id: Database(Database(7)) }@(0:1)]"]  3 [ label="[AddArc(UserToDb(Arc { source: User(13), target: Database(7), kind: UserToDbConnection }))@(1:3)]"]  4 [ label="[RemoveArc(UserToDb(Arc { source: User(13), target: Database(7), kind: UserToDbConnection }))@(1:4)]"]  5 [ label="[AddArc(UserToDb(Arc { source: User(13), target: Database(7), kind: UserToDbConnection }))@(0:2)]"]  1 -> 0 [ ]  2 -> 0 [ ]  3 -> 2 [ ]  3 -> 1 [ ]  4 -> 3 [ ]  5 -> 2 [ ]  5 -> 1 [ ]}
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e_b_1 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(13)),
            })
            .unwrap();

        replica_a.receive(e_b_1);

        let e_b_2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(19)),
            })
            .unwrap();

        let e_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(7)),
            })
            .unwrap();

        replica_a.receive(e_b_2);
        replica_b.receive(e_a_1);

        let e_a_2 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(13),
                target: Database(7),
                kind: UserToDbConnection,
            })))
            .unwrap();

        let e_b_3 = replica_b
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(13),
                target: Database(7),
                kind: UserToDbConnection,
            })))
            .unwrap();

        let e_b_4 = replica_b
            .send(MyTypedGraph::RemoveArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(13),
                target: Database(7),
                kind: UserToDbConnection,
            })))
            .unwrap();

        replica_b.receive(e_a_2);

        // println!("- - - - - - - - - - - - -");
        // println!("1. Stable A: {:?}", replica_a.state().stable());
        // println!("1. Unstable A: {:?}", replica_a.state().unstable());

        replica_a.receive(e_b_3);

        // println!("2. Stable A: {:?}", replica_a.state().stable());
        // println!("2. Unstable A: {:?}", replica_a.state().unstable());

        replica_a.receive(e_b_4);

        // println!("3. Stable A: {:?}", replica_a.state().stable());
        // println!("3. Unstable A: {:?}", replica_a.state().unstable());
        // let graph_a = replica_a.query(Read::new());
        // println!(
        //     "Graph A after receiving concurrent add arc:\n{:?}",
        //     petgraph::dot::Dot::with_config(&graph_a, &[])
        // );

        let graph_a = replica_a.query(Read::new());
        let graph_b = replica_b.query(Read::new());

        assert_eq!(graph_a.node_count(), 3);
        assert_eq!(graph_b.node_count(), 3);
        assert_eq!(graph_b.edge_count(), 1);
        assert_eq!(graph_a.edge_count(), 1);

        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn error_2() {
        // this test reproduce this trace:
        // digraph {  0 [ label="[AddVertex { id: User(User(15)) }@(1:1)]"]  1 [ label="[AddVertex { id: Database(Database(4)) }@(1:2)]"]  2 [ label="[AddArc(UserToDb(Arc { source: User(15), target: Database(4), kind: UserToDbConnection }))@(1:3)]"]  3 [ label="[AddVertex { id: Database(Database(17)) }@(0:1)]"]  4 [ label="[AddArc(UserToDb(Arc { source: User(15), target: Database(17), kind: UserToDbConnection }))@(0:2)]"]  0 -> 1 [ ]  1 -> 2 [ ]  0 -> 3 [ ]  3 -> 4 [ ]  0 -> 4 [ ]}
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e_b_1 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(15)),
            })
            .unwrap();

        replica_a.receive(e_b_1);

        let e_b_2 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(4)),
            })
            .unwrap();

        let e_b_3 = replica_b
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(15),
                target: Database(4),
                kind: UserToDbConnection,
            })))
            .unwrap();

        let e_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(17)),
            })
            .unwrap();

        let e_a_2 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(15),
                target: Database(17),
                kind: UserToDbConnection,
            })))
            .unwrap();

        replica_a.receive(e_b_2);
        replica_a.receive(e_b_3);
        replica_b.receive(e_a_1);
        replica_b.receive(e_a_2);

        let graph_a = replica_a.query(Read::new());
        let graph_b = replica_b.query(Read::new());

        assert_eq!(graph_a.node_count(), 3);
        assert_eq!(graph_b.node_count(), 3);
        assert_eq!(graph_a.edge_count(), 1);
        assert_eq!(graph_b.edge_count(), 1);

        assert_convergence(&replica_a, &replica_b);
    }

    #[test]
    fn error_3() {
        // this test reproduce this trace:
        // digraph {  0 [ label="[AddVertex { id: User(User(17)) }@(0:1)]"]  1 [ label="[AddVertex { id: Database(Database(3)) }@(0:2)]"]  2 [ label="[AddVertex { id: Database(Database(11)) }@(1:1)]"]  3 [ label="[AddArc(UserToDb(Arc { source: User(17), target: Database(3), kind: UserToDbConnection }))@(0:3)]"]  4 [ label="[AddArc(UserToDb(Arc { source: User(17), target: Database(11), kind: UserToDbConnection }))@(1:2)]"]  5 [ label="[RemoveVertex { id: Database(Database(11)) }@(1:3)]"]  0 -> 1 [ ]  0 -> 2 [ ]  1 -> 3 [ ]  2 -> 4 [ ]  0 -> 4 [ ]  4 -> 5 [ ]  0 -> 5 [ ]}
        let (mut replica_a, mut replica_b) = twins::<MyTypedGraph<LwwPolicy>>();

        let e_a_1 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::User(User(17)),
            })
            .unwrap();
        replica_b.receive(e_a_1);
        let e_a_2 = replica_a
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(3)),
            })
            .unwrap();
        let e_a_3 = replica_a
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(17),
                target: Database(3),
                kind: UserToDbConnection,
            })))
            .unwrap();

        let b_e_1 = replica_b
            .send(MyTypedGraph::AddVertex {
                id: MyTypedGraphVertex::Database(Database(11)),
            })
            .unwrap();
        let b_e_2 = replica_b
            .send(MyTypedGraph::AddArc(MyTypedGraphArcs::UserToDb(Arc {
                source: User(17),
                target: Database(11),
                kind: UserToDbConnection,
            })))
            .unwrap();
        let b_e_3 = replica_b
            .send(MyTypedGraph::RemoveVertex {
                id: MyTypedGraphVertex::Database(Database(11)),
            })
            .unwrap();

        replica_a.receive(b_e_1);
        replica_a.receive(b_e_2);
        replica_a.receive(b_e_3);
        replica_b.receive(e_a_2);
        replica_b.receive(e_a_3);

        let graph_a = replica_a.query(Read::new());
        let graph_b = replica_b.query(Read::new());

        assert_eq!(graph_a.node_count(), 2);
        assert_eq!(graph_b.node_count(), 2);
        assert_eq!(graph_a.edge_count(), 1);
        assert_eq!(graph_b.edge_count(), 1);

        assert_convergence(&replica_a, &replica_b);
    }

    #[cfg(feature = "fuzz")]
    #[test]
    fn fuzz_typed_graph() {
        use moirai_fuzz::{
            config::{FuzzerConfig, RunConfig},
            fuzzer::fuzzer,
        };
        use moirai_protocol::state::po_log::VecLog;

        let run_1 = RunConfig::new(0.4, 2, 6, None, None, true, false);
        let runs = vec![run_1; 10_000];

        let config = FuzzerConfig::<VecLog<MyTypedGraph<LwwPolicy>>>::new(
            "typed_graph",
            runs,
            true,
            // |a, b| vf2::isomorphisms(a, b).first().is_some(),
            |a, b| {
                a.node_count() == b.node_count() && a.edge_count() == b.edge_count()
                // && validate_schema(&a).is_ok()
                // && validate_schema(&b).is_ok()
            },
            false,
        );

        fuzzer::<VecLog<MyTypedGraph<LwwPolicy>>>(config);
    }
}
