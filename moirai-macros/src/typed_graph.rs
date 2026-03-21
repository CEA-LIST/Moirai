/// Generates a complete typed graph CRDT from a schema definition.
///
/// # Syntax
///
/// ```rust,ignore
/// typed_graph! {
///     graph: MyGraph,
///     vertex: MyVertex,
///     edge: MyEdge,
///     arcs_type: MyArcs,
///
///     vertices { Foo, Bar, Baz },
///
///     connections {
///         FooToBar: Foo -> Bar (FooBarEdge) [0, 3],
///         BarToBaz: Bar -> Baz (BarBazEdge) [1, 1],
///     }
/// }
/// ```
///
/// # Requirements
///
/// - Each edge identifier (e.g. `FooBarEdge`) must be a **unit struct** in scope
///   that implements `Debug + Clone + PartialEq + Eq + Hash`.
/// - The macro generates one tuple-struct vertex id per entry in `vertices { ... }`.
///   Each generated vertex id wraps an `ObjectPath`.
use std::fmt::Debug;
use std::hash::Hash;

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

impl<S, T, E> moirai_protocol::utils::translate_ids::TranslateIds for Arc<S, T, E>
where
    S: Connectable<T, E> + moirai_protocol::utils::translate_ids::TranslateIds,
    T: moirai_protocol::utils::translate_ids::TranslateIds,
    E: Clone,
{
    fn translate_ids(
        &self,
        from: moirai_protocol::replica::ReplicaIdx,
        interner: &moirai_protocol::utils::intern_str::Interner,
    ) -> Self {
        Self {
            source: self.source.translate_ids(from, interner),
            target: self.target.translate_ids(from, interner),
            kind: self.kind.clone(),
        }
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

#[macro_export]
macro_rules! typed_graph {
    (@max *) => {
        usize::MAX
    };
    (@max $e:expr) => {
        $e
    };

    // Internal arm: normalised form used by both public arms.
    // `$src [$src_ty]` separates the variant name (ident, used in patterns)
    // from the actual type path (used in trait impls and generic parameters).
    (@generate
        graph: $graph:ident,
        vertex: $vertex:ident,
        edge: $edge:ident,
        arcs_type: $arcs:ident,

        vertices { $( $v:ident ),* },

        connections {
            $( $conn:ident : $src:ident [$src_ty:path] -> $tgt:ident [$tgt_ty:path] ( $ety:path ) [ $min:expr , $max:tt ] ),* $(,)?
        } $(,)?
    ) => {
        $(
            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub struct $v(pub $crate::moirai_protocol::state::sink::ObjectPath);

            impl $crate::moirai_protocol::utils::translate_ids::TranslateIds for $v {
                fn translate_ids(
                    &self,
                    from: $crate::moirai_protocol::replica::ReplicaIdx,
                    interner: &$crate::moirai_protocol::utils::intern_str::Interner,
                ) -> Self {
                    Self(self.0.translate_ids(from, interner))
                }
            }
        )*

        $(
            impl $crate::typed_graph::Connectable<$tgt_ty, $ety> for $src_ty {
                const MIN: usize = $min;
                const MAX: usize = $crate::typed_graph!(@max $max);
            }
        )*

        // Vertex enum
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $vertex {
            $( $v($v) ),*
        }

        fn vertex_path(vertex: &$vertex) -> &$crate::moirai_protocol::state::sink::ObjectPath {
            match vertex {
                $( $vertex::$v(id) => &id.0 ),*
            }
        }

        impl $crate::moirai_protocol::utils::translate_ids::TranslateIds for $vertex {
            fn translate_ids(
                &self,
                from: $crate::moirai_protocol::replica::ReplicaIdx,
                interner: &$crate::moirai_protocol::utils::intern_str::Interner,
            ) -> Self {
                match self {
                    $( Self::$v(id) => Self::$v(id.translate_ids(from, interner)) ),*
                }
            }
        }

        // Edge enum
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $edge {
            $( $conn($ety) ),*
        }

        // Arcs enum
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $arcs {
            $( $conn($crate::typed_graph::Arc<$src_ty, $tgt_ty, $ety>) ),*
        }

        impl $crate::moirai_protocol::utils::translate_ids::TranslateIds for $arcs
        where
            $(
                $src_ty: $crate::moirai_protocol::utils::translate_ids::TranslateIds,
                $tgt_ty: $crate::moirai_protocol::utils::translate_ids::TranslateIds,
            )*
        {
            fn translate_ids(
                &self,
                from: $crate::moirai_protocol::replica::ReplicaIdx,
                interner: &$crate::moirai_protocol::utils::intern_str::Interner,
            ) -> Self {
                match self {
                    $( Self::$conn(arc) => Self::$conn(arc.translate_ids(from, interner)) ),*
                }
            }
        }

        impl $arcs {
            pub fn source(&self) -> $vertex {
                match self {
                    $( $arcs::$conn(arc) => $vertex::$src(arc.source.clone()) ),*
                }
            }

            pub fn target(&self) -> $vertex {
                match self {
                    $( $arcs::$conn(arc) => $vertex::$tgt(arc.target.clone()) ),*
                }
            }

            pub fn kind(&self) -> $edge {
                match self {
                    $( $arcs::$conn(arc) => $edge::$conn(arc.kind.clone()) ),*
                }
            }

            pub fn max(&self) -> usize {
                match self {
                    $( $arcs::$conn(arc) => arc.max() ),*
                }
            }

            pub fn min(&self) -> usize {
                match self {
                    $( $arcs::$conn(arc) => arc.min() ),*
                }
            }
        }

        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $graph<P> {
            AddVertex { id: $vertex },
            RemoveVertex { id: $vertex },
            DeleteSubtree { prefix: $crate::moirai_protocol::state::sink::ObjectPath },
            AddArc($arcs),
            RemoveArc($arcs),
            #[doc(hidden)]
            __Marker(::std::convert::Infallible, ::std::marker::PhantomData<P>),
        }

        $crate::paste::paste! {
            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum [<$graph DerivedKey>] {
                AddVertex($vertex),
                RemoveVertex($vertex),
                DeleteSubtree($crate::moirai_protocol::state::sink::ObjectPath),
                AddArc($arcs),
                RemoveArc($arcs),
            }

            pub type [<$graph State>]<P> =
                $crate::moirai_protocol::state::unstable_state::DerivedKeyState<$graph<P>>;

            impl<P> $crate::moirai_protocol::state::unstable_state::HasDerivedKey for $graph<P>
            where
                P: Clone + ::std::fmt::Debug,
            {
                type DerivedKey = [<$graph DerivedKey>];

                fn derived_key(&self) -> Self::DerivedKey {
                    match self {
                        Self::AddVertex { id } => Self::DerivedKey::AddVertex(id.clone()),
                        Self::RemoveVertex { id } => Self::DerivedKey::RemoveVertex(id.clone()),
                        Self::DeleteSubtree { prefix } => {
                            Self::DerivedKey::DeleteSubtree(prefix.clone())
                        }
                        Self::AddArc(arc) => Self::DerivedKey::AddArc(arc.clone()),
                        Self::RemoveArc(arc) => Self::DerivedKey::RemoveArc(arc.clone()),
                        Self::__Marker(never, _) => match *never {},
                    }
                }
            }
        }

        impl<P> $crate::moirai_protocol::utils::translate_ids::TranslateIds for $graph<P>
        where
            P: Clone,
            $vertex: $crate::moirai_protocol::utils::translate_ids::TranslateIds,
            $arcs: $crate::moirai_protocol::utils::translate_ids::TranslateIds,
        {
            fn translate_ids(
                &self,
                from: $crate::moirai_protocol::replica::ReplicaIdx,
                interner: &$crate::moirai_protocol::utils::intern_str::Interner,
            ) -> Self {
                match self {
                    Self::AddVertex { id } => Self::AddVertex {
                        id: id.translate_ids(from, interner),
                    },
                    Self::RemoveVertex { id } => Self::RemoveVertex {
                        id: id.translate_ids(from, interner),
                    },
                    Self::DeleteSubtree { prefix } => Self::DeleteSubtree {
                        prefix: prefix.translate_ids(from, interner),
                    },
                    Self::AddArc(arc) => Self::AddArc(arc.translate_ids(from, interner)),
                    Self::RemoveArc(arc) => Self::RemoveArc(arc.translate_ids(from, interner)),
                    Self::__Marker(never, marker) => match *never {},
                }
            }
        }

        fn possible_arcs_between(
            source: &$vertex,
            target: &$vertex,
        ) -> Vec<$arcs> {
            let mut result = Vec::new();
            $(
                if let ($vertex::$src(s), $vertex::$tgt(t)) = (source, target) {
                    result.push($arcs::$conn($crate::typed_graph::Arc {
                        source: s.clone(),
                        target: t.clone(),
                        kind: $ety,
                    }));
                }
            )*
            result
        }

        fn arc_from_vertices_and_edge(
            source: &$vertex,
            target: &$vertex,
            edge: &$edge,
        ) -> Option<$arcs> {
            match (source, target, edge) {
                $(
                    ($vertex::$src(s), $vertex::$tgt(t), $edge::$conn(e)) => {
                        Some($arcs::$conn($crate::typed_graph::Arc {
                            source: s.clone(),
                            target: t.clone(),
                            kind: e.clone(),
                        }))
                    }
                )*
                _ => None,
            }
        }

        fn max_edges_for(source: &$vertex, kind: &$edge) -> usize {
            match (source, kind) {
                $(
                    ($vertex::$src(_), $edge::$conn(_)) => $crate::typed_graph!(@max $max),
                )*
                _ => usize::MAX,
            }
        }

        fn edge_constraints_for(
            source: &$vertex,
            target: &$vertex,
            edge: &$edge,
        ) -> Option<(usize, usize)> {
            match (source, target, edge) {
                $(
                    ($vertex::$src(_), $vertex::$tgt(_), $edge::$conn(_)) => {
                        Some(($min, $crate::typed_graph!(@max $max)))
                    },
                )*
                _ => None,
            }
        }

        fn required_constraints_for(vertex: &$vertex) -> Vec<($edge, usize, usize)> {
            let mut constraints = Vec::new();
            $(
                if let $vertex::$src(_) = vertex {
                    constraints.push(($edge::$conn($ety), $min, $crate::typed_graph!(@max $max)));
                }
            )*
            constraints
        }

        #[derive(Debug, Clone)]
        pub struct ArcConstraints {
            pub addable: Vec<$arcs>,
            pub removable: Vec<$arcs>,
        }

        pub fn compute_arc_constraints(
            graph: &petgraph::graph::DiGraph<$vertex, $edge>,
        ) -> ArcConstraints {
            use petgraph::visit::EdgeRef;

            let mut addable = Vec::new();
            let mut removable = Vec::new();

            let existing_edges: $crate::HashSet<($vertex, $vertex, $edge)> = graph
                .edge_indices()
                .filter_map(|ei| {
                    let (si, ti) = graph.edge_endpoints(ei)?;
                    Some((graph[si].clone(), graph[ti].clone(), graph[ei].clone()))
                })
                .collect();

            for source_idx in graph.node_indices() {
                let source = &graph[source_idx];

                let mut outgoing_by_kind: $crate::HashMap<$edge, usize> = $crate::HashMap::default();
                for edge in graph.edges_directed(source_idx, petgraph::Direction::Outgoing) {
                    *outgoing_by_kind.entry(edge.weight().clone()).or_insert(0) += 1;
                }

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

        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum SchemaViolation {
            InvalidEdge {
                source: $vertex,
                target: $vertex,
                edge: $edge,
            },
            ExceedsMax {
                source: $vertex,
                edge_kind: $edge,
                count: usize,
                max: usize,
            },
            BelowMin {
                source: $vertex,
                edge_kind: $edge,
                count: usize,
                min: usize,
            },
        }

        impl ::std::fmt::Display for SchemaViolation {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self {
                    SchemaViolation::InvalidEdge { source, target, edge } => write!(
                        f, "Invalid edge {:?} between {:?} and {:?}", edge, source, target
                    ),
                    SchemaViolation::ExceedsMax { source, edge_kind, count, max } => write!(
                        f, "Vertex {:?} has {} outgoing {:?} edges, exceeding max of {}",
                        source, count, edge_kind, max
                    ),
                    SchemaViolation::BelowMin { source, edge_kind, count, min } => write!(
                        f, "Vertex {:?} has {} outgoing {:?} edges, below min of {}",
                        source, count, edge_kind, min
                    ),
                }
            }
        }

        pub fn validate_schema(
            graph: &petgraph::graph::DiGraph<$vertex, $edge>,
        ) -> Result<(), Vec<SchemaViolation>> {
            use petgraph::visit::EdgeRef;

            let mut violations = Vec::new();

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

            for node_idx in graph.node_indices() {
                let source = &graph[node_idx];

                let mut outgoing_by_kind: $crate::HashMap<$edge, usize> = $crate::HashMap::default();
                for edge in graph.edges_directed(node_idx, petgraph::Direction::Outgoing) {
                    *outgoing_by_kind.entry(edge.weight().clone()).or_insert(0) += 1;
                }

                for (kind, count) in &outgoing_by_kind {
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

                    if let Some(max) = max
                        && *count > max
                    {
                        violations.push(SchemaViolation::ExceedsMax {
                            source: source.clone(),
                            edge_kind: kind.clone(),
                            count: *count,
                            max,
                        });
                    }
                }

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

        impl<P> $crate::moirai_protocol::crdt::pure_crdt::PureCRDT for $graph<P>
        where
            P: $crate::moirai_protocol::crdt::policy::Policy,
        {
            type Value = petgraph::graph::DiGraph<$vertex, $edge>;
            type StableState = Vec<Self>;

            const DISABLE_R_WHEN_R: bool = false;
            const DISABLE_R_WHEN_NOT_R: bool = false;
            // TODO: find a way to enable stabilize for this CRDT
            const DISABLE_STABILIZE: bool = true;

            fn redundant_itself<'a>(
                new_tagged_op: &$crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
                _stable: &Self::StableState,
                _unstable: impl Iterator<Item = &'a $crate::moirai_protocol::event::tagged_op::TaggedOp<Self>>,
            ) -> bool
            where
                Self: 'a,
            {
                match new_tagged_op.op() {
                    $graph::AddVertex { .. } | $graph::AddArc(_) => false,
                    $graph::RemoveVertex { .. }
                    | $graph::DeleteSubtree { .. }
                    | $graph::RemoveArc(_) => true,
                    $graph::__Marker(_, _) => unreachable!(),
                }
            }

            fn redundant_by_when_redundant(
                old_op: &Self,
                _old_tag: Option<&$crate::moirai_protocol::event::tag::Tag>,
                is_conc: bool,
                new_tagged_op: &$crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
            ) -> bool {
                !is_conc
                    && match (old_op, new_tagged_op.op()) {
                        ($graph::AddArc(arc), $graph::RemoveVertex { id: v }) => {
                            arc.source() == *v || arc.target() == *v
                        }
                        ($graph::AddVertex { id }, $graph::DeleteSubtree { prefix }) => {
                            prefix.is_prefix_of(vertex_path(id))
                        }
                        ($graph::AddArc(arc), $graph::DeleteSubtree { prefix }) => {
                            let source = arc.source();
                            let target = arc.target();
                            prefix.is_prefix_of(vertex_path(&source))
                                || prefix.is_prefix_of(vertex_path(&target))
                        }
                        (
                            $graph::DeleteSubtree { prefix: old_prefix },
                            $graph::DeleteSubtree { prefix: new_prefix },
                        ) => new_prefix.is_prefix_of(old_prefix),
                        ($graph::AddArc(arc1), $graph::AddArc(arc2))
                        | ($graph::AddArc(arc1), $graph::RemoveArc(arc2)) => {
                            arc1.source() == arc2.source()
                                && arc1.target() == arc2.target()
                                && arc1.kind() == arc2.kind()
                        }
                        ($graph::AddVertex { id: v1 }, $graph::AddVertex { id: v2 })
                        | ($graph::AddVertex { id: v1 }, $graph::RemoveVertex { id: v2 }) => {
                            v1 == v2
                        }
                        _ => false,
                    }
            }

            fn redundant_by_when_not_redundant(
                old_op: &Self,
                old_tag: Option<&$crate::moirai_protocol::event::tag::Tag>,
                is_conc: bool,
                new_tagged_op: &$crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
            ) -> bool {
                Self::redundant_by_when_redundant(old_op, old_tag, is_conc, new_tagged_op)
            }

            fn is_enabled(
                op: &Self,
                stable: &Self::StableState,
                unstable: &impl $crate::moirai_protocol::state::unstable_state::IsUnstableState<Self>,
            ) -> bool {
                use $crate::moirai_protocol::crdt::eval::Eval;
                use $crate::moirai_protocol::crdt::query::Read;

                let graph = Self::execute_query(Read::new(), stable, unstable);
                match op {
                    $graph::AddVertex { .. } => true,
                    $graph::RemoveVertex { id } => graph.node_weights().any(|node| node == id),
                    $graph::DeleteSubtree { prefix } => {
                        graph.node_weights().any(|node| prefix.is_prefix_of(vertex_path(node)))
                    },
                    $graph::RemoveArc(arc) => {
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
                    $graph::AddArc(arc) => {
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

                        count < arc.max()
                    }
                    $graph::__Marker(_, _) => unreachable!(),
                }
            }
        }

        impl<P> $crate::moirai_protocol::crdt::eval::Eval<
            $crate::moirai_protocol::crdt::query::Read<
                <Self as $crate::moirai_protocol::crdt::pure_crdt::PureCRDT>::Value
            >
        > for $graph<P>
        where
            P: $crate::moirai_protocol::crdt::policy::Policy,
        {
            fn execute_query(
                _q: $crate::moirai_protocol::crdt::query::Read<<$graph<P> as $crate::moirai_protocol::crdt::pure_crdt::PureCRDT>::Value>,
                stable: &<Self as $crate::moirai_protocol::crdt::pure_crdt::PureCRDT>::StableState,
                unstable: &impl $crate::moirai_protocol::state::unstable_state::IsUnstableState<Self>) -> <$crate::moirai_protocol::crdt::query::Read<<$graph<P> as $crate::moirai_protocol::crdt::pure_crdt::PureCRDT>::Value> as $crate::moirai_protocol::crdt::query::QueryOperation>::Response
            {
                let tagged_ops: Vec<(
                    &Self,
                    Option<&$crate::moirai_protocol::event::tag::Tag>,
                )> = stable
                    .iter()
                    .map(|op| (op, None))
                    .chain(unstable.iter().map(|t| (t.op(), Some(t.tag()))))
                    .collect();

                let mut graph = petgraph::graph::DiGraph::new();
                let mut node_index: $crate::HashMap<$vertex, _> = $crate::HashMap::default();

                for (op, _) in &tagged_ops {
                    if let $graph::AddVertex { id } = op
                        && !node_index.contains_key(id)
                    {
                        let idx = graph.add_node(id.clone());
                        node_index.insert(id.clone(), idx);
                    }
                }

                // Collect deduplicated arc candidates
                let mut deduped_arcs: $crate::HashMap<
                    ($vertex, $vertex, $edge),
                    Option<&$crate::moirai_protocol::event::tag::Tag>,
                > = $crate::HashMap::default();

                for (op, tag) in &tagged_ops {
                    if let $graph::AddArc(arcs) = op {
                        let v1 = arcs.source();
                        let v2 = arcs.target();
                        let e = arcs.kind();
                        if node_index.contains_key(&v1) && node_index.contains_key(&v2) {
                            let key = (v1, v2, e);
                            match deduped_arcs.entry(key) {
                                ::std::collections::hash_map::Entry::Vacant(entry) => {
                                    entry.insert(*tag);
                                }
                                ::std::collections::hash_map::Entry::Occupied(mut entry) => {
                                    let replace = match (entry.get(), tag) {
                                        (None, None) => false,
                                        (None, Some(_)) => true,
                                        (Some(_), None) => false,
                                        (Some(old_tag), Some(new_tag)) => {
                                            P::compare(old_tag, new_tag)
                                                == ::std::cmp::Ordering::Less
                                        }
                                    };
                                    if replace {
                                        entry.insert(*tag);
                                    }
                                }
                            }
                        }
                    }
                }

                let mut arc_entries: Vec<(
                    $vertex,
                    $vertex,
                    $edge,
                    Option<&$crate::moirai_protocol::event::tag::Tag>,
                )> = deduped_arcs
                    .into_iter()
                    .map(|((v1, v2, e), tag)| (v1, v2, e, tag))
                    .collect();

                // MAX enforcement per (source, edge_kind) group
                let mut groups: $crate::HashMap<($vertex, $edge), Vec<usize>> =
                    $crate::HashMap::default();
                for (i, (source, _target, kind, _tag)) in arc_entries.iter().enumerate() {
                    groups
                        .entry((source.clone(), kind.clone()))
                        .or_default()
                        .push(i);
                }

                let mut surviving = vec![true; arc_entries.len()];

                for ((_, kind), indices) in &groups {
                    if indices.is_empty() {
                        continue;
                    }
                    let max = max_edges_for(&arc_entries[indices[0]].0, kind);
                    if indices.len() > max {
                        let mut sorted_indices = indices.clone();
                        sorted_indices.sort_by(|&a, &b| {
                            match (&arc_entries[a].3, &arc_entries[b].3) {
                                (None, None) => ::std::cmp::Ordering::Equal,
                                (None, Some(_)) => ::std::cmp::Ordering::Less,
                                (Some(_), None) => ::std::cmp::Ordering::Greater,
                                (Some(ta), Some(tb)) => P::compare(ta, tb),
                            }
                        });
                        let losers = sorted_indices.len() - max;
                        for &idx in sorted_indices.iter().take(losers) {
                            surviving[idx] = false;
                        }
                    }
                }

                // Add surviving arcs to the graph
                for (i, (v1, v2, e, _)) in arc_entries.iter().enumerate() {
                    if surviving[i]
                        && let (Some(&a), Some(&b)) = (node_index.get(v1), node_index.get(v2))
                    {
                        graph.add_edge(a, b, e.clone());
                    }
                }

                graph
            }
        }
    };
    // Public arm: vertex ids are generated directly from `vertices { ... }`.
    (
        graph: $graph:ident,
        vertex: $vertex:ident,
        edge: $edge:ident,
        arcs_type: $arcs:ident,

        vertices { $( $v:ident ),* $(,)? },

        connections {
            $( $conn:ident : $src:ident -> $tgt:ident ( $ety:path ) [ $min:expr , $max:tt ] ),* $(,)?
        } $(,)?
    ) => {
        $crate::typed_graph!(@generate
            graph: $graph,
            vertex: $vertex,
            edge: $edge,
            arcs_type: $arcs,
            vertices { $( $v ),* },
            connections {
                $( $conn : $src [$src] -> $tgt [$tgt] ( $ety ) [ $min, $max ] ),*
            }
        );
    };

}
