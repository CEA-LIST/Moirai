/// Generates a complete typed graph CRDT from a schema definition.
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arc<S, T, E> {
    pub source: S,
    pub target: T,
    pub kind: E,
}

impl<S, T, E> moirai_protocol::utils::translate_ids::TranslateIds for Arc<S, T, E>
where
    S: moirai_protocol::utils::translate_ids::TranslateIds,
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
pub enum Vertex<T>
where
    T: Debug + Clone + PartialEq + Eq + Hash,
{
    AddVertex { id: T },
    RemoveVertex { id: T },
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
        edge_types {
            $( $edge_ty:ident [ $edge_min:expr , $edge_max:tt ] ),* $(,)?
        },
        connections {
            $( $conn:ident : $src:ident [$src_ty:path] -> $tgt:ident [$tgt_ty:path] ( $ety:ident ) ),* $(,)?
        } $(,)?
    ) => {
        // Generate a vertex struct for each vertex variant, and implement TranslateIds for it.
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

        macro_rules! __typed_graph_min {
            $(
                ($edge_ty) => {
                    $edge_min
                };
            )*
        }

        macro_rules! __typed_graph_max {
            $(
                ($edge_ty) => {
                    $crate::typed_graph!(@max $edge_max)
                };
            )*
        }

        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        enum __TypedGraphEdgeType {
            $( $edge_ty ),*
        }

        // Enum of all vertices
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $vertex {
            $( $v($v) ),*
        }

        // Helper function to extract ObjectPath from any vertex variant
        impl $vertex {
            pub fn vertex_path(&self) -> &$crate::moirai_protocol::state::sink::ObjectPath {
                match self {
                    $( $vertex::$v(id) => &id.0 ),*
                }
            }
        }

        // Implement TranslateIds for the vertex enum by delegating to each variant's implementation
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

        // Enum of all edge types
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $edge {
            $( $conn($ety) ),*
        }

        // Enum of all arcs
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $arcs {
            $( $conn($crate::typed_graph::Arc<$src_ty, $tgt_ty, $ety>) ),*
        }

        // Implement TranslateIds for the arcs enum by delegating to each variant's implementation
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

        // Implement helper methods on the arcs enum to extract source, target, kind, and constraints
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
                    $( $arcs::$conn(_) => __typed_graph_max!($ety) ),*
                }
            }

            pub fn min(&self) -> usize {
                match self {
                    $( $arcs::$conn(_) => __typed_graph_min!($ety) ),*
                }
            }

            pub fn edge_type(&self) -> __TypedGraphEdgeType {
                match self {
                    $( $arcs::$conn(_) => __TypedGraphEdgeType::$ety ),*
                }
            }
        }

        // Main graph operation enum
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

        // Implement TranslateIds for the main graph operation enum by delegating to each variant's implementation
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

        // Helper functions for schema validation and constraints computation
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

        // Helper function to check if a given edge is valid between two vertices and return the corresponding arc if so
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

        // Helper function to get the max edges allowed for a given source vertex and edge kind
        fn max_edges_for(source: &$vertex, kind: &$edge) -> usize {
            match (source, kind) {
                $(
                    ($vertex::$src(_), $edge::$conn(_)) => __typed_graph_max!($ety),
                )*
                _ => usize::MAX,
            }
        }

        // Helper function to get the schema edge type for a given edge
        fn edge_type_of(edge: &$edge) -> __TypedGraphEdgeType {
            match edge {
                $( $edge::$conn(_) => __TypedGraphEdgeType::$ety ),*
            }
        }

        // Helper function to get the min and max constraints for a given source vertex, target vertex, and edge
        fn edge_constraints_for(
            source: &$vertex,
            target: &$vertex,
            edge: &$edge,
        ) -> Option<(usize, usize)> {
            match (source, target, edge) {
                $(
                    ($vertex::$src(_), $vertex::$tgt(_), $edge::$conn(_)) => {
                        Some((__typed_graph_min!($ety), __typed_graph_max!($ety)))
                    },
                )*
                _ => None,
            }
        }

        // Helper function to get the required edge type constraints for a given vertex
        fn required_constraints_for(vertex: &$vertex) -> Vec<(__TypedGraphEdgeType, usize, usize)> {
            let mut constraints = Vec::new();
            let mut seen_edge_types: $crate::HashSet<__TypedGraphEdgeType> = $crate::HashSet::default();
            $(
                if let $vertex::$src(_) = vertex {
                    let edge_type = __TypedGraphEdgeType::$ety;
                    if seen_edge_types.insert(edge_type) {
                        constraints.push((edge_type, __typed_graph_min!($ety), __typed_graph_max!($ety)));
                    }
                }
            )*
            constraints
        }

        // Struct to hold the addable and removable arcs for a given graph state
        #[derive(Debug, Clone)]
        pub struct ArcConstraints {
            pub addable: Vec<$arcs>,
            pub removable: Vec<$arcs>,
        }

        // Function to compute the addable and removable arcs for a given graph state based on the schema constraints
        // Mainly used for the fuzzer
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

                let mut outgoing_by_type: $crate::HashMap<__TypedGraphEdgeType, usize> =
                    $crate::HashMap::default();
                for edge in graph.edges_directed(source_idx, petgraph::Direction::Outgoing) {
                    *outgoing_by_type.entry(edge_type_of(edge.weight())).or_insert(0) += 1;
                }

                for target_idx in graph.node_indices() {
                    if source_idx == target_idx {
                        continue;
                    }
                    let target = &graph[target_idx];

                    for candidate in possible_arcs_between(source, target) {
                        let edge_type = candidate.edge_type();
                        let kind = candidate.kind();
                        let count = outgoing_by_type.get(&edge_type).copied().unwrap_or(0);
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
                        let count = outgoing_by_type
                            .get(&arc.edge_type())
                            .copied()
                            .unwrap_or(0);
                        if count > arc.min() {
                            removable.push(arc);
                        }
                    }
                }
            }

            ArcConstraints { addable, removable }
        }

        // Struct to represent schema violations found during validation
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

        // Function to validate a graph against the schema constraints, returning a list of violations if any are found
        // Mainly used for testing and debugging, but could also be used in the fuzzer to guide generation towards valid graphs
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

                let mut outgoing_by_type: $crate::HashMap<__TypedGraphEdgeType, usize> =
                    $crate::HashMap::default();
                for edge in graph.edges_directed(node_idx, petgraph::Direction::Outgoing) {
                    *outgoing_by_type.entry(edge_type_of(edge.weight())).or_insert(0) += 1;
                }

                for (edge_type, count) in &outgoing_by_type {
                    let max = graph
                        .edges_directed(node_idx, petgraph::Direction::Outgoing)
                        .find_map(|e| {
                            if edge_type_of(e.weight()) == *edge_type {
                                let target = &graph[e.target()];
                                edge_constraints_for(source, target, e.weight()).map(|(_, m)| m)
                            } else {
                                None
                            }
                        });

                    if let Some(max) = max
                        && *count > max
                    {
                        let edge_kind = graph
                            .edges_directed(node_idx, petgraph::Direction::Outgoing)
                            .find(|e| edge_type_of(e.weight()) == *edge_type)
                            .map(|e| e.weight().clone())
                            .unwrap();
                        violations.push(SchemaViolation::ExceedsMax {
                            source: source.clone(),
                            edge_kind,
                            count: *count,
                            max,
                        });
                    }
                }

                for (edge_type, min, _max) in required_constraints_for(source) {
                    if min > 0 {
                        let count = outgoing_by_type.get(&edge_type).copied().unwrap_or(0);
                        if count < min {
                            let edge_kind = graph
                                .edges_directed(node_idx, petgraph::Direction::Outgoing)
                                .find(|e| edge_type_of(e.weight()) == edge_type)
                                .map(|e| e.weight().clone())
                                .unwrap_or_else(|| match source {
                                    $(
                                        $vertex::$src(_) => $edge::$conn($ety),
                                    )*
                                    _ => unreachable!(),
                                });
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

        // Implement the PureCRDT trait for the graph operation enum, defining the CRDT behavior and how to execute queries to get the current graph state
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
                            prefix.is_prefix_of(id.vertex_path())
                        }
                        ($graph::AddArc(arc), $graph::DeleteSubtree { prefix }) => {
                            let source = arc.source();
                            let target = arc.target();
                            prefix.is_prefix_of(source.vertex_path())
                                || prefix.is_prefix_of(target.vertex_path())
                        }
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
                        graph.node_weights().any(|node| prefix.is_prefix_of(node.vertex_path()))
                    },
                    $graph::RemoveArc(arc) => {
                        let source = arc.source();
                        let target = arc.target();
                        let kind = arc.kind();
                        let edge_type = arc.edge_type();

                        let idx_1 = graph
                            .node_indices()
                            .find(|&idx| graph.node_weight(idx) == Some(&source));
                        let idx_2 = graph
                            .node_indices()
                            .find(|&idx| graph.node_weight(idx) == Some(&target));
                        // if both vertices exist but the specific edge doesn't exist,
                        // then it's not enabled (can't remove an edge that isn't there)
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
                            .filter(|edge| edge_type_of(edge.weight()) == edge_type)
                            .count();
                        // if the edge exists, then we can remove it as long as it doesn't violate the min constraint
                        count > arc.min()
                    }
                    $graph::AddArc(arc) => {
                        let source = arc.source();
                        let target = arc.target();
                        let edge_type = arc.edge_type();

                        // if either vertex doesn't exist, then it's not enabled
                        // (can't add an edge if one of the endpoints isn't there)
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
                            .filter(|edge| edge_type_of(edge.weight()) == edge_type)
                            .count();

                        // if both vertices exist, then we can add the edge as long as it doesn't violate the max constraint
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

                // First add all vertices
                // TODO: if the backend log is ordered, we could add vertices and arcs in one pass
                for (op, _) in &tagged_ops {
                    if let $graph::AddVertex { id } = op
                        && !node_index.contains_key(id)
                    {
                        let idx = graph.add_node(id.clone());
                        node_index.insert(id.clone(), idx);
                    }
                }

                // Collect deduplicated arc candidates
                // TODO: if the backend log is ordered, we could add arcs in one pass and skip this deduplication step
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
                        } else {
                            // This case happens when removeVertex(v1) || addArc(v1, v2, e)!
                            // This is normal :) if the vertex is added again, the arc will be reconsidered for addition at that time,
                            // and if not, then it shouldn't be in the graph anyway so we can just ignore this arc addition
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

                // MAX enforcement per (source, edge_type) group
                let mut groups: $crate::HashMap<($vertex, __TypedGraphEdgeType), Vec<usize>> =
                    $crate::HashMap::default();
                for (i, (source, _target, kind, _tag)) in arc_entries.iter().enumerate() {
                    groups
                        .entry((source.clone(), edge_type_of(kind)))
                        .or_default()
                        .push(i);
                }

                // Determine surviving arcs based on MAX constraints and tags
                let mut surviving = vec![true; arc_entries.len()];

                for ((_source, _family), indices) in &groups {
                    if indices.is_empty() {
                        continue;
                    }
                    let max = max_edges_for(&arc_entries[indices[0]].0, &arc_entries[indices[0]].2);
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
    // Public arm: block-style schema definition.
    (
        types {
            graph = $graph:ident,
            vertex_kind = $vertex:ident,
            edge_kind = $edge:ident,
            arc_kind = $arcs:ident $(,)?
        },

        vertices {
            $( $v:ident ),* $(,)?
        },

        edges {
            $( $edge_ty:ident [ $edge_min:expr , $edge_max:tt ] ),* $(,)?
        },

        arcs {
            $( $conn:ident : $src:ident -> $tgt:ident ( $ety:ident ) ),* $(,)?
        } $(,)?
    ) => {
        $crate::typed_graph!(@generate
            graph: $graph,
            vertex: $vertex,
            edge: $edge,
            arcs_type: $arcs,
            vertices { $( $v ),* },
            edge_types {
                $( $edge_ty [ $edge_min, $edge_max ] ),*
            },
            connections {
                $( $conn : $src [$src] -> $tgt [$tgt] ( $ety ) ),*
            }
        );
    };
}

#[macro_export]
macro_rules! type_graph {
    ($($tt:tt)*) => {
        $crate::typed_graph! { $($tt)* }
    };
}
