/// Generates a complete typed graph CRDT from a schema definition.
use std::fmt::{Debug, Display};
use std::hash::Hash;

#[cfg(feature = "test_utils")]
use deepsize::{Context, DeepSizeOf};

//* ARC *//

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Arc<S, T, E> {
    pub source: S,
    pub target: T,
    pub kind: E,
}

//* VERTEX *//

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
        ::std::primitive::usize::MAX
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
        //* VERTEX TYPES *//
        $(
            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub struct $v(pub $crate::moirai_protocol::state::object_path::ObjectPath);

            #[cfg(feature = "test_utils")]
            impl ::deepsize::DeepSizeOf for $v {
                fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                    ::deepsize::DeepSizeOf::deep_size_of_children(&self.0, context)
                }
            }

        )*

        //* SET OF VERTEX TYPES *//

        // Enum of all vertices
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $vertex {
            $( $v($v) ),*
        }

        #[cfg(feature = "test_utils")]
        impl ::deepsize::DeepSizeOf for $vertex {
            fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                match self {
                    $( Self::$v(id) => ::deepsize::DeepSizeOf::deep_size_of_children(id, context) ),*
                }
            }
        }

        // Helper function to extract ObjectPath from any vertex variant
        impl $vertex {
            pub fn vertex_path(&self) -> &$crate::moirai_protocol::state::object_path::ObjectPath {
                match self {
                    $( $vertex::$v(id) => &id.0 ),*
                }
            }
        }

        //* EDGE TYPES *//

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

        #[cfg(feature = "test_utils")]
        impl ::deepsize::DeepSizeOf for __TypedGraphEdgeType {
            fn deep_size_of_children(&self, _context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                0
            }
        }

        //* SET OF EDGE TYPES *//

        // Enum of all edge types
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $edge {
            $( $conn($ety) ),*
        }

        #[cfg(feature = "test_utils")]
        impl ::deepsize::DeepSizeOf for $edge {
            fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                match self {
                    $( Self::$conn(edge) => ::deepsize::DeepSizeOf::deep_size_of_children(edge, context) ),*
                }
            }
        }

        //* SET OF ARC TYPES *//

        // Enum of all arcs
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $arcs {
            $( $conn($crate::typed_graph::Arc<$src_ty, $tgt_ty, $ety>) ),*
        }

        // Implement helper methods on the arcs enum to extract source, target, kind, and constraints
        impl $arcs {
            pub fn source(&self) -> $vertex {
                match self {
                    $( $arcs::$conn(arc) => $vertex::$src(::std::clone::Clone::clone(&arc.source)) ),*
                }
            }

            pub fn target(&self) -> $vertex {
                match self {
                    $( $arcs::$conn(arc) => $vertex::$tgt(::std::clone::Clone::clone(&arc.target)) ),*
                }
            }

            pub fn kind(&self) -> $edge {
                match self {
                    $( $arcs::$conn(arc) => $edge::$conn(::std::clone::Clone::clone(&arc.kind)) ),*
                }
            }

            pub fn max(&self) -> ::std::primitive::usize {
                match self {
                    $( $arcs::$conn(_) => __typed_graph_max!($ety) ),*
                }
            }

            pub fn min(&self) -> ::std::primitive::usize {
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

        //* TYPE GRAPH *//

        // Main graph operation enum
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub enum $graph<P> {
            AddVertex { id: $vertex },
            RemoveVertex { id: $vertex },
            DeleteSubtree { prefix: $crate::moirai_protocol::state::object_path::ObjectPath },
            AddArc($arcs),
            RemoveArc($arcs),
            #[doc(hidden)]
            __Marker(::std::convert::Infallible, ::std::marker::PhantomData<P>),
        }

        //* HELPER FUNCTIONS */

        // Helper functions for schema validation and constraints computation
        fn possible_arcs_between(
            source: &$vertex,
            target: &$vertex,
        ) -> ::std::vec::Vec<$arcs> {
            let mut result = ::std::vec::Vec::new();
            $(
                if let ($vertex::$src(s), $vertex::$tgt(t)) = (source, target) {
                    result.push($arcs::$conn($crate::typed_graph::Arc {
                        source: ::std::clone::Clone::clone(s),
                        target: ::std::clone::Clone::clone(t),
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
        ) -> ::std::option::Option<$arcs> {
            match (source, target, edge) {
                $(
                    ($vertex::$src(s), $vertex::$tgt(t), $edge::$conn(e)) => {
                        ::std::option::Option::Some($arcs::$conn($crate::typed_graph::Arc {
                            source: ::std::clone::Clone::clone(s),
                            target: ::std::clone::Clone::clone(t),
                            kind: ::std::clone::Clone::clone(e),
                        }))
                    }
                )*
                _ => ::std::option::Option::None,
            }
        }

        // Helper function to get the max edges allowed for a given source vertex and edge kind
        fn max_edges_for(source: &$vertex, kind: &$edge) -> ::std::primitive::usize {
            match (source, kind) {
                $(
                    ($vertex::$src(_), $edge::$conn(_)) => __typed_graph_max!($ety),
                )*
                _ => ::std::primitive::usize::MAX,
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
        ) -> ::std::option::Option<(
            ::std::primitive::usize,
            ::std::primitive::usize,
        )> {
            match (source, target, edge) {
                $(
                    ($vertex::$src(_), $vertex::$tgt(_), $edge::$conn(_)) => {
                        ::std::option::Option::Some((
                            __typed_graph_min!($ety),
                            __typed_graph_max!($ety),
                        ))
                    },
                )*
                _ => ::std::option::Option::None,
            }
        }

        // Helper function to get the required edge type constraints for a given vertex
        fn required_constraints_for(
            vertex: &$vertex,
        ) -> ::std::vec::Vec<(
            __TypedGraphEdgeType,
            ::std::primitive::usize,
            ::std::primitive::usize,
        )> {
            let mut constraints = ::std::vec::Vec::new();
            let mut seen_edge_types: $crate::HashSet<__TypedGraphEdgeType> =
                ::std::default::Default::default();
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
            pub addable: ::std::vec::Vec<$arcs>,
            pub removable: ::std::vec::Vec<$arcs>,
        }

        // Function to compute the addable and removable arcs for a given graph state based on the schema constraints
        // Mainly used for the fuzzer
        pub fn compute_arc_constraints(
            graph: &::petgraph::graph::DiGraph<$vertex, $edge>,
        ) -> ArcConstraints {
            use ::petgraph::visit::EdgeRef;

            let mut addable = ::std::vec::Vec::new();
            let mut removable = ::std::vec::Vec::new();

            let existing_edges: $crate::HashSet<($vertex, $vertex, $edge)> = graph
                .edge_indices()
                .filter_map(|ei| {
                    let (si, ti) = graph.edge_endpoints(ei)?;
                    ::std::option::Option::Some((
                        ::std::clone::Clone::clone(&graph[si]),
                        ::std::clone::Clone::clone(&graph[ti]),
                        ::std::clone::Clone::clone(&graph[ei]),
                    ))
                })
                .collect();

            for source_idx in graph.node_indices() {
                let source = &graph[source_idx];

                let mut outgoing_by_type: $crate::HashMap<
                    __TypedGraphEdgeType,
                    ::std::primitive::usize,
                > = ::std::default::Default::default();
                for edge in graph.edges_directed(source_idx, ::petgraph::Direction::Outgoing) {
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
                            && !existing_edges.contains(&(
                                ::std::clone::Clone::clone(source),
                                ::std::clone::Clone::clone(target),
                                kind,
                            ))
                        {
                            addable.push(candidate);
                        }
                    }
                }

                for edge in graph.edges_directed(source_idx, ::petgraph::Direction::Outgoing) {
                    let target = &graph[edge.target()];
                    let kind = edge.weight();

                    if let ::std::option::Option::Some(arc) =
                        arc_from_vertices_and_edge(source, target, kind)
                    {
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
                count: ::std::primitive::usize,
                max: ::std::primitive::usize,
            },
            BelowMin {
                source: $vertex,
                edge_kind: $edge,
                count: ::std::primitive::usize,
                min: ::std::primitive::usize,
            },
        }

        impl ::std::fmt::Display for SchemaViolation {
            fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
                match self {
                    SchemaViolation::InvalidEdge { source, target, edge } => ::std::write!(
                        f, "Invalid edge {:?} between {:?} and {:?}", edge, source, target
                    ),
                    SchemaViolation::ExceedsMax { source, edge_kind, count, max } => ::std::write!(
                        f, "Vertex {:?} has {} outgoing {:?} edges, exceeding max of {}",
                        source, count, edge_kind, max
                    ),
                    SchemaViolation::BelowMin { source, edge_kind, count, min } => ::std::write!(
                        f, "Vertex {:?} has {} outgoing {:?} edges, below min of {}",
                        source, count, edge_kind, min
                    ),
                }
            }
        }

        // Function to validate a graph against the schema constraints, returning a list of violations if any are found
        // Mainly used for testing and debugging, but could also be used in the fuzzer to guide generation towards valid graphs
        pub fn validate_schema(
            graph: &::petgraph::graph::DiGraph<$vertex, $edge>,
        ) -> ::std::result::Result<(), ::std::vec::Vec<SchemaViolation>> {
            use ::petgraph::visit::EdgeRef;

            let mut violations = ::std::vec::Vec::new();

            for edge_idx in graph.edge_indices() {
                if let ::std::option::Option::Some((si, ti)) = graph.edge_endpoints(edge_idx) {
                    let source = &graph[si];
                    let target = &graph[ti];
                    let edge = &graph[edge_idx];

                    if edge_constraints_for(source, target, edge).is_none() {
                        violations.push(SchemaViolation::InvalidEdge {
                            source: ::std::clone::Clone::clone(source),
                            target: ::std::clone::Clone::clone(target),
                            edge: ::std::clone::Clone::clone(edge),
                        });
                    }
                }
            }

            for node_idx in graph.node_indices() {
                let source = &graph[node_idx];

                let mut outgoing_by_type: $crate::HashMap<
                    __TypedGraphEdgeType,
                    ::std::primitive::usize,
                > = ::std::default::Default::default();
                for edge in graph.edges_directed(node_idx, ::petgraph::Direction::Outgoing) {
                    *outgoing_by_type.entry(edge_type_of(edge.weight())).or_insert(0) += 1;
                }

                for (edge_type, count) in &outgoing_by_type {
                    let max = graph
                        .edges_directed(node_idx, ::petgraph::Direction::Outgoing)
                        .find_map(|e| {
                            if edge_type_of(e.weight()) == *edge_type {
                                let target = &graph[e.target()];
                                edge_constraints_for(source, target, e.weight()).map(|(_, m)| m)
                            } else {
                                ::std::option::Option::None
                            }
                        });

                    if let ::std::option::Option::Some(max) = max
                        && *count > max
                    {
                        let edge_kind = graph
                            .edges_directed(node_idx, ::petgraph::Direction::Outgoing)
                            .find(|e| edge_type_of(e.weight()) == *edge_type)
                            .map(|e| ::std::clone::Clone::clone(e.weight()))
                            .unwrap();
                        violations.push(SchemaViolation::ExceedsMax {
                            source: ::std::clone::Clone::clone(source),
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
                                .edges_directed(node_idx, ::petgraph::Direction::Outgoing)
                                .find(|e| edge_type_of(e.weight()) == edge_type)
                                .map(|e| ::std::clone::Clone::clone(e.weight()))
                                .unwrap_or_else(|| match source {
                                    $(
                                        $vertex::$src(_) => $edge::$conn($ety),
                                    )*
                                    _ => ::std::unreachable!(),
                                });
                            violations.push(SchemaViolation::BelowMin {
                                source: ::std::clone::Clone::clone(source),
                                edge_kind,
                                count,
                                min,
                            });
                        }
                    }
                }
            }

            if violations.is_empty() {
                ::std::result::Result::Ok(())
            } else {
                ::std::result::Result::Err(violations)
            }
        }

        // Implement the ReplicatedDataType trait for the graph operation enum, defining the CRDT behavior and how to execute queries to get the current graph state
        impl<P> $crate::moirai_protocol::crdt::replicated_data_type::ReplicatedDataType for $graph<P>
        where
            P: $crate::moirai_protocol::crdt::policy::Policy,
        {
            type Value = ::petgraph::graph::DiGraph<$vertex, $edge>;
            type StableState = ::std::vec::Vec<Self>;
            type Rejection = $crate::typed_graph::TypedGraphRejection;

            const DISABLE_R_WHEN_R: ::std::primitive::bool = false;
            const DISABLE_R_WHEN_NOT_R: ::std::primitive::bool = false;
            // TODO: find a way to enable stabilize for this CRDT
            const DISABLE_STABILIZE: ::std::primitive::bool = true;

            fn redundant_itself<'a>(
                new_tagged_op: &$crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
                _stable: &Self::StableState,
                _unstable: impl ::std::iter::Iterator<
                    Item = &'a $crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
                >,
            ) -> ::std::primitive::bool
            where
                Self: 'a,
            {
                match new_tagged_op.op() {
                    $graph::AddVertex { .. } | $graph::AddArc(_) => false,
                    $graph::RemoveVertex { .. }
                    | $graph::DeleteSubtree { .. }
                    | $graph::RemoveArc(_) => true,
                    $graph::__Marker(_, _) => ::std::unreachable!(),
                }
            }

            fn redundant_by_when_redundant(
                old_op: &Self,
                _old_tag: ::std::option::Option<&$crate::moirai_protocol::event::tag::Tag>,
                is_conc: ::std::primitive::bool,
                new_tagged_op: &$crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
            ) -> ::std::primitive::bool {
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
                old_tag: ::std::option::Option<&$crate::moirai_protocol::event::tag::Tag>,
                is_conc: ::std::primitive::bool,
                new_tagged_op: &$crate::moirai_protocol::event::tagged_op::TaggedOp<Self>,
            ) -> ::std::primitive::bool {
                <Self as $crate::moirai_protocol::crdt::replicated_data_type::ReplicatedDataType>::redundant_by_when_redundant(
                    old_op,
                    old_tag,
                    is_conc,
                    new_tagged_op,
                )
            }

        }

        impl<P, U> $crate::moirai_protocol::crdt::replicated_data_type::UsesUnstableService<U> for $graph<P>
        where
            P: $crate::moirai_protocol::crdt::policy::Policy,
            U: $crate::moirai_protocol::state::unstable_state::IsUnstableCore<Self>,
        {
            fn is_enabled(
                op: &Self,
                stable: &Self::StableState,
                unstable: &U,
            ) -> ::std::result::Result<(), Self::Rejection> {
                use $crate::moirai_protocol::crdt::eval::Eval;
                use $crate::moirai_protocol::crdt::query::Read;

                let graph = Self::execute_query(Read::new(), stable, unstable);
                match op {
                    $graph::AddVertex { .. } => ::std::result::Result::Ok(()),
                    $graph::RemoveVertex { id } => graph
                        .node_weights()
                        .any(|node| node == id)
                        .then_some(())
                        .ok_or($crate::typed_graph::TypedGraphRejection::MissingVertex),
                    $graph::DeleteSubtree { prefix } => {
                        graph
                            .node_weights()
                            .any(|node| prefix.is_prefix_of(node.vertex_path()))
                            .then_some(())
                            .ok_or($crate::typed_graph::TypedGraphRejection::MissingVertex)
                    },
                    $graph::RemoveArc(arc) => {
                        let source = arc.source();
                        let target = arc.target();
                        let kind = arc.kind();
                        let edge_type = arc.edge_type();

                        let source_idx = graph
                            .node_indices()
                            .find(|&idx| {
                                graph.node_weight(idx) == ::std::option::Option::Some(&source)
                            })
                            .ok_or($crate::typed_graph::TypedGraphRejection::MissingVertex)?;
                        let target_idx = graph
                            .node_indices()
                            .find(|&idx| {
                                graph.node_weight(idx) == ::std::option::Option::Some(&target)
                            })
                            .ok_or($crate::typed_graph::TypedGraphRejection::MissingVertex)?;
                        // if both vertices exist but the specific edge doesn't exist,
                        // then it's not enabled (can't remove an edge that isn't there)
                        if !graph
                            .edges_connecting(source_idx, target_idx)
                            .any(|edge| edge.weight() == &kind)
                        {
                            return ::std::result::Result::Err(
                                $crate::typed_graph::TypedGraphRejection::MissingArc,
                            );
                        }

                        let count = graph
                            .edges_directed(source_idx, ::petgraph::Direction::Outgoing)
                            .filter(|edge| edge_type_of(edge.weight()) == edge_type)
                            .count();
                        // if the edge exists, then we can remove it as long as it doesn't violate the min constraint
                        (count > arc.min())
                            .then_some(())
                            .ok_or($crate::typed_graph::TypedGraphRejection::MinCardinality)
                    }
                    $graph::AddArc(arc) => {
                        let source = arc.source();
                        let target = arc.target();
                        let edge_type = arc.edge_type();

                        // if either vertex doesn't exist, then it's not enabled
                        // (can't add an edge if one of the endpoints isn't there)
                        let source_idx = graph
                            .node_indices()
                            .find(|&i| graph[i] == source)
                            .ok_or($crate::typed_graph::TypedGraphRejection::MissingVertex)?;
                        graph
                            .node_indices()
                            .find(|&i| graph[i] == target)
                            .ok_or($crate::typed_graph::TypedGraphRejection::MissingVertex)?;

                        let count = graph
                            .edges_directed(source_idx, ::petgraph::Direction::Outgoing)
                            .filter(|edge| edge_type_of(edge.weight()) == edge_type)
                            .count();

                        // if both vertices exist, then we can add the edge as long as it doesn't violate the max constraint
                        (count < arc.max())
                            .then_some(())
                            .ok_or($crate::typed_graph::TypedGraphRejection::MaxCardinality)
                    }
                    $graph::__Marker(_, _) => ::std::unreachable!(),
                }
            }
        }

        impl<P, U> $crate::moirai_protocol::crdt::eval::Eval<
            $crate::moirai_protocol::crdt::query::Read<
                <Self as $crate::moirai_protocol::crdt::replicated_data_type::ReplicatedDataType>::Value
            >,
            U
        > for $graph<P>
        where
            P: $crate::moirai_protocol::crdt::policy::Policy,
            U: $crate::moirai_protocol::state::unstable_state::IsUnstableCore<Self> ,
        {
            fn execute_query(
                _q: $crate::moirai_protocol::crdt::query::Read<<$graph<P> as $crate::moirai_protocol::crdt::replicated_data_type::ReplicatedDataType>::Value>,
                stable: &<Self as $crate::moirai_protocol::crdt::replicated_data_type::ReplicatedDataType>::StableState,
                unstable: &U) -> <$crate::moirai_protocol::crdt::query::Read<<$graph<P> as $crate::moirai_protocol::crdt::replicated_data_type::ReplicatedDataType>::Value> as $crate::moirai_protocol::crdt::query::QueryOperation>::Response
            {
                let tagged_ops: ::std::vec::Vec<(
                    &Self,
                    ::std::option::Option<&$crate::moirai_protocol::event::tag::Tag>,
                )> = stable
                    .iter()
                    .map(|op| (op, ::std::option::Option::None))
                    .chain(unstable.iter().map(|t| {
                        (t.op(), ::std::option::Option::Some(t.tag()))
                    }))
                    .collect();

                let mut graph = ::petgraph::graph::DiGraph::new();
                let mut node_index: $crate::HashMap<$vertex, _> =
                    ::std::default::Default::default();

                // First add all vertices
                // TODO: if the backend log is ordered, we could add vertices and arcs in one pass
                for (op, _) in &tagged_ops {
                    if let $graph::AddVertex { id } = op
                        && !node_index.contains_key(id)
                    {
                        let idx = graph.add_node(::std::clone::Clone::clone(id));
                        node_index.insert(::std::clone::Clone::clone(id), idx);
                    }
                }

                // Collect deduplicated arc candidates
                // TODO: if the backend log is ordered, we could add arcs in one pass and skip this deduplication step
                let mut deduped_arcs: $crate::HashMap<
                    ($vertex, $vertex, $edge),
                    ::std::option::Option<&$crate::moirai_protocol::event::tag::Tag>,
                > = ::std::default::Default::default();

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
                                        (::std::option::Option::None, ::std::option::Option::None) => false,
                                        (::std::option::Option::None, ::std::option::Option::Some(_)) => true,
                                        (::std::option::Option::Some(_), ::std::option::Option::None) => false,
                                        (::std::option::Option::Some(old_tag), ::std::option::Option::Some(new_tag)) => {
                                            <P as $crate::moirai_protocol::crdt::policy::Policy>::compare(old_tag, new_tag)
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

                let mut arc_entries: ::std::vec::Vec<(
                    $vertex,
                    $vertex,
                    $edge,
                    ::std::option::Option<&$crate::moirai_protocol::event::tag::Tag>,
                )> = deduped_arcs
                    .into_iter()
                    .map(|((v1, v2, e), tag)| (v1, v2, e, tag))
                    .collect();

                // MAX enforcement per (source, edge_type) group
                // We assume that their can exist at most one arc of a given type between a given source and target,
                // so we only need to enforce MAX constraints per (source, edge_type) group
                let mut groups: $crate::HashMap<
                    ($vertex, __TypedGraphEdgeType),
                    ::std::vec::Vec<::std::primitive::usize>,
                > = ::std::default::Default::default();
                for (i, (source, _target, kind, _tag)) in arc_entries.iter().enumerate() {
                    groups
                        .entry((::std::clone::Clone::clone(source), edge_type_of(kind)))
                        .or_default()
                        .push(i);
                }

                // Determine surviving arcs based on MAX constraints and tags
                let mut surviving = ::std::vec![true; arc_entries.len()];

                for ((_source, _family), indices) in &groups {
                    if indices.is_empty() {
                        continue;
                    }
                    let max = max_edges_for(&arc_entries[indices[0]].0, &arc_entries[indices[0]].2);
                    if indices.len() > max {
                        let mut sorted_indices = ::std::clone::Clone::clone(indices);
                        sorted_indices.sort_by(|&a, &b| {
                            match (&arc_entries[a].3, &arc_entries[b].3) {
                                (::std::option::Option::None, ::std::option::Option::None) => ::std::cmp::Ordering::Equal,
                                (::std::option::Option::None, ::std::option::Option::Some(_)) => ::std::cmp::Ordering::Less,
                                (::std::option::Option::Some(_), ::std::option::Option::None) => ::std::cmp::Ordering::Greater,
                                (::std::option::Option::Some(ta), ::std::option::Option::Some(tb)) => {
                                    <P as $crate::moirai_protocol::crdt::policy::Policy>::compare(ta, tb)
                                },
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
                        && let (
                            ::std::option::Option::Some(&a),
                            ::std::option::Option::Some(&b),
                        ) = (node_index.get(v1), node_index.get(v2))
                    {
                        graph.add_edge(a, b, ::std::clone::Clone::clone(e));
                    }
                }

                graph
            }
        }

        //* DEEP SIZE */

        #[cfg(feature = "test_utils")]
        impl<P> ::deepsize::DeepSizeOf for $graph<P> {
            fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                match self {
                    Self::AddVertex { id } | Self::RemoveVertex { id } => {
                        ::deepsize::DeepSizeOf::deep_size_of_children(id, context)
                    }
                    Self::DeleteSubtree { prefix } => {
                        ::deepsize::DeepSizeOf::deep_size_of_children(prefix, context)
                    },
                    Self::AddArc(arc) | Self::RemoveArc(arc) => {
                        ::deepsize::DeepSizeOf::deep_size_of_children(arc, context)
                    }
                    Self::__Marker(never, _) => match *never {},
                }
            }
        }

        #[cfg(feature = "test_utils")]
        impl ::deepsize::DeepSizeOf for $arcs {
            fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> ::std::primitive::usize {
                match self {
                    $( Self::$conn(arc) => ::deepsize::DeepSizeOf::deep_size_of_children(arc, context) ),*
                }
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

//* REJECTION *//

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypedGraphRejection {
    MissingVertex,
    MissingArc,
    MinCardinality,
    MaxCardinality,
}

impl Display for TypedGraphRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingVertex => write!(f, "required vertex does not exist"),
            Self::MissingArc => write!(f, "required arc does not exist"),
            Self::MinCardinality => {
                write!(
                    f,
                    "operation would violate a minimum cardinality constraint"
                )
            }
            Self::MaxCardinality => {
                write!(
                    f,
                    "operation would violate a maximum cardinality constraint"
                )
            }
        }
    }
}

//* DEEP SIZE *//

#[cfg(feature = "test_utils")]
impl<S, T, E> DeepSizeOf for Arc<S, T, E>
where
    S: DeepSizeOf,
    T: DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        // Edge kinds are schema marker values. Only the endpoint identifiers can
        // carry heap-backed state, such as interned object paths.
        self.source.deep_size_of_children(context) + self.target.deep_size_of_children(context)
    }
}

#[cfg(feature = "test_utils")]
impl<T> DeepSizeOf for Vertex<T>
where
    T: Debug + Clone + PartialEq + Eq + Hash + ::deepsize::DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut ::deepsize::Context) -> usize {
        match self {
            Self::AddVertex { id } | Self::RemoveVertex { id } => id.deep_size_of_children(context),
        }
    }
}
