use moirai_protocol::{
    crdt::{eval::Eval, pure_crdt::PureCRDT, query::QueryOperation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
};
use petgraph::graph::DiGraph;
use std::fmt::Debug;

// pub trait NodeKind: Debug {}
// pub trait EdgeKind: Debug {}

// pub trait CanConnectTo<Target: NodeKind, Edge: EdgeKind> {
//     const MIN: usize = 0;
//     const MAX: Option<usize> = None;
// }

// pub trait TypedGraphSchema {
//     type Vertex: Debug + Clone;
//     type Edge: Debug + Clone;
// }

pub trait Schema<V, E> {
    fn can_connect(source: &V, target: &V, edge: &E) -> bool;
}

#[derive(Clone, Debug)]
pub enum TypedGraph<V, E> {
    AddVertex { id: V },
    RemoveVertex { id: V },
    AddArc { source: V, target: V, kind: E },
    RemoveArc { source: V, target: V, kind: E },
}

impl<V, E, S> PureCRDT for TypedGraph<V, E>
where
    V: Debug + Clone,
    E: Debug + Clone,
    S: Schema<V, E>,
{
    type Value = DiGraph<V, E>;
    type StableState = Vec<Self>;

    const DISABLE_R_WHEN_R: bool = false;
    const DISABLE_R_WHEN_NOT_R: bool = false;
    const DISABLE_STABILIZE: bool = false;

    fn redundant_itself<'a>(
        _new_tagged_op: &TaggedOp<Self>,
        _stable: &Self::StableState,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self>>,
    ) -> bool
    where
        Self: 'a,
    {
        false
    }

    fn redundant_by_when_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self>,
    ) -> bool {
        false
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
        _op: &Self,
        _stable: &Self::StableState,
        _unstable: &impl IsUnstableState<Self>,
    ) -> bool {
        true
    }
}

// #[cfg(test)]
// mod tests {
//     use super::{CanConnectTo, EdgeKind, NodeKind, TypedGraph, TypedGraphSchema};

//     #[derive(Clone, Debug)]
//     struct BehaviorTree(u8);
//     impl NodeKind for BehaviorTree {}

//     #[derive(Clone, Debug)]
//     struct SubTree(u8);
//     impl NodeKind for SubTree {}

//     #[derive(Clone, Debug)]
//     struct Tree;
//     impl EdgeKind for Tree {}

//     impl CanConnectTo<BehaviorTree, Tree> for SubTree {
//         const MAX: Option<usize> = Some(1);
//         const MIN: usize = 1;
//     }

//     #[derive(Clone, Debug)]
//     enum Vertex {
//         BehaviorTree(BehaviorTree),
//         SubTree(SubTree),
//     }

//     #[derive(Clone, Debug)]
//     enum Edge {
//         Tree(Tree),
//     }

//     impl From<BehaviorTree> for Vertex {
//         fn from(value: BehaviorTree) -> Self {
//             Self::BehaviorTree(value)
//         }
//     }

//     impl From<SubTree> for Vertex {
//         fn from(value: SubTree) -> Self {
//             Self::SubTree(value)
//         }
//     }

//     impl From<Tree> for Edge {
//         fn from(value: Tree) -> Self {
//             Self::Tree(value)
//         }
//     }

//     struct BtSchema;

//     impl TypedGraphSchema for BtSchema {
//         type Vertex = Vertex;
//         type Edge = Edge;
//     }

//     #[test]
//     fn add_arc_checked_enforces_schema_and_connectivity_at_compile_time() {
//         let op = TypedGraph::<Vertex, Edge>::add_arc_checked::<BtSchema, _, _, _>(
//             SubTree(1),
//             BehaviorTree(2),
//             Tree,
//         );

//         match op {
//             TypedGraph::AddArc { .. } => {}
//             _ => panic!("expected AddArc"),
//         }
//     }
// }
