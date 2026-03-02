use moirai_protocol::{
    crdt::{eval::Eval, pure_crdt::PureCRDT, query::QueryOperation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::IsUnstableState,
};
use petgraph::graph::DiGraph;
use std::fmt::Debug;

pub trait NodeKind: Debug {}
pub trait EdgeKind: Debug {}

pub trait CanConnectTo<Target: NodeKind, Edge: EdgeKind> {
    const MIN: usize = 0;
    const MAX: Option<usize> = None;
}

pub enum Patate {
    A(Box<dyn NodeKind + Debug>),
}

#[derive(Clone, Debug)]
pub enum TypedGraph<S, T, E>
where
    S: NodeKind + CanConnectTo<T, E>,
    T: NodeKind,
    E: EdgeKind,
{
    AddVertex { id: S },
    RemoveVertex { id: S },
    AddArc { source: S, target: T, kind: E },
    RemoveArc { source: S, target: T, kind: E },
}

impl<V, E> PureCRDT for TypedGraph<V, V, E>
where
    V: NodeKind + CanConnectTo<V, E>,
    E: EdgeKind,
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

#[cfg(test)]
mod tests {
    use crate::{
        graph::typed_graph::{CanConnectTo, EdgeKind, NodeKind, TypedGraph},
        utils::membership::twins_log,
    };

    #[derive(Clone, Debug)]
    struct BehaviorTree(u8);
    impl NodeKind for BehaviorTree {}

    #[derive(Clone, Debug)]
    struct SubTree(u8);
    impl NodeKind for SubTree {}

    #[derive(Clone, Debug)]
    struct BlackBoard(u8);
    impl NodeKind for BlackBoard {}

    #[derive(Clone, Debug)]
    struct BlackBoardEntry(u8);
    impl NodeKind for BlackBoardEntry {}

    #[derive(Clone, Debug)]
    struct Entries;
    impl EdgeKind for Entries {}

    #[derive(Clone, Debug)]
    struct Tree;
    impl EdgeKind for Tree {}

    impl CanConnectTo<BehaviorTree, Tree> for SubTree {
        const MAX: Option<usize> = Some(1);
        const MIN: usize = 1;
    }

    impl CanConnectTo<BlackBoardEntry, Entries> for BlackBoard {
        const MAX: Option<usize> = None;
        const MIN: usize = 0;
    }

    // #[test]
    // fn new_typed_graph() {
    //     type Test = dyn NodeKind + CanConnectTo<dyn NodeKind, dyn EdgeKind>;
    //     let (mut replica_a, mut replica_b) = twins_log::<TypedGraph>();
    // }
}
