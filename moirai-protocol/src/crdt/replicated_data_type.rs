use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    marker::PhantomData,
};

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    crdt::{eval::Eval, query::QueryOperation, redundancy::RedundancyRelation},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::{IsUnstableCore, IsUnstablePrune},
};

pub trait Precondition<U>: ReplicatedDataType {
    fn is_enabled(
        stable: &Stable<Self>,
        unstable: &U,
        update: &Self::Update,
    ) -> Result<(), Self::Rejection>;
}

pub trait ReplicatedDataType: Sized {
    type Update: Clone + Debug;
    type Stable: Sequential<Update = Self::Update>;
    type Rejection: Debug + Display;

    // TODO: is it really useful?
    fn eval<Q, U>(stable: &Stable<Self>, unstable: &U, query: &Q) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<Stable<Self>, U, Q>,
    {
        Self::execute_query(stable, unstable, query)
    }
}

pub type Stable<R> = <R as ReplicatedDataType>::Stable;

/// Sequential behavior implemented directly by the stored state.
pub trait Sequential: Default {
    type Update: Clone + Debug;

    fn apply(&mut self, update: &Self::Update);

    /// Whether the state contains no retained contribution.
    fn is_default(&self) -> bool;
}

pub trait SequentialEval<Q>
where
    Self: Sequential,
    Q: QueryOperation,
{
    fn eval(state: &Self, query: &Q) -> Q::Response;
}

pub struct Reconciled<S, R>(PhantomData<fn() -> (S, R)>);

pub trait Reconciliation<O, U> {
    fn linearize<'a>(log: &'a U) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a;
}

impl<S: Sequential, R> Reconciled<S, R> {
    pub fn fold<U>(stable: &S, unstable: &U) -> S
    where
        S: Clone,
        R: Reconciliation<S::Update, U>,
    {
        let mut state = stable.clone();
        for event in R::linearize(unstable) {
            S::apply(&mut state, event.op());
        }
        state
    }
}

impl<S, R> ReplicatedDataType for Reconciled<S, R>
where
    S: Sequential,
{
    type Update = S::Update;
    type Stable = S;
    type Rejection = Infallible;
}

pub trait CausalCompaction<U>
where
    Self: ReplicatedDataType,
    U: IsUnstableCore<Self::Update>,
{
    const DISABLE_R_WHEN_R: bool = false;
    const DISABLE_R_WHEN_NOT_R: bool = false;

    fn redundant_itself<'a>(
        _new_tagged_op: &TaggedOp<Self::Update>,
        _stable: &Stable<Self>,
        _unstable: impl Iterator<Item = &'a TaggedOp<Self::Update>>,
    ) -> bool
    where
        Self: 'a,
    {
        false
    }

    fn redundant_by_when_redundant(
        _old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        false
    }

    fn redundant_by_when_not_redundant(
        _old_op: &Self::Update,
        _old_tag: Option<&Tag>,
        _is_conc: bool,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) -> bool {
        false
    }

    fn compact_stable(
        _stable: &mut Stable<Self>,
        _rdnt: RedundancyRelation<Self::Update>,
        _new_tagged_op: &TaggedOp<Self::Update>,
    ) {
    }
}

/// How a CRDT folds causally stable history into its stable state.
pub enum Stabilization<S> {
    Regular,
    Critical { frontier: CausalFrontier, stable: S },
    None,
}

pub trait CausalStabilize<U>: ReplicatedDataType {
    const DISABLE_STABILIZE: bool = false;

    fn on_stabilize(
        _tagged_op: &TaggedOp<Self::Update>,
        _stable: &mut Stable<Self>,
        _unstable: &mut (impl IsUnstablePrune<Self::Update> + IsUnstableCore<Self::Update>),
    ) {
    }

    fn causal_stabilize(
        _frontier: &CausalFrontier,
        _stable: &Stable<Self>,
        _unstable: &U,
    ) -> Stabilization<Stable<Self>> {
        if Self::DISABLE_STABILIZE {
            Stabilization::None
        } else {
            Stabilization::Regular
        }
    }
}

pub enum Reset<Update> {
    Prune,
    Inject(Vec<Update>),
}

pub trait CausalReset<U>: ReplicatedDataType {
    fn reset(
        _version: &Version,
        _conservative: bool,
        _stable: &Stable<Self>,
        _unstable: &U,
    ) -> Reset<Self::Update> {
        Reset::Prune
    }
}
