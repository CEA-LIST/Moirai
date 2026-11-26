/////////////////////////////////////////////////////////////////////////////
//  Copyright (c) 2025, 2026 CEA LIST.
//  
//  This program and the accompanying materials
//  are made available under the terms of the Apache License, Version 2.0
//  which accompanies this distribution, and is available at
//  https://www.apache.org/licenses/LICENSE-2.0
// 
//  SPDX-License-Identifier: Apache-2.0
//  
//  Contributors:
// Léo Olivier -  (CEA LIST) - Initial implementation
//
/////////////////////////////////////////////////////////////////////////////

use crate::{
    clock::{causal_frontier::CausalFrontier, version_vector::Version},
    data_type::{query::QueryOperation, sequential::SequentialDataType},
    event::{tag::Tag, tagged_op::TaggedOp},
    state::unstable_state::{IsUnstableCore, IsUnstablePrune},
};

pub trait ReplicatedDataType: Sized {
    type Update: Clone;
    type Stable: SequentialDataType<Update = Self::Update>;
    type Rejection;

    // TODO: is it really useful?
    fn eval<Q, U>(stable: &Stable<Self>, unstable: &U, query: &Q) -> Q::Response
    where
        Q: QueryOperation,
        Self: Eval<U, Q>,
    {
        Self::execute_query(stable, unstable, query)
    }
}

pub trait Precondition<U>
where
    Self: ReplicatedDataType,
{
    fn is_enabled(
        stable: &Stable<Self>,
        unstable: &U,
        update: &Self::Update,
    ) -> Result<(), Self::Rejection>;
}

pub trait Eval<U, Q>
where
    Q: QueryOperation,
    Self: ReplicatedDataType,
{
    fn execute_query(
        stable: &<Self as ReplicatedDataType>::Stable,
        unstable: &U,
        q: &Q,
    ) -> Q::Response;
}

pub type Stable<R> = <R as ReplicatedDataType>::Stable;

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

pub type RedundancyRelation<O> = fn(
    _old_op: &O,
    _old_event_id: Option<&Tag>,
    is_conc: bool,
    new_tagged_op: &TaggedOp<O>,
) -> bool;

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
