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

use std::{convert::Infallible, marker::PhantomData};

use crate::{
    data_type::{replicated::ReplicatedDataType, sequential::SequentialDataType},
    event::tagged_op::TaggedOp,
};

pub struct Reconciled<S, R>(PhantomData<fn() -> (S, R)>);

pub trait Reconciliation<O, U> {
    fn linearize<'a>(log: &'a U) -> impl Iterator<Item = &'a TaggedOp<O>>
    where
        O: 'a;
}

impl<S, R> Reconciled<S, R>
where
    S: SequentialDataType,
{
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
    S: SequentialDataType,
{
    type Update = S::Update;
    type Stable = S;
    type Rejection = Infallible;
}
