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

use std::convert::Infallible;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_protocol::data_type::sequential::SequentialDataType;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use crate::counter::{CounterValue, update::CounterUpdate};

#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(
    feature = "serde",
    serde(bound(
        serialize = "V::Accumulator: Serialize",
        deserialize = "V::Accumulator: Deserialize<'de>"
    ))
)]

pub struct CounterStable<V: CounterValue>(V::Accumulator);

impl<V: CounterValue> CounterStable<V> {
    pub fn into_inner(self) -> V::Accumulator {
        self.0
    }
    pub const fn as_inner(&self) -> &V::Accumulator {
        &self.0
    }
    pub const fn as_inner_mut(&mut self) -> &mut V::Accumulator {
        &mut self.0
    }
}

impl<V> SequentialDataType for CounterStable<V>
where
    V: CounterValue,
{
    type Update = CounterUpdate<V>;
    type Rejection = Infallible;

    fn is_default(&self) -> bool {
        V::value(&self.0) == V::default()
    }

    fn apply(&mut self, value: &Self::Update) {
        match value {
            Self::Update::Inc(v) => V::increment(&mut self.0, *v),
            Self::Update::Dec(v) => V::decrement(&mut self.0, *v),
            Self::Update::Reset => unreachable!(),
        }
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }
}

#[cfg(feature = "test_utils")]
impl<V: CounterValue> DeepSizeOf for CounterStable<V>
where
    V::Accumulator: DeepSizeOf,
{
    fn deep_size_of_children(&self, context: &mut deepsize::Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

impl<V: CounterValue> Copy for CounterStable<V> where V::Accumulator: Copy {}

impl<V: CounterValue> PartialEq for CounterStable<V>
where
    V::Accumulator: PartialEq,
{
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl<V: CounterValue> Eq for CounterStable<V> where V::Accumulator: Eq {}
