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

use moirai_protocol::{
    data_type::replicated::{ReplicatedDataType, Stable},
    state::unstable_state::IsUnstableCore,
};
use rand::Rng;

/// Generates valid and enabled updates for a replicated data type
/// given the current state of its log.
pub trait OpGenerator<U>: ReplicatedDataType
where
    U: IsUnstableCore<Self::Update>,
{
    type Config: Default;

    fn generate(
        rng: &mut impl Rng,
        config: &Self::Config,
        stable: &Stable<Self>,
        unstable: &U,
    ) -> Self::Update;
}
