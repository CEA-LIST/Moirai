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

use crate::data_type::query::QueryOperation;

/// SequentialDataType behavior implemented directly by the stored state.
pub trait SequentialDataType: Default {
    type Update: Clone;
    type Rejection;

    fn apply(&mut self, update: &Self::Update);

    fn is_enabled(&self, update: &Self::Update) -> Result<(), Self::Rejection>;

    /// Whether the state contains no retained contribution.
    fn is_default(&self) -> bool;
}

pub trait SequentialEval<Q>
where
    Self: SequentialDataType,
    Q: QueryOperation,
{
    fn eval(state: &Self, query: &Q) -> Q::Response;
}
