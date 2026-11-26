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

use crate::data_type::sequential::SequentialDataType;

/// SequentialDataType storage for CRDTs that retain their stable operations.
impl<O> SequentialDataType for Vec<O>
where
    O: Clone,
{
    type Update = O;
    type Rejection = Infallible;

    fn apply(&mut self, update: &O) {
        self.push(update.clone());
    }

    fn is_enabled(&self, _update: &Self::Update) -> Result<(), Self::Rejection> {
        Ok(())
    }

    fn is_default(&self) -> bool {
        self.is_empty()
    }
}
