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

use std::fmt::Display;

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::event::{id::EventId, lamport::Lamport};

/// A tag that contains an event ID and a Lamport timestamp
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Tag {
    id: EventId,
    lamport: Lamport,
}

impl Tag {
    pub const fn new(id: EventId, lamport: Lamport) -> Self {
        Self { id, lamport }
    }

    pub const fn id(&self) -> &EventId {
        &self.id
    }

    pub const fn lamport(&self) -> &Lamport {
        &self.lamport
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{},{}]", self.id, self.lamport)
    }
}
