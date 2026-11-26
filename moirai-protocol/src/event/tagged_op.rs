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

use std::fmt::{Debug, Display};

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;

use crate::event::{Event, id::EventId, lamport::Lamport, tag::Tag};

/// An operation tagged with an event ID and Lamport timestamp.
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[derive(Debug, Clone)]
pub struct TaggedOp<O> {
    op: O,
    tag: Tag,
}

impl<O> TaggedOp<O> {
    pub const fn new(id: EventId, lamport: Lamport, op: O) -> Self {
        Self {
            op,
            tag: Tag::new(id, lamport),
        }
    }

    pub const fn id(&self) -> &EventId {
        self.tag.id()
    }

    pub const fn lamport(&self) -> &Lamport {
        self.tag.lamport()
    }

    pub const fn op(&self) -> &O {
        &self.op
    }

    pub const fn tag(&self) -> &Tag {
        &self.tag
    }
}

impl<O> From<&Event<O>> for TaggedOp<O>
where
    O: Clone,
{
    fn from(event: &Event<O>) -> Self {
        Self {
            op: event.op().clone(),
            tag: Tag::new(event.id().clone(), *event.lamport()),
        }
    }
}

impl<O> Display for TaggedOp<O>
where
    O: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{:?}@{}]", self.op, self.tag.id())
    }
}
