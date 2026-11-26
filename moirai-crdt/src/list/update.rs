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

#[cfg(feature = "test_utils")]
use deepsize::DeepSizeOf;
use moirai_protocol::utils::boxer::Boxer;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ListUpdate<V> {
    /// Insert `content` at the visible position observed by the issuing replica.
    Insert { content: V, pos: usize },
    /// Delete the visible item at `pos` in the issuing replica's parent version.
    Delete { pos: usize },
    /// Delete `len` visible items starting at `start`.
    DeleteRange { start: usize, len: usize },
    /// Mark the item at `pos` as freshly alive without changing its payload.
    ///
    /// This operation models update-wins behavior for nested values: a concurrent
    /// delete can only remove life dots it observed, so an unseen update dot keeps
    /// the item visible.
    Update { pos: usize },
}

impl<V> ListUpdate<V> {
    pub const fn insert(content: V, pos: usize) -> Self {
        Self::Insert { content, pos }
    }

    pub const fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    pub const fn delete_range(start: usize, len: usize) -> Self {
        Self::DeleteRange { start, len }
    }

    pub const fn update(pos: usize) -> Self {
        Self::Update { pos }
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "test_utils", derive(DeepSizeOf))]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum NestedListUpdate<O> {
    /// Insert a new child CRDT at the given position
    Insert { pos: usize, op: O },
    /// Update the child at the given position
    Update { pos: usize, op: O },
    /// Delete the child at the given position
    Delete { pos: usize },
}

impl<O> Boxer<NestedListUpdate<O>> for NestedListUpdate<Box<O>> {
    fn boxer(self) -> NestedListUpdate<O> {
        match self {
            Self::Insert { pos, op } => NestedListUpdate::Insert { pos, op: *op },
            Self::Update { pos, op } => NestedListUpdate::Update { pos, op: *op },
            Self::Delete { pos } => NestedListUpdate::Delete { pos },
        }
    }
}

impl<O> Boxer<NestedListUpdate<Box<O>>> for NestedListUpdate<O> {
    fn boxer(self) -> NestedListUpdate<Box<O>> {
        match self {
            Self::Insert { pos, op } => NestedListUpdate::Insert {
                pos,
                op: Box::new(op),
            },
            Self::Update { pos, op } => NestedListUpdate::Update {
                pos,
                op: Box::new(op),
            },
            Self::Delete { pos } => NestedListUpdate::Delete { pos },
        }
    }
}

impl<O> NestedListUpdate<O> {
    pub const fn insert(pos: usize, op: O) -> Self {
        Self::Insert { pos, op }
    }

    pub const fn delete(pos: usize) -> Self {
        Self::Delete { pos }
    }

    pub const fn update(pos: usize, op: O) -> Self {
        Self::Update { pos, op }
    }
}
