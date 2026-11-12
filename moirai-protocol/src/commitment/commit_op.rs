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

use crate::commitment::leader_vote::LeaderVoteUpdate;

/// A commitment operation, which consists of an updateoperation and a vote for a leader
#[derive(Debug, Clone)]
#[cfg_attr(feature = "test_utils", derive(deepsize::DeepSizeOf))]
pub struct CommitOp<O> {
    /// Update operation
    pub op: O,
    /// Leader vote
    pub vote: LeaderVoteUpdate,
}

impl<O> CommitOp<O> {
    pub fn new(op: O, vote: LeaderVoteUpdate) -> Self {
        Self { op, vote }
    }

    pub fn split(self) -> (O, LeaderVoteUpdate) {
        (self.op, self.vote)
    }
}
