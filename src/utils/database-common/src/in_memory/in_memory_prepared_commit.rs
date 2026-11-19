// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A checked commit of one participant, holding its lock
pub trait InMemoryPreparedCommit {
    /// Applies the working set and releases the lock
    fn apply(self: Box<Self>);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Checked commits applied together, in the order they were prepared
pub struct InMemoryPreparedCommits<'a>(pub Vec<Box<dyn InMemoryPreparedCommit + 'a>>);

impl InMemoryPreparedCommit for InMemoryPreparedCommits<'_> {
    fn apply(self: Box<Self>) {
        for prepared_commit in self.0 {
            prepared_commit.apply();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
