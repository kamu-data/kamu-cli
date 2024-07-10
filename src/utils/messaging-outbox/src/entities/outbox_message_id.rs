// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A monotonically increasing identifier
/// assigned by event stores to events

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OutboxMessageID(i64);

impl OutboxMessageID {
    pub fn new(v: i64) -> Self {
        Self(v)
    }

    pub fn into_inner(self) -> i64 {
        self.0
    }
}

impl From<OutboxMessageID> for i64 {
    fn from(val: OutboxMessageID) -> Self {
        val.0
    }
}

impl From<i64> for OutboxMessageID {
    fn from(val: i64) -> Self {
        OutboxMessageID::new(val)
    }
}

impl std::fmt::Debug for OutboxMessageID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for OutboxMessageID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
