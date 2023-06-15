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
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventID(u64);

impl EventID {
    pub fn new(v: u64) -> Self {
        Self(v)
    }

    pub fn into_inner(self) -> u64 {
        self.0
    }
}

impl Into<u64> for EventID {
    fn into(self) -> u64 {
        self.0
    }
}

impl std::fmt::Debug for EventID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Display for EventID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
