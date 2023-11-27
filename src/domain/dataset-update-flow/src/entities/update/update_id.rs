// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a task within a compute node deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpdateID(u64);

impl UpdateID {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for UpdateID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Into<u64> for UpdateID {
    fn into(self) -> u64 {
        self.0
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
