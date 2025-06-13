// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::TryFromIntError;

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a task within a compute node deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd, Serialize, Deserialize)]
pub struct TaskID(u64);

impl TaskID {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

impl TryFrom<i64> for TaskID {
    type Error = TryFromIntError;

    fn try_from(val: i64) -> Result<Self, Self::Error> {
        let id: u64 = u64::try_from(val)?;
        Ok(Self::new(id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for TaskID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TaskID> for u64 {
    fn from(val: TaskID) -> Self {
        val.0
    }
}

impl TryFrom<TaskID> for i64 {
    type Error = TryFromIntError;

    fn try_from(val: TaskID) -> Result<Self, Self::Error> {
        i64::try_from(val.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
