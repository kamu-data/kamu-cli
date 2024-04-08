// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

/////////////////////////////////////////////////////////////////////////////////////////

/// Uniquely identifies a task within a compute node deployment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskID(i64);

impl TaskID {
    pub fn new(id: i64) -> Self {
        Self(id)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for TaskID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<TaskID> for i64 {
    fn from(val: TaskID) -> Self {
        val.0
    }
}
