// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::TaskOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAttempt {
    pub attempt_number: u32,
    pub started_at: DateTime<Utc>,
    pub attempt_result: Option<TaskAttemptResult>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskAttemptResult {
    pub finished_at: DateTime<Utc>,
    pub outcome: TaskOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
