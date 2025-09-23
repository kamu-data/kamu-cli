// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system::{self as ts};
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowOutcome {
    /// Flow succeeded
    Success(ts::TaskResult),
    /// Flow failed to complete, even after retry logic
    Failed(ts::TaskError),
    /// Flow was aborted by user or by system
    Aborted,
}

impl FlowOutcome {
    pub fn try_task_result_as_ref(&self) -> Option<&ts::TaskResult> {
        match self {
            Self::Success(task_result) => Some(task_result),
            _ => None,
        }
    }

    pub fn is_success(&self) -> bool {
        matches!(self, Self::Success(_))
    }

    pub fn is_failure(&self) -> bool {
        matches!(self, Self::Failed(_))
    }

    pub fn is_aborted(&self) -> bool {
        matches!(self, Self::Aborted)
    }

    pub fn is_unrecoverable_failure(&self) -> bool {
        matches!(self, Self::Failed(err) if !err.recoverable)
    }
}

impl From<ts::TaskOutcome> for FlowOutcome {
    fn from(value: ts::TaskOutcome) -> Self {
        match value {
            ts::TaskOutcome::Success(result) => Self::Success(result),
            ts::TaskOutcome::Failed(error) => Self::Failed(error),
            ts::TaskOutcome::Cancelled => Self::Aborted,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
