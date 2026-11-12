// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_task_system::TaskOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a task ended, recorded as the `outcome` field of its span. Why a task
/// failed is in the spans and events inside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum TaskSpanOutcome {
    Succeeded,
    Failed,
    Cancelled,
}

impl TaskSpanOutcome {
    /// Records how a task ended on the current span, which must declare the
    /// field
    pub(crate) fn record(outcome: &Result<TaskOutcome, InternalError>) {
        let outcome: &'static str = Self::from(outcome).into();
        tracing::Span::current().record("outcome", outcome);
    }
}

impl From<&Result<TaskOutcome, InternalError>> for TaskSpanOutcome {
    fn from(outcome: &Result<TaskOutcome, InternalError>) -> Self {
        match outcome {
            Ok(TaskOutcome::Success(_)) => Self::Succeeded,
            Ok(TaskOutcome::Failed(_)) | Err(_) => Self::Failed,
            Ok(TaskOutcome::Cancelled) => Self::Cancelled,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
