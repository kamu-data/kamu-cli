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

/// Records how a task ended as the `outcome` field of the current span. Why a
/// task failed is in the spans and events inside it.
pub(crate) fn record_task_outcome(outcome: &Result<TaskOutcome, InternalError>) {
    tracing::Span::current().record(
        "outcome",
        match outcome {
            Ok(TaskOutcome::Success(_)) => "succeeded",
            Ok(TaskOutcome::Failed(_)) | Err(_) => "failed",
            Ok(TaskOutcome::Cancelled) => "cancelled",
        },
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
