// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::TaskFailureReason;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Final result of a finished flow
#[derive(Union)]
pub(crate) enum FlowOutcome {
    Success(FlowSuccessResult),
    Failed(FlowFailedError),
    Aborted(FlowAbortedResult),
}

/// Flow failed
#[derive(SimpleObject)]
pub(crate) struct FlowFailedError {
    /// Why the flow failed
    reason: TaskFailureReason,
}

/// Flow completed successfully
#[derive(SimpleObject)]
pub(crate) struct FlowSuccessResult {
    /// Fixed status text
    message: String,
}

/// Flow was aborted before completion
#[derive(SimpleObject)]
pub(crate) struct FlowAbortedResult {
    /// Fixed status text
    message: String,
}

impl FlowOutcome {
    pub async fn from_flow_outcome(
        ctx: &Context<'_>,
        flow_outcome: &fs::FlowOutcome,
    ) -> Result<Self, InternalError> {
        let result = match flow_outcome {
            fs::FlowOutcome::Success(_) => Self::Success(FlowSuccessResult {
                message: "SUCCESS".to_owned(),
            }),
            fs::FlowOutcome::Failed(error) => Self::Failed(FlowFailedError {
                reason: TaskFailureReason::from_task_error(ctx, error).await?,
            }),
            fs::FlowOutcome::Aborted => Self::Aborted(FlowAbortedResult {
                message: "ABORTED".to_owned(),
            }),
        };
        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
