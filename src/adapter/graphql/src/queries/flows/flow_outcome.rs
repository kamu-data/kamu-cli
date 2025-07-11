// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use {kamu_flow_system as fs, kamu_task_system as ts};

use crate::prelude::*;
use crate::queries::TaskFailureReason;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowOutcome {
    Success(FlowSuccessResult),
    Failed(FlowFailedError),
    Aborted(FlowAbortedResult),
}

#[derive(SimpleObject)]
pub(crate) struct FlowFailedError {
    reason: TaskFailureReason,
}

#[derive(SimpleObject)]
pub(crate) struct FlowSuccessResult {
    message: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowAbortedResult {
    message: String,
}

impl FlowOutcome {
    pub async fn from_flow_and_task_outcomes(
        ctx: &Context<'_>,
        flow_outcome: &fs::FlowOutcome,
        maybe_task_outcome: Option<&ts::TaskOutcome>,
    ) -> Result<Self, InternalError> {
        let result = match flow_outcome {
            fs::FlowOutcome::Success(_) => Self::Success(FlowSuccessResult {
                message: "SUCCESS".to_owned(),
            }),
            fs::FlowOutcome::Failed => {
                if let Some(ts::TaskOutcome::Failed(e)) = maybe_task_outcome {
                    Self::Failed(FlowFailedError {
                        reason: TaskFailureReason::from_task_error(ctx, e).await?,
                    })
                } else {
                    unreachable!("Flow outcome is failed, but task outcome is not failed");
                }
            }
            fs::FlowOutcome::Aborted => Self::Aborted(FlowAbortedResult {
                message: "ABORTED".to_owned(),
            }),
        };
        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
