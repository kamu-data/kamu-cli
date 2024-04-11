// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::FlowError;

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowOutcome {
    Success(FlowSuccessResult),
    Failed(FlowFailedError),
    Abotted(FlowAbortedResult),
}

#[derive(SimpleObject)]
pub(crate) struct FlowFailedError {
    reason: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowSuccessResult {
    message: String,
}

#[derive(SimpleObject)]
pub(crate) struct FlowAbortedResult {
    message: String,
}

impl From<&kamu_flow_system::FlowOutcome> for FlowOutcome {
    fn from(value: &kamu_flow_system::FlowOutcome) -> Self {
        match value {
            kamu_flow_system::FlowOutcome::Success(_) => Self::Success(FlowSuccessResult {
                message: "SUCCESS".to_owned(),
            }),
            kamu_flow_system::FlowOutcome::Failed(err) => match err {
                FlowError::Failed => Self::Failed(FlowFailedError {
                    reason: "FAILED".to_owned(),
                }),
                FlowError::RootDatasetWasCompacted(err) => Self::Failed(FlowFailedError {
                    reason: format!("Root dataset {} was compacted", err.dataset_id),
                }),
            },
            kamu_flow_system::FlowOutcome::Aborted => Self::Success(FlowSuccessResult {
                message: "ABORTED".to_owned(),
            }),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
