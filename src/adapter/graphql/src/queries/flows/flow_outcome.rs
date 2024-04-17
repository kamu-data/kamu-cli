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
    DatasetCompacted(FlowDatasetCompactedFailedError),
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

#[derive(SimpleObject)]
pub(crate) struct FlowDatasetCompactedFailedError {
    root_dataset_id: DatasetID,
    reason: String,
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
                FlowError::RootDatasetCompacted(err) => {
                    Self::DatasetCompacted(FlowDatasetCompactedFailedError {
                        reason: "Root dataset was compacted".to_string(),
                        root_dataset_id: err.dataset_id.clone().into(),
                    })
                }
            },
            kamu_flow_system::FlowOutcome::Aborted => Self::Success(FlowSuccessResult {
                message: "ABORTED".to_owned(),
            }),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
