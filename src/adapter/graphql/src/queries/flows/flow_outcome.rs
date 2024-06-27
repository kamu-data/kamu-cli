// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetRepository;
use kamu_flow_system::FlowError;

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowOutcome {
    Success(FlowSuccessResult),
    Failed(FlowFailedError),
    Aborted(FlowAbortedResult),
}

#[derive(SimpleObject)]
pub(crate) struct FlowFailedError {
    reason: FlowFailedReason,
}

#[derive(Union)]
pub(crate) enum FlowFailedReason {
    FlowFailed(FlowFailedMessage),
    FlowDatasetCompactedFailed(FlowDatasetCompactedFailedError),
}

#[derive(SimpleObject)]
pub(crate) struct FlowFailedMessage {
    message: String,
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
    root_dataset: Dataset,
    message: String,
}

impl FlowOutcome {
    pub async fn from_maybe_flow_outcome(
        outcome_result: &Option<kamu_flow_system::FlowOutcome>,
        ctx: &Context<'_>,
    ) -> Result<Option<Self>, InternalError> {
        if let Some(value) = outcome_result {
            let result = match value {
                kamu_flow_system::FlowOutcome::Success(_) => Self::Success(FlowSuccessResult {
                    message: "SUCCESS".to_owned(),
                }),
                kamu_flow_system::FlowOutcome::Failed(err) => match err {
                    FlowError::Failed => Self::Failed(FlowFailedError {
                        reason: FlowFailedReason::FlowFailed(FlowFailedMessage {
                            message: "FAILED".to_owned(),
                        }),
                    }),
                    FlowError::RootDatasetCompacted(err) => {
                        let dataset_repository =
                            from_catalog::<dyn DatasetRepository>(ctx).unwrap();
                        let hdl = dataset_repository
                            .resolve_dataset_ref(&err.dataset_id.as_local_ref())
                            .await
                            .int_err()?;

                        let account = Account::from_dataset_alias(ctx, &hdl.alias)
                            .await?
                            .expect("Account must exist");

                        let dataset = Dataset::new(account, hdl);
                        Self::Failed(FlowFailedError {
                            reason: FlowFailedReason::FlowDatasetCompactedFailed(
                                FlowDatasetCompactedFailedError {
                                    message: "Root dataset was compacted".to_owned(),
                                    root_dataset: dataset,
                                },
                            ),
                        })
                    }
                },
                kamu_flow_system::FlowOutcome::Aborted => Self::Aborted(FlowAbortedResult {
                    message: "ABORTED".to_owned(),
                }),
            };
            return Ok(Some(result));
        }
        Ok(None)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
