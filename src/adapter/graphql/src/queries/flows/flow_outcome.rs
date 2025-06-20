// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_task_dataset::{TaskErrorDatasetReset, TaskErrorDatasetUpdate};
use kamu_core::DatasetRegistry;
use {kamu_flow_system as fs, kamu_task_system as ts};

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
    reason: FlowFailureReason,
}

#[derive(Union)]
pub(crate) enum FlowFailureReason {
    General(FlowFailureReasonGeneral),
    InputDatasetCompacted(FlowFailureReasonInputDatasetCompacted),
}

#[derive(SimpleObject)]
pub(crate) struct FlowFailureReasonGeneral {
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
pub(crate) struct FlowFailureReasonInputDatasetCompacted {
    input_dataset: Dataset,
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
                    match e.error_type.as_str() {
                        ts::TaskError::TASK_ERROR_EMPTY => Self::Failed(FlowFailedError {
                            reason: FlowFailureReason::General(FlowFailureReasonGeneral {
                                message: "FAILED".to_owned(),
                            }),
                        }),

                        TaskErrorDatasetUpdate::TYPE_ID => {
                            let update_error = TaskErrorDatasetUpdate::from_task_error(e)?;
                            match update_error {
                                TaskErrorDatasetUpdate::InputDatasetCompacted(e) => {
                                    let dataset_registry =
                                        from_catalog_n!(ctx, dyn DatasetRegistry);
                                    let hdl = dataset_registry
                                        .resolve_dataset_handle_by_ref(&e.dataset_id.as_local_ref())
                                        .await
                                        .int_err()?;

                                    let account = Account::from_dataset_alias(ctx, &hdl.alias)
                                        .await?
                                        .expect("Account must exist");

                                    let dataset = Dataset::new_access_checked(account, hdl);
                                    Self::Failed(FlowFailedError {
                                        reason: FlowFailureReason::InputDatasetCompacted(
                                            FlowFailureReasonInputDatasetCompacted {
                                                message: "Input dataset was compacted".to_owned(),
                                                input_dataset: dataset,
                                            },
                                        ),
                                    })
                                }
                            }
                        }

                        TaskErrorDatasetReset::TYPE_ID => {
                            let reset_error = TaskErrorDatasetReset::from_task_error(e)?;
                            match reset_error {
                                TaskErrorDatasetReset::ResetHeadNotFound => {
                                    Self::Failed(FlowFailedError {
                                        reason: FlowFailureReason::General(
                                            FlowFailureReasonGeneral {
                                                message: "New head hash to reset not found"
                                                    .to_owned(),
                                            },
                                        ),
                                    })
                                }
                            }
                        }

                        _ => {
                            tracing::error!(
                                "Unexpected task error type: {} for flow outcome: {:?}",
                                e.error_type,
                                flow_outcome
                            );
                            Self::Failed(FlowFailedError {
                                reason: FlowFailureReason::General(FlowFailureReasonGeneral {
                                    message: "Unexpected task error type".to_owned(),
                                }),
                            })
                        }
                    }
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
