// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_task_dataset::{TaskErrorDatasetReset, TaskErrorDatasetUpdate};
use kamu_adapter_task_webhook::TaskErrorWebhookDelivery;
use kamu_core::DatasetRegistry;
use kamu_task_system as ts;

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Describes a certain final outcome of the task
#[derive(Union, Debug)]
pub enum TaskOutcome {
    /// Task succeeded
    Success(TaskOutcomeSuccess),
    /// Task failed to complete
    Failed(TaskOutcomeFailed),
    /// Task was cancelled by a user
    Cancelled(TaskOutcomeCancelled),
}

impl TaskOutcome {
    pub async fn from_task_outcome(
        ctx: &Context<'_>,
        task_outcome: &ts::TaskOutcome,
    ) -> Result<Self, InternalError> {
        let result = match task_outcome {
            ts::TaskOutcome::Success(_) => Self::Success(TaskOutcomeSuccess {
                message: "SUCCESS".to_owned(),
            }),

            ts::TaskOutcome::Failed(e) => Self::Failed(TaskOutcomeFailed {
                reason: TaskFailureReason::from_task_error(ctx, e).await?,
            }),

            ts::TaskOutcome::Cancelled => Self::Cancelled(TaskOutcomeCancelled {
                message: "CANCELLED".to_owned(),
            }),
        };
        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct TaskOutcomeSuccess {
    message: String,
}

#[derive(SimpleObject, Debug)]
pub struct TaskOutcomeCancelled {
    message: String,
}

#[derive(SimpleObject, Debug)]
pub struct TaskOutcomeFailed {
    reason: TaskFailureReason,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug)]
pub enum TaskFailureReason {
    General(TaskFailureReasonGeneral),
    InputDatasetCompacted(TaskFailureReasonInputDatasetCompacted),
    WebhookDeliveryProblem(TaskFailureReasonWebhookDeliveryProblem),
}

#[derive(SimpleObject, Debug)]
pub struct TaskFailureReasonGeneral {
    message: String,
}

#[derive(SimpleObject, Debug)]
pub struct TaskFailureReasonInputDatasetCompacted {
    input_dataset: Dataset,
    message: String,
}

#[derive(SimpleObject, Debug)]
pub struct TaskFailureReasonWebhookDeliveryProblem {
    target_url: url::Url,
    message: String,
}

impl TaskFailureReason {
    pub async fn from_task_error(
        ctx: &Context<'_>,
        e: &ts::TaskError,
    ) -> Result<Self, InternalError> {
        Ok(match e.error_type.as_str() {
            ts::TaskError::TASK_ERROR_EMPTY => {
                TaskFailureReason::General(TaskFailureReasonGeneral {
                    message: "FAILED".to_owned(),
                })
            }

            TaskErrorDatasetUpdate::TYPE_ID => {
                let update_error = TaskErrorDatasetUpdate::from_task_error(e)?;
                match update_error {
                    TaskErrorDatasetUpdate::InputDatasetCompacted(e) => {
                        let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);
                        let hdl = dataset_registry
                            .resolve_dataset_handle_by_ref(&e.dataset_id.as_local_ref())
                            .await
                            .int_err()?;

                        let account = Account::from_dataset_alias(ctx, &hdl.alias)
                            .await?
                            .expect("Account must exist");

                        let dataset = Dataset::new_access_checked(account, hdl);
                        TaskFailureReason::InputDatasetCompacted(
                            TaskFailureReasonInputDatasetCompacted {
                                message: "Input dataset was compacted".to_owned(),
                                input_dataset: dataset,
                            },
                        )
                    }
                }
            }

            TaskErrorDatasetReset::TYPE_ID => {
                let reset_error = TaskErrorDatasetReset::from_task_error(e)?;
                match reset_error {
                    TaskErrorDatasetReset::ResetHeadNotFound => {
                        TaskFailureReason::General(TaskFailureReasonGeneral {
                            message: "New head hash to reset not found".to_owned(),
                        })
                    }
                }
            }

            TaskErrorWebhookDelivery::TYPE_ID => {
                let webhook_error = TaskErrorWebhookDelivery::from_task_error(e)?;
                TaskFailureReason::WebhookDeliveryProblem(TaskFailureReasonWebhookDeliveryProblem {
                    target_url: webhook_error.target_url().clone(),
                    message: match webhook_error {
                        TaskErrorWebhookDelivery::ConnectionTimeout(e) => {
                            format!("Response timeout after {} seconds", e.timeout.as_secs())
                        }
                        TaskErrorWebhookDelivery::FailedToConnect(_) => {
                            "Failed to connect".to_owned()
                        }
                        TaskErrorWebhookDelivery::UnsuccessfulResponse(e) => {
                            format!(
                                "Unsuccessful response code: {} {}",
                                e.status_code,
                                ::http::StatusCode::from_u16(e.status_code)
                                    .unwrap()
                                    .canonical_reason()
                                    .unwrap_or("Unknown")
                            )
                        }
                    },
                })
            }

            _ => {
                tracing::error!("Unexpected task error type: {}", e.error_type,);
                TaskFailureReason::General(TaskFailureReasonGeneral {
                    message: "Unexpected task error type".to_owned(),
                })
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
