// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::LoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_task_system as ts;

use crate::{
    FlowActivationCause,
    FlowBinding,
    FlowConfigurationRule,
    FlowID,
    FlowNotFoundError,
    FlowState,
    FlowTriggerRule,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait FlowRunService: Sync + Send {
    /// Initiates the specified flow manually, unless it's already waiting
    async fn run_flow_manually(
        &self,
        activation_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        initiator_account_id: odf::AccountID,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
        maybe_task_run_arguments: Option<ts::TaskRunArguments>,
    ) -> Result<FlowState, RunFlowError>;

    /// Initiates the specified flow with custom activation cause,
    /// unless it's already waiting
    async fn run_flow_automatically(
        &self,
        flow_binding: &FlowBinding,
        activation_cause: FlowActivationCause,
        maybe_flow_trigger_rule: Option<FlowTriggerRule>,
        maybe_forced_flow_config_rule: Option<FlowConfigurationRule>,
        maybe_task_run_arguments: Option<ts::TaskRunArguments>,
    ) -> Result<FlowState, RunFlowError>;

    /// Attempts to cancel the tasks already scheduled for the given flow
    async fn cancel_scheduled_tasks(
        &self,
        flow_id: FlowID,
    ) -> Result<FlowState, CancelScheduledTasksError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum RunFlowError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CancelScheduledTasksError {
    #[error(transparent)]
    NotFound(#[from] FlowNotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<LoadError<FlowState>> for CancelScheduledTasksError {
    fn from(value: LoadError<FlowState>) -> Self {
        match value {
            LoadError::NotFound(err) => Self::NotFound(FlowNotFoundError { flow_id: err.query }),
            LoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            LoadError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
