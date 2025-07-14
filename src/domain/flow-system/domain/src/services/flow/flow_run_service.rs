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

use crate::{
    FlowBinding,
    FlowConfigurationRule,
    FlowID,
    FlowNotFoundError,
    FlowState,
    FlowTriggerInstance,
    FlowTriggerRule,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowRunService: Sync + Send {
    /// Initiates the specified flow manually, unless it's already waiting
    async fn run_flow_manually(
        &self,
        trigger_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        initiator_account_id: odf::AccountID,
        maybe_flow_config_snapshot: Option<FlowConfigurationRule>,
    ) -> Result<FlowState, RunFlowError>;

    /// Initiates the specified flow with custom trigger instance,
    /// unless it's already waiting
    async fn run_flow_with_trigger(
        &self,
        flow_binding: &FlowBinding,
        trigger_instance: FlowTriggerInstance,
        maybe_flow_trigger_rule: Option<FlowTriggerRule>,
        maybe_flow_config_snapshot: Option<FlowConfigurationRule>,
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
