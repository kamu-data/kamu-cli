// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::TryLoadError;
use internal_error::{ErrorIntoInternal, InternalError};
use tokio_stream::Stream;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait FlowTriggerService: Sync + Send {
    /// Find current trigger of a certain type
    async fn find_trigger(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowTriggerState>, InternalError>;

    /// Match triggers against flow scope query
    async fn match_triggers(
        &self,
        flow_scope_query: FlowScopeQuery,
    ) -> Result<Vec<FlowTriggerState>, InternalError>;

    /// Set or modify flow trigger
    async fn set_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        rule: FlowTriggerRule,
        stop_policy: FlowTriggerStopPolicy,
    ) -> Result<FlowTriggerState, SetFlowTriggerError>;

    /// Lists all flow triggers, which are currently enabled
    #[allow(clippy::elidable_lifetime_names)] // due to mock
    fn list_enabled_triggers<'a>(&'a self) -> FlowTriggerStateStream<'a>;

    /// Pauses particular flow trigger (user initiative)
    async fn pause_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError>;

    /// Resumes particular flow trigger
    async fn resume_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
    ) -> Result<(), InternalError>;

    /// Pauses flow triggers for given list of scopes
    async fn pause_flow_triggers_for_scopes(
        &self,
        request_time: DateTime<Utc>,
        scopes: &[FlowScope],
    ) -> Result<(), InternalError>;

    /// Resumes flow triggers for given list of scopes
    async fn resume_flow_triggers_for_scopes(
        &self,
        request_time: DateTime<Utc>,
        scopes: &[FlowScope],
    ) -> Result<(), InternalError>;

    /// Checks if there are any active triggers for the given list of scopes
    async fn has_active_triggers_for_scopes(
        &self,
        scopes: &[FlowScope],
    ) -> Result<bool, InternalError>;

    /// Evaluates trigger stop policy after a failure
    async fn evaluate_trigger_on_failure(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: &FlowBinding,
        unrecoverable: bool,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowTriggerServiceExt {
    async fn try_get_flow_active_schedule_rule(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<Schedule>, InternalError>;

    async fn try_get_flow_active_reactive_rule(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<ReactiveRule>, InternalError>;
}

#[async_trait::async_trait]
impl<T: FlowTriggerService + ?Sized> FlowTriggerServiceExt for T {
    async fn try_get_flow_active_schedule_rule(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<Schedule>, InternalError> {
        let maybe_trigger = self.find_trigger(flow_binding).await?;
        Ok(
            if let Some(trigger) = maybe_trigger
                && trigger.is_active()
            {
                trigger.try_get_schedule_rule()
            } else {
                None
            },
        )
    }

    async fn try_get_flow_active_reactive_rule(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<ReactiveRule>, InternalError> {
        let maybe_trigger = self.find_trigger(flow_binding).await?;
        Ok(
            if let Some(trigger) = maybe_trigger
                && trigger.is_active()
            {
                trigger.try_get_reactive_rule()
            } else {
                None
            },
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SetFlowTriggerError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowTriggerStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<FlowTriggerState, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<TryLoadError<FlowTriggerState>> for SetFlowTriggerError {
    fn from(value: TryLoadError<FlowTriggerState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
