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

#[async_trait::async_trait]
pub trait FlowTriggerService: Sync + Send {
    /// Find current trigger of a certain type
    async fn find_trigger(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowTriggerState>, FindFlowTriggerError>;

    /// Set or modify flow configuration
    async fn set_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
        paused: bool,
        rule: FlowTriggerRule,
    ) -> Result<FlowTriggerState, SetFlowTriggerError>;

    /// Lists all flow configurations, which are currently enabled
    fn list_enabled_triggers(&self) -> FlowTriggerStateStream;

    /// Pauses particular flow configuration
    async fn pause_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError>;

    /// Resumes particular flow configuration
    async fn resume_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError>;

    /// Pauses dataset flows of given type for given dataset.
    /// If type is omitted, all possible dataset flow types are paused
    async fn pause_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<(), InternalError>;

    /// Pauses system flows of given type.
    /// If type is omitted, all possible system flow types are paused
    async fn pause_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<SystemFlowType>,
    ) -> Result<(), InternalError>;

    /// Resumes dataset flows of given type for given dataset.
    /// If type is omitted, all possible types are resumed (where configured)
    async fn resume_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &odf::DatasetID,
        maybe_dataset_flow_type: Option<DatasetFlowType>,
    ) -> Result<(), InternalError>;

    /// Resumes system flows of given type.
    /// If type is omitted, all possible system flow types are resumed (where
    /// configured)
    async fn resume_system_flows(
        &self,
        request_time: DateTime<Utc>,
        maybe_system_flow_type: Option<SystemFlowType>,
    ) -> Result<(), InternalError>;

    /// Checks if there are any active triggers for the given list of datasets
    async fn has_active_triggers_for_datasets(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<bool, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowTriggerServiceExt {
    async fn try_get_flow_schedule_rule(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<Schedule>, FindFlowTriggerError>;

    async fn try_get_flow_batching_rule(
        &self,
        dataset_id: odf::DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<BatchingRule>, FindFlowTriggerError>;
}

#[async_trait::async_trait]
impl<T: FlowTriggerService + ?Sized> FlowTriggerServiceExt for T {
    async fn try_get_flow_schedule_rule(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<Schedule>, FindFlowTriggerError> {
        let maybe_trigger = self.find_trigger(flow_key).await?;
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

    async fn try_get_flow_batching_rule(
        &self,
        dataset_id: odf::DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<BatchingRule>, FindFlowTriggerError> {
        let maybe_trigger = self
            .find_trigger(FlowKey::dataset(dataset_id, flow_type))
            .await?;
        Ok(
            if let Some(trigger) = maybe_trigger
                && trigger.is_active()
            {
                trigger.try_get_batching_rule()
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

#[derive(thiserror::Error, Debug)]
pub enum FindFlowTriggerError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowTriggerStateStream<'a> =
    std::pin::Pin<Box<dyn Stream<Item = Result<FlowTriggerState, InternalError>> + Send + 'a>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<TryLoadError<FlowTriggerState>> for FindFlowTriggerError {
    fn from(value: TryLoadError<FlowTriggerState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<FlowTriggerState>> for SetFlowTriggerError {
    fn from(value: TryLoadError<FlowTriggerState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
