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
use opendatafabric::DatasetID;
use tokio_stream::Stream;

use crate::{
    DatasetFlowType,
    FlowConfigurationRule,
    FlowConfigurationState,
    FlowKey,
    SystemFlowType,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationService: Sync + Send {
    /// Find current configuration of a certain type
    async fn find_configuration(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationState>, FindFlowConfigurationError>;

    /// Set or modify flow configuration
    async fn set_configuration(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError>;

    /// Lists all flow configurations, which are currently enabled
    fn list_enabled_configurations(&self) -> FlowConfigurationStateStream;

    /// Pauses dataset flows of given type for given dataset.
    /// If type is omitted, all possible dataset flow types are paused
    async fn pause_dataset_flows(
        &self,
        request_time: DateTime<Utc>,
        dataset_id: &DatasetID,
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
        dataset_id: &DatasetID,
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SetFlowConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum FindFlowConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type FlowConfigurationStateStream<'a> = std::pin::Pin<
    Box<dyn Stream<Item = Result<FlowConfigurationState, InternalError>> + Send + 'a>,
>;

/////////////////////////////////////////////////////////////////////////////////////////

impl From<TryLoadError<FlowConfigurationState>> for FindFlowConfigurationError {
    fn from(value: TryLoadError<FlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<FlowConfigurationState>> for SetFlowConfigurationError {
    fn from(value: TryLoadError<FlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
