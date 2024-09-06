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

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationService: Sync + Send {
    /// Find current configuration of a certain type
    async fn find_configuration(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationState>, FindFlowConfigurationError>;

    /// Find all configurations by dataset ids
    async fn find_configurations_by_datasets(
        &self,
        dataset_ids: Vec<DatasetID>,
    ) -> FlowConfigurationStateStream;

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

    /// Pauses particular flow configuration
    async fn pause_flow_configuration(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError>;

    /// Resumes particular flow configuration
    async fn resume_flow_configuration(
        &self,
        request_time: DateTime<Utc>,
        flow_key: FlowKey,
    ) -> Result<(), InternalError>;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationServiceExt {
    async fn try_get_flow_schedule(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<Schedule>, FindFlowConfigurationError>;

    async fn try_get_dataset_transform_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<TransformRule>, FindFlowConfigurationError>;

    async fn try_get_dataset_ingest_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<IngestRule>, FindFlowConfigurationError>;

    async fn try_get_dataset_compaction_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<CompactionRule>, FindFlowConfigurationError>;

    async fn try_get_dataset_reset_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<ResetRule>, FindFlowConfigurationError>;

    async fn try_get_config_snapshot_by_key(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationSnapshot>, FindFlowConfigurationError>;
}

#[async_trait::async_trait]
impl<T: FlowConfigurationService + ?Sized> FlowConfigurationServiceExt for T {
    async fn try_get_flow_schedule(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<Schedule>, FindFlowConfigurationError> {
        let maybe_config = self.find_configuration(flow_key).await?;
        Ok(
            if let Some(config) = maybe_config
                && config.is_active()
            {
                config.try_get_schedule()
            } else {
                None
            },
        )
    }

    async fn try_get_dataset_transform_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<TransformRule>, FindFlowConfigurationError> {
        let maybe_config = self
            .find_configuration(FlowKey::dataset(dataset_id, flow_type))
            .await?;
        Ok(
            if let Some(config) = maybe_config
                && config.is_active()
            {
                config.try_get_transform_rule()
            } else {
                None
            },
        )
    }

    async fn try_get_dataset_ingest_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<IngestRule>, FindFlowConfigurationError> {
        let maybe_config = self
            .find_configuration(FlowKey::dataset(dataset_id, flow_type))
            .await?;
        Ok(
            if let Some(config) = maybe_config
                && config.is_active()
            {
                config.try_get_ingest_rule()
            } else {
                None
            },
        )
    }

    async fn try_get_dataset_compaction_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<CompactionRule>, FindFlowConfigurationError> {
        let maybe_config = self
            .find_configuration(FlowKey::dataset(dataset_id, flow_type))
            .await?;
        Ok(
            if let Some(config) = maybe_config
                && config.is_active()
            {
                config.try_get_compaction_rule()
            } else {
                None
            },
        )
    }

    async fn try_get_dataset_reset_rule(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<ResetRule>, FindFlowConfigurationError> {
        let maybe_config = self
            .find_configuration(FlowKey::dataset(dataset_id, flow_type))
            .await?;
        Ok(
            if let Some(config) = maybe_config
                && config.is_active()
            {
                config.try_get_reset_rule()
            } else {
                None
            },
        )
    }

    async fn try_get_config_snapshot_by_key(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationSnapshot>, FindFlowConfigurationError> {
        let maybe_snapshot = match flow_key {
            FlowKey::System(_) => self
                .try_get_flow_schedule(flow_key)
                .await?
                .map(FlowConfigurationSnapshot::Schedule),
            FlowKey::Dataset(dataset_flow_key) => match dataset_flow_key.flow_type {
                DatasetFlowType::ExecuteTransform => self
                    .try_get_dataset_transform_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationSnapshot::Transform),
                DatasetFlowType::Ingest => self
                    .try_get_dataset_ingest_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationSnapshot::Ingest),
                DatasetFlowType::Reset => self
                    .try_get_dataset_reset_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationSnapshot::Reset),
                DatasetFlowType::HardCompaction => self
                    .try_get_dataset_compaction_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationSnapshot::Compaction),
            },
        };

        Ok(maybe_snapshot)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type FlowConfigurationStateStream<'a> = std::pin::Pin<
    Box<dyn Stream<Item = Result<FlowConfigurationState, InternalError>> + Send + 'a>,
>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
