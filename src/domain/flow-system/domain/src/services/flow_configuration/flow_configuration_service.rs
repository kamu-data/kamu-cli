// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::TryLoadError;
use internal_error::{ErrorIntoInternal, InternalError};
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
        dataset_ids: Vec<odf::DatasetID>,
    ) -> FlowConfigurationStateStream;

    /// Set or modify flow configuration
    async fn set_configuration(
        &self,
        flow_key: FlowKey,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError>;

    /// Lists all active flow configurations
    fn list_active_configurations(&self) -> FlowConfigurationStateStream;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationServiceExt {
    async fn try_get_dataset_ingest_rule(
        &self,
        dataset_id: odf::DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<IngestRule>, FindFlowConfigurationError>;

    async fn try_get_dataset_compaction_rule(
        &self,
        dataset_id: odf::DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<CompactionRule>, FindFlowConfigurationError>;

    async fn try_get_dataset_reset_rule(
        &self,
        dataset_id: odf::DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<ResetRule>, FindFlowConfigurationError>;

    async fn try_get_config_snapshot_by_key(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationRule>, FindFlowConfigurationError>;
}

#[async_trait::async_trait]
impl<T: FlowConfigurationService + ?Sized> FlowConfigurationServiceExt for T {
    async fn try_get_dataset_ingest_rule(
        &self,
        dataset_id: odf::DatasetID,
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
        dataset_id: odf::DatasetID,
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
        dataset_id: odf::DatasetID,
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
    ) -> Result<Option<FlowConfigurationRule>, FindFlowConfigurationError> {
        let maybe_snapshot = match flow_key {
            FlowKey::System(_) => None,
            FlowKey::Dataset(dataset_flow_key) => match dataset_flow_key.flow_type {
                DatasetFlowType::ExecuteTransform => None,
                DatasetFlowType::Ingest => self
                    .try_get_dataset_ingest_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationRule::IngestRule),
                DatasetFlowType::Reset => self
                    .try_get_dataset_reset_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationRule::ResetRule),
                DatasetFlowType::HardCompaction => self
                    .try_get_dataset_compaction_rule(
                        dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .await?
                    .map(FlowConfigurationRule::CompactionRule),
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
