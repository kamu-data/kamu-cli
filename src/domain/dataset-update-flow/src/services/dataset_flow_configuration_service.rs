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
use opendatafabric::DatasetID;
use tokio_stream::Stream;

use crate::{DatasetFlowConfigurationState, DatasetFlowType, FlowConfigurationRule};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetFlowConfigurationService: Sync + Send {
    /// Lists flow configurations, which are currently enabled
    fn list_enabled_configurations(
        &self,
        flow_type: DatasetFlowType,
    ) -> DatasetFlowConfigurationStateStream;

    /// Find current configuration of a certian type,
    /// which may or may not be associated with the given dataset
    async fn find_configuration(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<DatasetFlowConfigurationState>, FindDatasetFlowConfigurationError>;

    /// Set or modify dataset flow configuration
    async fn set_configuration(
        &self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Result<DatasetFlowConfigurationState, SetDatasetFlowConfigurationError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetFlowConfigurationStateStream<'a> = std::pin::Pin<
    Box<dyn Stream<Item = Result<DatasetFlowConfigurationState, InternalError>> + Send + 'a>,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SetDatasetFlowConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum FindDatasetFlowConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<TryLoadError<DatasetFlowConfigurationState>> for FindDatasetFlowConfigurationError {
    fn from(value: TryLoadError<DatasetFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<DatasetFlowConfigurationState>> for SetDatasetFlowConfigurationError {
    fn from(value: TryLoadError<DatasetFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
