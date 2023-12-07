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

use crate::{
    DatasetFlowConfigurationState,
    DatasetFlowKey,
    DatasetFlowType,
    FindFlowConfigurationError,
    FlowConfigurationService,
    SetFlowConfigurationError,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetFlowConfigurationService: FlowConfigurationService<DatasetFlowKey> {
    /// Lists dataset flow configurations, which are currently enabled
    fn list_enabled_configurations(
        &self,
        flow_type: DatasetFlowType,
    ) -> DatasetFlowConfigurationStateStream;
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type DatasetFlowConfigurationStateStream<'a> = std::pin::Pin<
    Box<dyn Stream<Item = Result<DatasetFlowConfigurationState, InternalError>> + Send + 'a>,
>;

/////////////////////////////////////////////////////////////////////////////////////////

impl From<TryLoadError<DatasetFlowConfigurationState>> for FindFlowConfigurationError {
    fn from(value: TryLoadError<DatasetFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<DatasetFlowConfigurationState>> for SetFlowConfigurationError {
    fn from(value: TryLoadError<DatasetFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
