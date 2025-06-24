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
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowConfigurationState>, FindFlowConfigurationError>;

    /// Set or modify flow configuration
    async fn set_configuration(
        &self,
        flow_binding: FlowBinding,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError>;

    /// Lists all active flow configurations
    fn list_active_configurations(&self) -> FlowConfigurationStateStream;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationServiceExt {
    async fn try_get_config_snapshot_by_binding(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowConfigurationRule>, FindFlowConfigurationError>;
}

#[async_trait::async_trait]
impl<T: FlowConfigurationService + ?Sized> FlowConfigurationServiceExt for T {
    async fn try_get_config_snapshot_by_binding(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowConfigurationRule>, FindFlowConfigurationError> {
        let maybe_config = self.find_configuration(flow_binding).await?;
        Ok(maybe_config.map(|config| config.rule))
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
