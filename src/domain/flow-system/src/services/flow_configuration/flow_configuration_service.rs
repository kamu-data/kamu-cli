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

use crate::{FlowConfigurationRule, FlowConfigurationState, FlowKey};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationService: Sync + Send {
    /// Find current configuration of a certian type
    async fn find_configuration(
        &self,
        flow_key: FlowKey,
    ) -> Result<Option<FlowConfigurationState>, FindFlowConfigurationError>;

    /// Set or modify flow configuration
    async fn set_configuration(
        &self,
        flow_key: FlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState, SetFlowConfigurationError>;

    /// Lists all flow configurations, which are currently enabled
    fn list_enabled_configurations(&self) -> FlowConfigurationStateStream;
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
