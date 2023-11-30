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

use crate::{Schedule, SystemFlowConfigurationState, SystemFlowType};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SystemFlowConfigurationService: Sync + Send {
    /// Find current configuration of a certian type
    async fn find_configuration(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<SystemFlowConfigurationState>, FindSystemFlowConfigurationError>;

    /// Set or modify system flow configuration
    async fn set_configuration(
        &self,
        flow_type: SystemFlowType,
        paused: bool,
        schedule: Schedule,
    ) -> Result<SystemFlowConfigurationState, SetSystemFlowConfigurationError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SetSystemFlowConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
pub enum FindSystemFlowConfigurationError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<TryLoadError<SystemFlowConfigurationState>> for FindSystemFlowConfigurationError {
    fn from(value: TryLoadError<SystemFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<SystemFlowConfigurationState>> for SetSystemFlowConfigurationError {
    fn from(value: TryLoadError<SystemFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
