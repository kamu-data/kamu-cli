// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::TryLoadError;
use internal_error::ErrorIntoInternal;

use crate::{
    FindFlowConfigurationError,
    FlowConfigurationService,
    SetFlowConfigurationError,
    SystemFlowConfigurationState,
    SystemFlowKey,
};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait SystemFlowConfigurationService: FlowConfigurationService<SystemFlowKey> {}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<TryLoadError<SystemFlowConfigurationState>> for FindFlowConfigurationError {
    fn from(value: TryLoadError<SystemFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<TryLoadError<SystemFlowConfigurationState>> for SetFlowConfigurationError {
    fn from(value: TryLoadError<SystemFlowConfigurationState>) -> Self {
        match value {
            TryLoadError::ProjectionError(err) => Self::Internal(err.int_err()),
            TryLoadError::Internal(err) => Self::Internal(err),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
