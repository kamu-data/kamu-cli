// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{FlowConfigurationRule, FlowConfigurationState};

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowConfigurationService<TFlowKey: std::fmt::Debug>: Sync + Send {
    /// Find current configuration of a certian type,
    /// which may or may not be associated with the given dataset
    async fn find_configuration(
        &self,
        flow_key: TFlowKey,
    ) -> Result<Option<FlowConfigurationState<TFlowKey>>, FindFlowConfigurationError>;

    /// Set or modify dataset flow configuration
    async fn set_configuration(
        &self,
        flow_key: TFlowKey,
        paused: bool,
        rule: FlowConfigurationRule,
    ) -> Result<FlowConfigurationState<TFlowKey>, SetFlowConfigurationError>;
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
