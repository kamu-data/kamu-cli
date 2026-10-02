// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use thiserror::Error;

use crate::{FlowActivationCause, FlowBinding, FlowScope, ReactiveRule};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait FlowSensor: Send + Sync + Any {
    fn flow_scope(&self) -> &FlowScope;

    async fn get_sensitive_to_scopes(&self, catalog: &dill::Catalog) -> Vec<FlowScope>;

    fn update_rule(&self, rule: ReactiveRule);

    async fn on_activated(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError>;

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        activation_cause: &FlowActivationCause,
    ) -> Result<(), FlowSensorSensitizationError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a sensor treats input changes it has not seen when it is registered
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowSensorActivation {
    /// Reacts to input changes since the last run of its binding, via
    /// [`FlowSensor::on_activated`]
    CatchUp(DateTime<Utc>),

    /// Comes back after a restart while its binding has a pending flow, which
    /// already holds the input changes seen before; nothing to catch up on
    Restore,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FlowSensorSensitizationError {
    #[error("Flow binding unexpected: {binding:?}")]
    InvalidInputFlowBinding { binding: FlowBinding },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
