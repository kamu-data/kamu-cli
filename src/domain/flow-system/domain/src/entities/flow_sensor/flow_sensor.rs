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

use crate::{FlowActivationCause, FlowBinding, FlowScope};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSensor: Send + Sync + Any {
    fn flow_scope(&self) -> &FlowScope;

    async fn get_sensitive_to_scopes(&self, catalog: &dill::Catalog) -> Vec<FlowScope>;

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

#[derive(Error, Debug)]
pub enum FlowSensorSensitizationError {
    #[error("Flow binding unexpected: {binding:?}")]
    InvalidInputFlowBinding { binding: FlowBinding },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
