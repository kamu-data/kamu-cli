// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;

use crate::{FlowActivationCause, FlowBinding, FlowScope, FlowSensor};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowSensorDispatcher: Send + Sync {
    async fn register_sensor(&self, flow_sensor: Arc<dyn FlowSensor>) -> Result<(), InternalError>;

    async fn unregister_sensor(&self, flow_scope: &FlowScope) -> Result<(), InternalError>;

    async fn dispatch_input_flow_success(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        activation_cause: FlowActivationCause,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
