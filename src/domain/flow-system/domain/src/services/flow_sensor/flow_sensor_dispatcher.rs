// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::InternalError;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait FlowSensorDispatcher: Send + Sync {
    async fn find_sensor(&self, flow_scope: &FlowScope) -> Option<Arc<dyn FlowSensor>>;

    async fn register_sensor(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
        flow_sensor: Arc<dyn FlowSensor>,
    ) -> Result<(), InternalError>;

    async fn unregister_sensor(&self, flow_scope: &FlowScope) -> Result<(), InternalError>;

    async fn refresh_sensor_dependencies(
        &self,
        flow_scope: &FlowScope,
        catalog: &dill::Catalog,
    ) -> Result<(), InternalError>;

    async fn dispatch_input_flow_success(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        activation_cause: FlowActivationCause,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "testing")]
impl MockFlowSensorDispatcher {
    pub fn with_register_sensor_for_scope(flow_scope: FlowScope) -> Self {
        let mut mock = MockFlowSensorDispatcher::new();
        mock.expect_register_sensor()
            .withf(move |_, _, sensor| sensor.flow_scope() == &flow_scope)
            .returning(|_, _, _| Ok(()));
        mock
    }

    pub fn with_dispatch_for_resource_update_cause(
        input_flow_binding: FlowBinding,
        activation_cause: FlowActivationCauseResourceUpdate,
    ) -> Self {
        use std::assert_matches::assert_matches;

        let mut mock = MockFlowSensorDispatcher::new();
        mock.expect_dispatch_input_flow_success()
            .withf(move |_, binding, cause| {
                assert_eq!(*binding, input_flow_binding);
                assert_matches!(cause, FlowActivationCause::ResourceUpdate(c)
                    if c.changes == activation_cause.changes &&
                        c.resource_type == activation_cause.resource_type &&
                        c.details == activation_cause.details
                );
                true
            })
            .returning(|_, _, _| Ok(()));
        mock
    }

    pub fn with_refresh_sensor_dependencies(flow_scope: FlowScope) -> Self {
        let mut mock = MockFlowSensorDispatcher::new();
        mock.expect_refresh_sensor_dependencies()
            .withf(move |scope, _| scope == &flow_scope)
            .returning(|_, _| Ok(()));
        mock
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
