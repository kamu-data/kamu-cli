// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_flow_system::*;
use kamu_flow_system_services::FlowSensorDispatcherImpl;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unregistering_upstream_sensor_keeps_downstream_routing() {
    let harness = FlowSensorDispatcherHarness::new();

    // "a" reacts to "x", "c" reacts to "a"
    let sensor_a = harness.register_sensor("a", &["x"]).await;
    let sensor_c = harness.register_sensor("c", &["a"]).await;

    harness.unregister_sensor("a").await;

    harness.dispatch_success_of("a").await;
    harness.dispatch_success_of("x").await;

    assert_eq!(sensor_c.sensitized_by(), vec![scope("a")]);
    assert_eq!(sensor_a.sensitized_by(), Vec::<FlowScope>::new());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unregistering_sensor_drops_its_routing() {
    let harness = FlowSensorDispatcherHarness::new();

    let sensor_a = harness.register_sensor("a", &["x"]).await;
    let sensor_b = harness.register_sensor("b", &["x"]).await;

    harness.unregister_sensor("a").await;
    harness.dispatch_success_of("x").await;

    assert_eq!(sensor_a.sensitized_by(), Vec::<FlowScope>::new());
    assert_eq!(sensor_b.sensitized_by(), vec![scope("x")]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const TEST_FLOW_TYPE: &str = "test.flow";

fn scope(name: &str) -> FlowScope {
    FlowScope::new(serde_json::json!({ "type": "test", "name": name }))
}

struct FlowSensorDispatcherHarness {
    catalog: dill::Catalog,
    dispatcher: FlowSensorDispatcherImpl,
}

impl FlowSensorDispatcherHarness {
    fn new() -> Self {
        Self {
            catalog: dill::CatalogBuilder::new().build(),
            dispatcher: FlowSensorDispatcherImpl::new(),
        }
    }

    async fn register_sensor(&self, name: &str, sensitive_to: &[&str]) -> Arc<RecordingSensor> {
        let sensor = Arc::new(RecordingSensor {
            flow_scope: scope(name),
            sensitive_to: sensitive_to.iter().map(|s| scope(s)).collect(),
            sensitized_by: Mutex::new(Vec::new()),
        });
        self.dispatcher
            .register_sensor(
                &self.catalog,
                FlowSensorActivation::CatchUp(Utc::now()),
                sensor.clone(),
            )
            .await
            .unwrap();
        sensor
    }

    async fn unregister_sensor(&self, name: &str) {
        self.dispatcher
            .unregister_sensor(&scope(name))
            .await
            .unwrap();
    }

    async fn dispatch_success_of(&self, name: &str) {
        self.dispatcher
            .dispatch_input_flow_success(
                &self.catalog,
                &FlowBinding::new(TEST_FLOW_TYPE, scope(name)),
                FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                    activation_time: Utc::now(),
                }),
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records the input scopes it was sensitized by.
struct RecordingSensor {
    flow_scope: FlowScope,
    sensitive_to: Vec<FlowScope>,
    sensitized_by: Mutex<Vec<FlowScope>>,
}

impl RecordingSensor {
    fn sensitized_by(&self) -> Vec<FlowScope> {
        self.sensitized_by.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl FlowSensor for RecordingSensor {
    fn flow_scope(&self) -> &FlowScope {
        &self.flow_scope
    }

    async fn get_sensitive_to_scopes(&self, _catalog: &dill::Catalog) -> Vec<FlowScope> {
        self.sensitive_to.clone()
    }

    fn update_rule(&self, _rule: ReactiveRule) {}

    async fn on_activated(
        &self,
        _catalog: &dill::Catalog,
        _activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn on_sensitized(
        &self,
        _catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        _activation_cause: &FlowActivationCause,
    ) -> Result<(), FlowSensorSensitizationError> {
        self.sensitized_by
            .lock()
            .unwrap()
            .push(input_flow_binding.scope.clone());
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
