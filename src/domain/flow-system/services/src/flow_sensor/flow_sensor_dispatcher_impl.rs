// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    FlowActivationCause,
    FlowBinding,
    FlowScope,
    FlowScopeRemovalHandler,
    FlowSensor,
    FlowSensorDispatcher,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowSensorDispatcherImpl {
    state: Arc<tokio::sync::RwLock<State>>,
}

#[dill::component(pub)]
#[dill::interface(dyn FlowSensorDispatcher)]
#[dill::interface(dyn FlowScopeRemovalHandler)]
#[dill::scope(dill::Singleton)]
impl FlowSensorDispatcherImpl {
    pub fn new() -> Self {
        Self {
            state: Arc::new(tokio::sync::RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    sensors: HashMap<FlowScope, Arc<dyn FlowSensor>>,
    sensitive_scopes_by_input_scope: HashMap<FlowScope, HashSet<FlowScope>>,
    sensitive_to_by_output_scope: HashMap<FlowScope, HashSet<FlowScope>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSensorDispatcher for FlowSensorDispatcherImpl {
    async fn register_sensor(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
        flow_sensor: Arc<dyn FlowSensor>,
    ) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        // Get the flow scope for this sensor
        let flow_scope = flow_sensor.flow_scope().clone();

        // Assert that this flow scope is not already registered
        if state.sensors.contains_key(&flow_scope) {
            return Err(InternalError::new(format!(
                "Flow sensor with scope {flow_scope:?} is already registered",
            )));
        }

        // Get scopes this sensor is interested in
        let sensitive_to_scopes = flow_sensor.get_sensitive_to_scopes(catalog).await;

        // Remember the sensor's interest in these scopes, as it might get updated later
        state.sensitive_to_by_output_scope.insert(
            flow_scope.clone(),
            sensitive_to_scopes.iter().cloned().collect(),
        );

        // Register sensor for each scope it's interested in
        for sensitive_to_scope in sensitive_to_scopes {
            state
                .sensitive_scopes_by_input_scope
                .entry(sensitive_to_scope)
                .or_insert_with(HashSet::new)
                .insert(flow_scope.clone());
        }

        // Store the sensor
        state.sensors.insert(flow_scope, flow_sensor.clone());

        // Notify the sensor that it has been activated
        flow_sensor.on_activated(catalog, activation_time).await?;

        Ok(())
    }

    async fn unregister_sensor(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        // Try to remove the sensor - if it exists, clean up its scope mappings
        state.sensors.remove(flow_scope);

        // Get scopes this sensor was interested in from the removed sensor
        if let Some(sensitive_to_scopes) = state.sensitive_to_by_output_scope.remove(flow_scope) {
            // Remove sensor from mappings
            for sensitive_to_scope in sensitive_to_scopes {
                if let Some(sensor_set) = state
                    .sensitive_scopes_by_input_scope
                    .get_mut(&sensitive_to_scope)
                {
                    sensor_set.remove(flow_scope);
                    if sensor_set.is_empty() {
                        state.sensitive_scopes_by_input_scope.remove(flow_scope);
                    }
                }
            }
        }

        Ok(())
    }

    async fn refresh_sensor_dependencies(
        &self,
        flow_scope: &FlowScope,
        catalog: &dill::Catalog,
    ) -> Result<(), InternalError> {
        let mut state = self.state.write().await;
        if let Some(sensor) = state.sensors.get(flow_scope) {
            let latest_dependencies: HashSet<FlowScope> = sensor
                .get_sensitive_to_scopes(catalog)
                .await
                .into_iter()
                .collect();

            // Get the current dependencies for this sensor
            let current_dependencies = state
                .sensitive_to_by_output_scope
                .get(flow_scope)
                .cloned()
                .unwrap_or_default();

            // Find dependencies to add (in latest but not in current)
            let dependencies_to_add: HashSet<_> = latest_dependencies
                .difference(&current_dependencies)
                .cloned()
                .collect();

            // Find dependencies to remove (in current but not in latest)
            let dependencies_to_remove: HashSet<_> = current_dependencies
                .difference(&latest_dependencies)
                .cloned()
                .collect();

            // Add new dependencies to sensitive_scopes_by_input_scope
            for new_dependency in dependencies_to_add {
                state
                    .sensitive_scopes_by_input_scope
                    .entry(new_dependency)
                    .or_insert_with(HashSet::new)
                    .insert(flow_scope.clone());
            }

            // Remove old dependencies from sensitive_scopes_by_input_scope
            for old_dependency in dependencies_to_remove {
                if let Some(sensor_set) = state
                    .sensitive_scopes_by_input_scope
                    .get_mut(&old_dependency)
                {
                    sensor_set.remove(flow_scope);
                    // Clean up empty sets
                    if sensor_set.is_empty() {
                        state
                            .sensitive_scopes_by_input_scope
                            .remove(&old_dependency);
                    }
                }
            }

            // Update the stored dependencies for this sensor
            state
                .sensitive_to_by_output_scope
                .insert(flow_scope.clone(), latest_dependencies);
        }

        Ok(())
    }

    async fn dispatch_input_flow_success(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        activation_cause: FlowActivationCause,
    ) -> Result<(), InternalError> {
        // Get sensors interested in the input scope
        let sensors_to_notify: Vec<Arc<dyn FlowSensor>> = {
            let state = self.state.read().await;

            if let Some(interested_flow_scopes) = state
                .sensitive_scopes_by_input_scope
                .get(&input_flow_binding.scope)
            {
                interested_flow_scopes
                    .iter()
                    .filter_map(|flow_scope| state.sensors.get(flow_scope))
                    .cloned()
                    .collect()
            } else {
                // No sensors interested in this input scope
                return Ok(());
            }
        };

        // Notify each interested sensor
        for sensor in sensors_to_notify {
            sensor
                .on_sensitized(catalog, input_flow_binding, &activation_cause)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowScopeRemovalHandler for FlowSensorDispatcherImpl {
    async fn handle_flow_scope_removal(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        // Remove the sensor associated with this flow scope, if it exists
        state.sensors.remove(flow_scope);

        // Clean up this sensor's associations with all scopes
        if let Some(sensitive_to_scopes) = state.sensitive_to_by_output_scope.remove(flow_scope) {
            for other_scope in sensitive_to_scopes {
                if let Some(sensor_set) =
                    state.sensitive_scopes_by_input_scope.get_mut(&other_scope)
                {
                    sensor_set.remove(flow_scope);
                    // Clean up empty sets
                    if sensor_set.is_empty() {
                        state.sensitive_scopes_by_input_scope.remove(&other_scope);
                    }
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
