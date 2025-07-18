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

use internal_error::InternalError;
use kamu_flow_system::{
    FlowActivationCause,
    FlowBinding,
    FlowScope,
    FlowSensor,
    FlowSensorDispatcher,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowSensorDispatcherImpl {
    state: Arc<tokio::sync::RwLock<State>>,
}

#[dill::component(pub)]
#[dill::interface(dyn FlowSensorDispatcher)]
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
    dataset_to_sensitive_scopes: HashMap<odf::DatasetID, HashSet<FlowScope>>,
    dataset_own_scopes: HashMap<odf::DatasetID, HashSet<FlowScope>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSensorDispatcher for FlowSensorDispatcherImpl {
    async fn register_sensor(&self, flow_sensor: Arc<dyn FlowSensor>) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        // Get the flow scope for this sensor
        let flow_scope = flow_sensor.flow_scope().clone();

        // Assert that this flow scope is not already registered
        if state.sensors.contains_key(&flow_scope) {
            return Err(InternalError::new(format!(
                "Flow sensor with scope {flow_scope:?} is already registered",
            )));
        }

        // Get datasets this sensor is interested in
        let sensitive_datasets = flow_sensor.get_sensitive_datasets();

        // Register sensor for each dataset it's interested in
        for dataset_id in sensitive_datasets {
            state
                .dataset_to_sensitive_scopes
                .entry(dataset_id)
                .or_insert_with(HashSet::new)
                .insert(flow_scope.clone());
        }

        // If this sensor is scoped to a specific dataset, track that relationship
        if let Some(scope_dataset_id) = flow_scope.dataset_id() {
            state
                .dataset_own_scopes
                .entry(scope_dataset_id.clone())
                .or_insert_with(HashSet::new)
                .insert(flow_scope.clone());
        }

        // Store the sensor
        state.sensors.insert(flow_scope, flow_sensor);

        Ok(())
    }

    async fn unregister_sensor(&self, flow_scope: &FlowScope) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        // Try to remove the sensor - if it exists, clean up its dataset mappings
        if let Some(removed_sensor) = state.sensors.remove(flow_scope) {
            // Get datasets this sensor was interested in from the removed sensor
            let sensitive_datasets = removed_sensor.get_sensitive_datasets();

            // Remove sensor from dataset mappings
            for dataset_id in sensitive_datasets {
                if let Some(sensor_set) = state.dataset_to_sensitive_scopes.get_mut(&dataset_id) {
                    sensor_set.remove(flow_scope);
                    if sensor_set.is_empty() {
                        state.dataset_to_sensitive_scopes.remove(&dataset_id);
                    }
                }
            }

            // Remove from scoped sensors mapping if applicable
            if let Some(scope_dataset_id) = flow_scope.dataset_id()
                && let Some(scoped_sensor_set) = state.dataset_own_scopes.get_mut(scope_dataset_id)
            {
                scoped_sensor_set.remove(flow_scope);
                if scoped_sensor_set.is_empty() {
                    state.dataset_own_scopes.remove(scope_dataset_id);
                }
            }
        }

        Ok(())
    }

    async fn on_dataset_deleted(&self, dataset_id: &odf::DatasetID) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        // Remove the dataset from dataset_to_sensors mapping
        // This stops any future notifications for this upstream dataset
        state.dataset_to_sensitive_scopes.remove(dataset_id);

        // Get all sensors that are scoped to the deleted dataset using precomputed
        // mapping
        let sensors_to_remove = state
            .dataset_own_scopes
            .remove(dataset_id)
            .unwrap_or_default();

        // Remove sensors that are scoped to the deleted dataset
        for flow_scope in sensors_to_remove {
            if let Some(removed_sensor) = state.sensors.remove(&flow_scope) {
                // Clean up this sensor's associations with all datasets
                let sensitive_datasets = removed_sensor.get_sensitive_datasets();

                for other_dataset_id in sensitive_datasets {
                    if let Some(sensor_set) =
                        state.dataset_to_sensitive_scopes.get_mut(&other_dataset_id)
                    {
                        sensor_set.remove(&flow_scope);
                        // Clean up empty sets
                        if sensor_set.is_empty() {
                            state.dataset_to_sensitive_scopes.remove(&other_dataset_id);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn dispatch_input_flow_success(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &FlowBinding,
        activation_cause: FlowActivationCause,
    ) -> Result<(), InternalError> {
        // Check if this flow binding has a dataset ID
        let Some(dataset_id) = input_flow_binding.dataset_id() else {
            // For now, let's ignore flow bindings without dataset IDs
            // In future we might have other types of sensors for different resources
            return Ok(());
        };

        // Get sensors interested in this dataset
        let sensors_to_notify: Vec<Arc<dyn FlowSensor>> = {
            let state = self.state.read().await;

            if let Some(interested_flow_scopes) = state.dataset_to_sensitive_scopes.get(dataset_id)
            {
                interested_flow_scopes
                    .iter()
                    .filter_map(|flow_scope| state.sensors.get(flow_scope))
                    .cloned()
                    .collect()
            } else {
                // No sensors interested in this dataset
                return Ok(());
            }
        };

        // Notify each interested sensor
        for sensor in sensors_to_notify {
            sensor
                .on_sensitized(catalog, input_flow_binding, &activation_cause)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
