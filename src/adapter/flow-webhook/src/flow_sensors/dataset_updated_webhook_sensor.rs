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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_adapter_flow_dataset::{DATASET_RESOURCE_TYPE, FlowScopeDataset};
use kamu_flow_system::{self as fs, FlowSensorSensitizationError};

use crate::{FLOW_TYPE_WEBHOOK_DELIVER, FlowScopeSubscription};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetUpdatedWebhookSensor {
    webhook_flow_scope: fs::FlowScope,
    dataset_id: odf::DatasetID,
    state: Arc<Mutex<State>>,
}

struct State {
    reactive_rule: fs::ReactiveRule,
}

impl State {
    fn new(reactive_rule: fs::ReactiveRule) -> Self {
        Self { reactive_rule }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetUpdatedWebhookSensor {
    pub fn new(webhook_flow_scope: fs::FlowScope, reactive_rule: fs::ReactiveRule) -> Self {
        Self {
            dataset_id: FlowScopeSubscription::new(&webhook_flow_scope)
                .maybe_dataset_id()
                .unwrap(),
            webhook_flow_scope,
            state: Arc::new(Mutex::new(State::new(reactive_rule))),
        }
    }

    #[inline]
    pub fn reactive_rule(&self) -> fs::ReactiveRule {
        self.state.lock().unwrap().reactive_rule
    }
}

#[async_trait::async_trait]
impl fs::FlowSensor for DatasetUpdatedWebhookSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.webhook_flow_scope
    }

    async fn get_sensitive_to_scopes(&self, _: &dill::Catalog) -> Vec<fs::FlowScope> {
        vec![FlowScopeDataset::make_scope(&self.dataset_id)]
    }

    fn update_rule(&self, new_rule: fs::ReactiveRule) {
        let mut state = self.state.lock().unwrap();
        state.reactive_rule = new_rule;
    }

    async fn on_activated(
        &self,
        _catalog: &dill::Catalog,
        _activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.webhook_flow_scope, "DatasetUpdatedWebhookSensor activated");
        // TODO
        Ok(())
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), FlowSensorSensitizationError> {
        tracing::info!(?self.webhook_flow_scope, ?input_flow_binding, ?activation_cause, "DatasetUpdatedWebhookSensor sensitized");

        // Ensure sensitized for right dataset id
        let input_dataset_id = FlowScopeDataset::new(&input_flow_binding.scope).dataset_id();
        if input_dataset_id != self.dataset_id {
            return Err(FlowSensorSensitizationError::InvalidInputFlowBinding {
                binding: input_flow_binding.clone(),
            });
        }

        // React to dataset updates
        if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause {
            // Decode dataset update
            if update.resource_type != DATASET_RESOURCE_TYPE {
                return Err(InternalError::new(format!(
                    "Unexpected resource type: {}",
                    update.resource_type
                ))
                .into());
            }

            // Extract reactive rule
            let reactive_rule = self.reactive_rule();

            // Skip if disabled for breaking changes
            if matches!(update.changes, fs::ResourceChanges::Breaking) {
                match reactive_rule.for_breaking_change {
                    fs::BreakingChangeRule::NoAction => {
                        tracing::warn!(
                            "Skipping breaking change for dataset {} in flow {}",
                            self.dataset_id,
                            input_flow_binding.flow_type
                        );
                        return Ok(());
                    }
                    fs::BreakingChangeRule::Recover => { /* Allow webhook flow to run */ }
                }
            }

            // Extract necessary services
            let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();

            // Trigger webhook flow
            let target_flow_binding =
                fs::FlowBinding::new(FLOW_TYPE_WEBHOOK_DELIVER, self.webhook_flow_scope.clone());
            flow_run_service
                .run_flow_automatically(
                    &target_flow_binding,
                    vec![activation_cause.clone()],
                    Some(fs::FlowTriggerRule::Reactive(reactive_rule)),
                    None,
                )
                .await
                .int_err()?;

            Ok(())
        } else {
            Err(InternalError::new(format!(
                "Invalid activation cause for DatasetUpdatedWebhookSensor: {activation_cause:?}",
            ))
            .into())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
