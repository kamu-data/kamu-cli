// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system as fs;

use crate::{FLOW_TYPE_DATASET_RESET_TO_METADATA, FLOW_TYPE_DATASET_TRANSFORM, FlowScopeDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DerivedDatasetFlowSensor {
    flow_scope: fs::FlowScope,
    sensitive_dataset_ids: HashSet<odf::DatasetID>,
    reactive_rule: fs::ReactiveRule,
}

impl DerivedDatasetFlowSensor {
    pub fn new(
        dataset_id: &odf::DatasetID,
        input_dataset_ids: Vec<odf::DatasetID>,
        reactive_rule: fs::ReactiveRule,
    ) -> Self {
        Self {
            flow_scope: FlowScopeDataset::make_scope(dataset_id),
            sensitive_dataset_ids: HashSet::from_iter(input_dataset_ids),
            reactive_rule,
        }
    }

    pub fn add_sensitive_dataset(
        &mut self,
        dataset_id: odf::DatasetID,
    ) -> Result<(), InternalError> {
        if self.sensitive_dataset_ids.contains(&dataset_id) {
            return Err(InternalError::new(format!(
                "Dataset '{dataset_id}' is already in the sensitivity list",
            )));
        }
        self.sensitive_dataset_ids.insert(dataset_id);
        Ok(())
    }

    pub fn remove_sensitive_dataset(
        &mut self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        if !self.sensitive_dataset_ids.remove(dataset_id) {
            return Err(InternalError::new(format!(
                "Dataset '{dataset_id}' is not in the sensitivity list",
            )));
        }
        Ok(())
    }

    async fn run_transform_flow(
        &self,
        activation_cause: &fs::FlowActivationCause,
        flow_run_service: &dyn fs::FlowRunService,
        with_reactive_rule: bool,
    ) -> Result<(), InternalError> {
        let target_flow_binding =
            fs::FlowBinding::new(FLOW_TYPE_DATASET_TRANSFORM, self.flow_scope.clone());
        flow_run_service
            .run_flow_automatically(
                &target_flow_binding,
                activation_cause.clone(),
                if with_reactive_rule {
                    Some(fs::FlowTriggerRule::Reactive(self.reactive_rule))
                } else {
                    None
                },
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn run_reset_to_metadata_only(
        &self,
        activation_cause: &fs::FlowActivationCause,
        flow_run_service: &dyn fs::FlowRunService,
    ) -> Result<(), InternalError> {
        let target_flow_binding =
            fs::FlowBinding::new(FLOW_TYPE_DATASET_RESET_TO_METADATA, self.flow_scope.clone());
        flow_run_service
            .run_flow_automatically(&target_flow_binding, activation_cause.clone(), None, None)
            .await
            .int_err()?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl fs::FlowSensor for DerivedDatasetFlowSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.flow_scope
    }

    fn get_sensitive_to_scopes(&self) -> Vec<fs::FlowScope> {
        self.sensitive_dataset_ids
            .iter()
            .map(FlowScopeDataset::make_scope)
            .collect()
    }

    async fn on_activated(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.flow_scope, "Derived dataset flow sensor activated");

        // Activate transform flow for the target dataset

        let flow_event_store = catalog.get_one::<dyn fs::FlowEventStore>().unwrap();
        let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();

        let flow_binding =
            fs::FlowBinding::new(FLOW_TYPE_DATASET_TRANSFORM, self.flow_scope.clone());

        let activation_cause =
            fs::FlowActivationCause::AutoPolling(fs::FlowActivationCauseAutoPolling {
                activation_time,
            });

        // If the flow had ever run, use the reactive condition,
        //  otherwise schedule unconditionally
        let flow_run_stats = flow_event_store.get_flow_run_stats(&flow_binding).await?;
        self.run_transform_flow(
            &activation_cause,
            flow_run_service.as_ref(),
            flow_run_stats.last_success_time.is_some(),
        )
        .await?;

        Ok(())
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.flow_scope, ?input_flow_binding, ?activation_cause, "Derived dataset flow sensor sensitized");

        // First we should ensure we are sensitized with a valid input dataset

        let input_dataset_id = FlowScopeDataset::new(&input_flow_binding.scope).dataset_id();
        if !self.sensitive_dataset_ids.contains(&input_dataset_id) {
            return Err(InternalError::new(format!(
                "Flow sensor {:?} received an input dataset {} that is not in the sensitivity list",
                self.flow_scope, input_dataset_id
            )));
        }

        // Depending on what happened to the input dataset,
        // we may need to trigger a specific flow run
        if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause {
            // Extract flow run service
            let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();

            match update.changes {
                // Input dataset was normally updated, there is new data available to process
                fs::ResourceChanges::NewData { .. } => {
                    // Trigger transform flow for the target dataset
                    self.run_transform_flow(activation_cause, flow_run_service.as_ref(), true)
                        .await?;
                }
                // Input dataset was updated with a breaking change.
                // With auto-updates only, we can reset data and keep metadata only.
                fs::ResourceChanges::Breaking => {
                    // Trigger metadata-only compaction, if recovery is enabled
                    match self.reactive_rule.for_breaking_change {
                        fs::BreakingChangeRule::Recover => {
                            self.run_reset_to_metadata_only(
                                activation_cause,
                                flow_run_service.as_ref(),
                            )
                            .await?;
                        }
                        fs::BreakingChangeRule::NoAction => {
                            tracing::warn!(
                                "Flow sensor {:?} received a breaking change for dataset {}, but \
                                 recovery is disabled",
                                self.flow_scope,
                                input_dataset_id
                            );
                        }
                    }
                }
            }
        } else {
            // Handle other activation causes if necessary
            return Err(InternalError::new(format!(
                "Flow sensor {:?} received unsupported activation cause: {:?}",
                self.flow_scope, activation_cause
            )));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
