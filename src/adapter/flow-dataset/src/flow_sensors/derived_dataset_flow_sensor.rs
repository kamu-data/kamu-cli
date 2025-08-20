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
use kamu_core::TransformStatus;
use kamu_datasets::*;
use kamu_flow_system::{self as fs, FlowSensorSensitizationError};

use crate::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_TYPE_DATASET_RESET_TO_METADATA,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowScopeDataset,
    TransformFlowEvaluator,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DerivedDatasetFlowSensor {
    flow_scope: fs::FlowScope,
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

impl DerivedDatasetFlowSensor {
    pub fn new(dataset_id: &odf::DatasetID, reactive_rule: fs::ReactiveRule) -> Self {
        Self {
            flow_scope: FlowScopeDataset::make_scope(dataset_id),
            state: Arc::new(Mutex::new(State::new(reactive_rule))),
        }
    }

    #[inline]
    fn reactive_rule(&self) -> fs::ReactiveRule {
        self.state.lock().unwrap().reactive_rule
    }

    async fn run_transform_flow(
        &self,
        activation_causes: Vec<fs::FlowActivationCause>,
        flow_run_service: &dyn fs::FlowRunService,
    ) -> Result<(), InternalError> {
        let flow_binding =
            fs::FlowBinding::new(FLOW_TYPE_DATASET_TRANSFORM, self.flow_scope.clone());

        flow_run_service
            .run_flow_automatically(
                &flow_binding,
                activation_causes,
                Some(fs::FlowTriggerRule::Reactive(self.reactive_rule())),
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
            .run_flow_automatically(
                &target_flow_binding,
                vec![activation_cause.clone()],
                None,
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }

    async fn get_transform_status(
        &self,
        catalog: &dill::Catalog,
    ) -> Result<TransformStatus, InternalError> {
        let transform_flow_evaluator = catalog.get_one::<dyn TransformFlowEvaluator>().unwrap();

        let dataset_id = FlowScopeDataset::new(&self.flow_scope).dataset_id();
        transform_flow_evaluator
            .evaluate_transform_status(&dataset_id)
            .await
    }
}

#[async_trait::async_trait]
impl fs::FlowSensor for DerivedDatasetFlowSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.flow_scope
    }

    async fn get_sensitive_to_scopes(&self, catalog: &dill::Catalog) -> Vec<fs::FlowScope> {
        let dependency_graph_service = catalog.get_one::<dyn DependencyGraphService>().unwrap();
        let dataset_id = FlowScopeDataset::new(&self.flow_scope).dataset_id();

        use futures::StreamExt;
        let upstream_dataset_ids: Vec<_> = dependency_graph_service
            .get_upstream_dependencies(&dataset_id)
            .await
            .collect()
            .await;

        upstream_dataset_ids
            .iter()
            .map(FlowScopeDataset::make_scope)
            .collect()
    }

    fn update_rule(&self, new_rule: fs::ReactiveRule) {
        let mut state = self.state.lock().unwrap();
        state.reactive_rule = new_rule;
    }

    async fn on_activated(
        &self,
        catalog: &dill::Catalog,
        activation_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        tracing::info!(?self.flow_scope, "Derived dataset flow sensor activated");

        // Evaluate if there is a need to run the transform flow
        let transform_status = self.get_transform_status(catalog).await?;
        match transform_status {
            // The dataset is up to date, no action needed
            TransformStatus::UpToDate => {
                tracing::info!("Derived dataset is up to date, no flow sensor action needed",);
                return Ok(());
            }

            // The input datasets have advanced since the last transform run (if any)
            TransformStatus::NewInputDataAvailable { input_advancements } => {
                tracing::info!(
                    "Derived dataset has new input data available, triggering transform flow",
                );

                let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();

                // Use increment of each input dataset to create activation causes
                let dataset_increment_query_service = catalog
                    .get_one::<dyn DatasetIncrementQueryService>()
                    .unwrap();

                let mut all_activation_causes = Vec::with_capacity(input_advancements.len());
                for input_advancement in input_advancements {
                    // Compute increment for 1 input dataset
                    let dataset_increment = dataset_increment_query_service
                        .get_increment_between(
                            &input_advancement.dataset_id,
                            input_advancement.prev_block_hash.as_ref(),
                            input_advancement.new_block_hash.as_ref().unwrap(),
                        )
                        .await
                        .int_err()?;

                    // Form activation cause
                    let activation_cause = fs::FlowActivationCause::ResourceUpdate(
                        fs::FlowActivationCauseResourceUpdate {
                            activation_time,
                            changes: fs::ResourceChanges::NewData {
                                blocks_added: dataset_increment.num_blocks,
                                records_added: dataset_increment.num_records,
                                new_watermark: dataset_increment.updated_watermark,
                            },
                            resource_type: DATASET_RESOURCE_TYPE.to_string(),
                            details: serde_json::to_value(DatasetResourceUpdateDetails {
                                dataset_id: input_advancement.dataset_id,
                                source: DatasetUpdateSource::ExternallyDetectedChange,
                                new_head: input_advancement.new_block_hash.unwrap(),
                                old_head_maybe: input_advancement.prev_block_hash.clone(),
                            })
                            .int_err()?,
                        },
                    );
                    all_activation_causes.push(activation_cause);
                }

                // Trigger transform flow with all activation causes
                self.run_transform_flow(all_activation_causes, flow_run_service.as_ref())
                    .await?;
            }
        }

        Ok(())
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), FlowSensorSensitizationError> {
        tracing::info!(?self.flow_scope, ?input_flow_binding, ?activation_cause, "Derived dataset flow sensor sensitized");

        // Extract dataset IDs
        let input_dataset_id = FlowScopeDataset::new(&input_flow_binding.scope).dataset_id();
        let own_dataset_id = FlowScopeDataset::new(&self.flow_scope).dataset_id();

        // First we should ensure we are sensitized with a valid input dataset
        let dependency_graph_service = catalog.get_one::<dyn DependencyGraphService>().unwrap();
        if !dependency_graph_service
            .dependency_exists(&input_dataset_id, &own_dataset_id)
            .await?
        {
            return Err(FlowSensorSensitizationError::InvalidInputFlowBinding {
                binding: input_flow_binding.clone(),
            });
        }

        if let fs::FlowActivationCause::ResourceUpdate(update) = activation_cause {
            match update.changes {
                // Input dataset was normally updated, there is new data available to process
                fs::ResourceChanges::NewData { .. } => {
                    // Trigger transform flow for the target dataset
                    let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();
                    self.run_transform_flow(
                        vec![activation_cause.clone()],
                        flow_run_service.as_ref(),
                    )
                    .await?;
                }
                // Input dataset was updated with a breaking change.
                // With auto-updates only, we can reset data and keep metadata only.
                fs::ResourceChanges::Breaking => {
                    // Trigger metadata-only compaction, if recovery is enabled
                    match self.reactive_rule().for_breaking_change {
                        fs::BreakingChangeRule::Recover => {
                            let flow_run_service =
                                catalog.get_one::<dyn fs::FlowRunService>().unwrap();
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
            ))
            .into());
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
