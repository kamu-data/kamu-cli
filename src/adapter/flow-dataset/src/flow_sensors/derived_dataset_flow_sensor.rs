// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system as fs;

use crate::{
    // FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_TRANSFORM,
    // FlowConfigRuleCompact,
    // FlowConfigRuleReset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DerivedDatasetFlowSensor {
    flow_scope: fs::FlowScope,
    sensitive_dataset_ids: HashSet<odf::DatasetID>,
    batching_rule: fs::BatchingRule,
}

impl DerivedDatasetFlowSensor {
    pub fn new(
        dataset_id: odf::DatasetID,
        input_dataset_ids: Vec<odf::DatasetID>,
        batching_rule: fs::BatchingRule,
    ) -> Self {
        Self {
            flow_scope: fs::FlowScope::Dataset { dataset_id },
            sensitive_dataset_ids: HashSet::from_iter(input_dataset_ids),
            batching_rule,
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
    ) -> Result<(), InternalError> {
        let target_flow_binding = fs::FlowBinding::for_dataset(
            self.flow_scope.dataset_id().unwrap().clone(),
            FLOW_TYPE_DATASET_TRANSFORM,
        );
        flow_run_service
            .run_flow_automatically(
                &target_flow_binding,
                activation_cause.clone(),
                Some(fs::FlowTriggerRule::Batching(self.batching_rule)),
                None,
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }

    /*
    async fn run_metadata_only_compaction_flow(
        &self,
        activation_cause: &fs::FlowActivationCause,
        flow_run_service: &dyn fs::FlowRunService,
    ) -> Result<(), InternalError> {
        let target_flow_binding = fs::FlowBinding::for_dataset(
            self.flow_scope.dataset_id().unwrap().clone(),
            FLOW_TYPE_DATASET_COMPACT,
        );
        flow_run_service
            .run_flow_automatically(
                &target_flow_binding,
                activation_cause.clone(),
                None,
                Some(FlowConfigRuleCompact::MetadataOnly { recursive: true }.into_flow_config()),
                None,
            )
            .await
            .int_err()?;

        Ok(())
    }
    */
}

#[async_trait::async_trait]
impl fs::FlowSensor for DerivedDatasetFlowSensor {
    fn flow_scope(&self) -> &fs::FlowScope {
        &self.flow_scope
    }

    fn get_sensitive_datasets(&self) -> Vec<odf::DatasetID> {
        self.sensitive_dataset_ids.iter().cloned().collect()
    }

    async fn on_sensitized(
        &self,
        catalog: &dill::Catalog,
        input_flow_binding: &fs::FlowBinding,
        activation_cause: &fs::FlowActivationCause,
    ) -> Result<(), InternalError> {
        // First we should ensure we are sensitized with a valid input dataset
        let input_dataset_id = input_flow_binding.get_dataset_id_or_die()?;
        if !self.sensitive_dataset_ids.contains(&input_dataset_id) {
            return Err(InternalError::new(format!(
                "Flow sensor {:?} received an input dataset {} that is not in the sensitivity list",
                self.flow_scope, input_dataset_id
            )));
        }

        // Depending on what happened to the input dataset,
        // we may need to trigger a specific flow run
        if let fs::FlowActivationCause::DatasetUpdate(dataset_update) = activation_cause {
            // Extract flow run service
            let flow_run_service = catalog.get_one::<dyn fs::FlowRunService>().unwrap();

            match dataset_update.changes {
                // Dataset was normally updated, there is new data available to process
                fs::DatasetChanges::NewData { .. } => {
                    // Trigger transform flow for the target dataset
                    self.run_transform_flow(activation_cause, flow_run_service.as_ref())
                        .await?;
                }
                // Note: will not be activated for now
                fs::DatasetChanges::Breaking => {
                    /*
                    // Trigger metadata-only compaction
                    let maybe_config_snapshot = dataset_update.source.maybe_flow_config_snapshot();
                    if let Some(config_snapshot) = maybe_config_snapshot {
                        match config_snapshot.rule_type.as_str() {
                            FlowConfigRuleCompact::TYPE_ID => {
                                let compaction_rule =
                                    FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
                                if compaction_rule.recursive() {
                                    self.run_metadata_only_compaction_flow(
                                        activation_cause,
                                        flow_run_service.as_ref(),
                                    )
                                    .await?;
                                }
                            }
                            FlowConfigRuleReset::TYPE_ID => {
                                let reset_rule =
                                    FlowConfigRuleReset::from_flow_config(config_snapshot)?;
                                if reset_rule.recursive {
                                    self.run_metadata_only_compaction_flow(
                                        activation_cause,
                                        flow_run_service.as_ref(),
                                    )
                                    .await?;
                                }
                            }
                            _ => {
                                return Err(InternalError::new(format!(
                                    "Unsupported flow config rule type: {}",
                                    config_snapshot.rule_type
                                )));
                            }
                        }
                    } else if input_flow_binding.flow_type == FLOW_TYPE_DATASET_COMPACT {
                        // Trigger transform flow for all downstream datasets ...
                        //   .. they will all explicitly break, and we need this visibility
                        self.run_metadata_only_compaction_flow(
                            activation_cause,
                            flow_run_service.as_ref(),
                        )
                        .await?;
                    }*/
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
