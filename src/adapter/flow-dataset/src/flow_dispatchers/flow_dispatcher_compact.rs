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
use kamu_datasets::{DatasetEntryService, DependencyGraphService};
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_COMPACT,
    FlowConfigRuleCompact,
    trigger_hard_compaction_flow_for_own_downstream_datasets,
    trigger_transform_flow_for_all_downstream_datasets,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
#[dill::meta(fs::FlowDispatcherMeta {
    flow_type: FLOW_TYPE_DATASET_COMPACT,
})]
pub struct FlowDispatcherCompact {
    flow_trigger_service: Arc<dyn fs::FlowTriggerService>,
    flow_query_service: Arc<dyn fs::FlowQueryService>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherCompact {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

        let mut max_slice_size: Option<u64> = None;
        let mut max_slice_records: Option<u64> = None;
        let mut keep_metadata_only = false;

        if let Some(config_snapshot) = maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleCompact::TYPE_ID
        {
            let compaction_rule = FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
            max_slice_size = compaction_rule.max_slice_size();
            max_slice_records = compaction_rule.max_slice_records();
            keep_metadata_only =
                matches!(compaction_rule, FlowConfigRuleCompact::MetadataOnly { .. });
        }

        Ok(ats::LogicalPlanDatasetHardCompact {
            dataset_id: dataset_id.clone(),
            max_slice_size,
            max_slice_records,
            keep_metadata_only,
        }
        .into_logical_plan())
    }

    async fn propagate_success(
        &self,
        flow_binding: &fs::FlowBinding,
        trigger_instance: fs::FlowTriggerInstance,
        maybe_config_snapshot: Option<fs::FlowConfigurationRule>,
    ) -> Result<(), InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

        if let Some(config_snapshot) = &maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleCompact::TYPE_ID
        {
            let compaction_rule = FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
            if compaction_rule.recursive() {
                trigger_hard_compaction_flow_for_own_downstream_datasets(
                    self.dataset_entry_service.as_ref(),
                    self.dependency_graph_service.as_ref(),
                    self.flow_query_service.as_ref(),
                    &dataset_id,
                    trigger_instance,
                )
                .await
            } else {
                // Nothing to do here, non-recursive compaction
                Ok(())
            }
        } else {
            // Trigger transform flow for all downstream datasets ...
            //   .. they will all explicitly break, and we need this visibility
            trigger_transform_flow_for_all_downstream_datasets(
                self.dependency_graph_service.as_ref(),
                self.flow_trigger_service.as_ref(),
                self.flow_query_service.as_ref(),
                flow_binding,
                trigger_instance,
            )
            .await
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
