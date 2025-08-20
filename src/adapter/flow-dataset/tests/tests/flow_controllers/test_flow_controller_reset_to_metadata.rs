// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use kamu_accounts::DEFAULT_ACCOUNT_ID;
use kamu_adapter_flow_dataset::*;
use kamu_adapter_task_dataset::{
    LogicalPlanDatasetResetToMetadata,
    TaskResultDatasetResetToMetadata,
};
use kamu_core::CompactionResult;
use kamu_flow_system::*;
use kamu_flow_system_inmem::InMemoryFlowEventStore;
use kamu_task_system::LogicalPlan;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_to_metadata_logical_plan() {
    let harness = FlowControllerResetToMetadataHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let reset_to_metadata_flow = harness
        .make_reset_to_metadata_flow(FlowID::new(1), &foo_dataset_id)
        .await;

    let logical_plan = harness
        .build_task_logical_plan(&reset_to_metadata_flow)
        .await;
    pretty_assertions::assert_eq!(
        logical_plan,
        LogicalPlan {
            plan_type: LogicalPlanDatasetResetToMetadata::TYPE_ID.to_string(),
            payload: json!({
                "dataset_id": foo_dataset_id.to_string(),
            }),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reset_to_metadata_propagate_success_untouched_causes_no_interaction() {
    let harness = FlowControllerResetToMetadataHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let reset_to_metadata_flow = harness
        .make_reset_to_metadata_flow(FlowID::new(1), &foo_dataset_id)
        .await;

    harness
        .propagate_success(&reset_to_metadata_flow, CompactionResult::NothingToDo)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_compact_propagate_success_compacted_notifies_dispatcher() {
    const FLOW_ID: u64 = 1;
    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let old_head = odf::Multihash::from_digest_sha3_256(b"old_head");
    let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");

    let mock_flow_sensor_dispatcher =
        MockFlowSensorDispatcher::with_dispatch_for_resource_update_cause(
            reset_to_metadata_dataset_binding(&foo_dataset_id),
            FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                changes: ResourceChanges::Breaking,
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                details: json!({
                    "dataset_id": foo_dataset_id.to_string(),
                    "new_head": new_head.to_string(),
                    "old_head_maybe": old_head.to_string(),
                    "source": {
                        "UpstreamFlow": {
                            "flow_id": FLOW_ID,
                            "flow_type": FLOW_TYPE_DATASET_RESET_TO_METADATA,
                            "maybe_flow_config_snapshot": null,
                        }
                    }
                }),
            },
        );

    let harness = FlowControllerResetToMetadataHarness::with_overrides(mock_flow_sensor_dispatcher);

    let reset_to_metadata_flow = harness
        .make_reset_to_metadata_flow(FlowID::new(FLOW_ID), &foo_dataset_id)
        .await;

    harness
        .propagate_success(
            &reset_to_metadata_flow,
            CompactionResult::Success {
                old_head,
                new_head,
                old_num_blocks: 50,
                new_num_blocks: 4,
            },
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowControllerResetToMetadataHarness {
    controller: Arc<FlowControllerResetToMetadata>,
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl FlowControllerResetToMetadataHarness {
    fn new() -> Self {
        Self::with_overrides(MockFlowSensorDispatcher::new())
    }

    fn with_overrides(mock_flow_sensor_dispatcher: MockFlowSensorDispatcher) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<FlowControllerResetToMetadata>()
            .add::<InMemoryFlowEventStore>()
            .add_value(mock_flow_sensor_dispatcher)
            .bind::<dyn FlowSensorDispatcher, MockFlowSensorDispatcher>();

        let catalog = b.build();
        Self {
            controller: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
        }
    }

    async fn make_reset_to_metadata_flow(
        &self,
        flow_id: FlowID,
        dataset_id: &odf::DatasetID,
    ) -> FlowState {
        let mut flow = Flow::new(
            Utc::now(),
            flow_id,
            reset_to_metadata_dataset_binding(dataset_id),
            FlowActivationCause::Manual(FlowActivationCauseManual {
                activation_time: Utc::now(),
                initiator_account_id: DEFAULT_ACCOUNT_ID.clone(),
            }),
            None,
            None,
        );
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
        flow.into()
    }

    async fn build_task_logical_plan(&self, flow_state: &FlowState) -> LogicalPlan {
        self.controller
            .build_task_logical_plan(flow_state)
            .await
            .unwrap()
    }

    async fn propagate_success(
        &self,
        flow_state: &FlowState,
        compaction_metadata_only_result: CompactionResult,
    ) {
        let task_result = TaskResultDatasetResetToMetadata {
            compaction_metadata_only_result,
        }
        .into_task_result();

        self.controller
            .propagate_success(flow_state, &task_result, Utc::now())
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
