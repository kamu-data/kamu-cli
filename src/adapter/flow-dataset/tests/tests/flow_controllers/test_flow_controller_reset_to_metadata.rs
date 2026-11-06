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
use kamu_accounts::TEST_ACCOUNT_ID;
use kamu_adapter_flow_dataset::*;
use kamu_adapter_task_dataset::{
    LogicalPlanDatasetResetToMetadata,
    TaskResultDatasetResetToMetadata,
};
use kamu_core::CompactionResult;
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_task_system::LogicalPlan;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use serde_json::json;
use time_source::SystemTimeSourceDefault;
use wakeup_listener::WakeupListenerMetrics;

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

    let mut mock_flow_sensor_dispatcher =
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
    FlowControllerResetToMetadataHarness::expect_own_sensor(
        &mut mock_flow_sensor_dispatcher,
        &foo_dataset_id,
        None,
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

#[test_log::test(tokio::test)]
async fn test_reset_to_metadata_propagate_success_reactivates_own_sensor() {
    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_own_sensor = MockFlowSensor::new();
    mock_own_sensor
        .expect_on_activated()
        .times(1)
        .returning(|_, _| Ok(()));

    let mut mock_flow_sensor_dispatcher = MockFlowSensorDispatcher::new();
    mock_flow_sensor_dispatcher
        .expect_dispatch_input_flow_success()
        .returning(|_, _, _| Ok(()));
    FlowControllerResetToMetadataHarness::expect_own_sensor(
        &mut mock_flow_sensor_dispatcher,
        &foo_dataset_id,
        Some(mock_own_sensor),
    );

    let harness = FlowControllerResetToMetadataHarness::with_overrides(mock_flow_sensor_dispatcher);

    let reset_to_metadata_flow = harness
        .make_reset_to_metadata_flow(FlowID::new(1), &foo_dataset_id)
        .await;

    harness
        .propagate_success(
            &reset_to_metadata_flow,
            CompactionResult::Success {
                old_head: odf::Multihash::from_digest_sha3_256(b"old_head"),
                new_head: odf::Multihash::from_digest_sha3_256(b"new_head"),
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
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryWakeupHub>()
            .add::<WakeupListenerMetrics>()
            .add_value(mock_flow_sensor_dispatcher)
            .bind::<dyn FlowSensorDispatcher, MockFlowSensorDispatcher>()
            .add::<FakeDatasetEntryService>()
            .add::<SystemTimeSourceDefault>();

        let catalog = b.build();
        Self {
            controller: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
        }
    }

    fn expect_own_sensor(
        mock_flow_sensor_dispatcher: &mut MockFlowSensorDispatcher,
        dataset_id: &odf::DatasetID,
        own_sensor: Option<MockFlowSensor>,
    ) {
        let own_scope = FlowScopeDataset::make_scope(dataset_id);
        let own_sensor = own_sensor.map(|sensor| Arc::new(sensor) as Arc<dyn FlowSensor>);
        mock_flow_sensor_dispatcher
            .expect_find_sensor()
            .withf(move |scope| *scope == own_scope)
            .times(1)
            .return_once(move |_| own_sensor);
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
                initiator_account_id: TEST_ACCOUNT_ID.clone(),
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
