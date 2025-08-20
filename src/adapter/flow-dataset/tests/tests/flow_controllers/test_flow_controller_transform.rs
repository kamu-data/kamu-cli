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
use kamu_adapter_task_dataset::{LogicalPlanDatasetUpdate, TaskResultDatasetUpdate};
use kamu_core::{PullResult, PullResultUpToDate};
use kamu_datasets::{DatasetIncrementQueryService, DatasetIntervalIncrement};
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use kamu_flow_system_inmem::InMemoryFlowEventStore;
use kamu_task_system::LogicalPlan;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_register_sensor() {
    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_flow_sensor_dispatcher = MockFlowSensorDispatcher::with_register_sensor_for_scope(
        FlowScopeDataset::make_scope(&foo_dataset_id),
    );
    mock_flow_sensor_dispatcher
        .expect_find_sensor()
        .times(1)
        .returning(|_| None);

    let harness = FlowControllerTransformHarness::with_overrides(
        MockDatasetIncrementQueryService::new(),
        mock_flow_sensor_dispatcher,
    );

    let transform_binding = transform_dataset_binding(&foo_dataset_id);

    harness
        .controller
        .ensure_flow_sensor(&transform_binding, Utc::now(), ReactiveRule::empty())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_logical_plan() {
    let harness = FlowControllerTransformHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let transform_flow = harness
        .make_transform_flow(FlowID::new(1), &foo_dataset_id)
        .await;

    let logical_plan = harness.build_task_logical_plan(&transform_flow).await;
    pretty_assertions::assert_eq!(
        logical_plan,
        LogicalPlan {
            plan_type: LogicalPlanDatasetUpdate::TYPE_ID.to_string(),
            payload: json!({
                "dataset_id": foo_dataset_id.to_string(),
                "fetch_uncacheable": false,
            }),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_propagate_success_up_to_date_causes_no_interaction() {
    let harness = FlowControllerTransformHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let transform_flow = harness
        .make_transform_flow(FlowID::new(1), &foo_dataset_id)
        .await;

    harness
        .propagate_success(
            &transform_flow,
            PullResult::UpToDate(PullResultUpToDate::Transform),
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_transform_propagate_success_updated_notifies_dispatcher() {
    const FLOW_ID: u64 = 1;
    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");

    let mock_dataset_increment_service =
        MockDatasetIncrementQueryService::with_increment_between_for_args(
            foo_dataset_id.clone(),
            None,
            new_head.clone(),
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            },
        );

    let mock_flow_sensor_dispatcher =
        MockFlowSensorDispatcher::with_dispatch_for_resource_update_cause(
            transform_dataset_binding(&foo_dataset_id),
            FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                changes: ResourceChanges::NewData {
                    blocks_added: 1,
                    records_added: 5,
                    new_watermark: None,
                },
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                details: json!({
                    "dataset_id": foo_dataset_id.to_string(),
                    "new_head": new_head.to_string(),
                    "old_head_maybe": null,
                    "source": {
                        "UpstreamFlow": {
                            "flow_id": FLOW_ID,
                            "flow_type": FLOW_TYPE_DATASET_TRANSFORM,
                            "maybe_flow_config_snapshot": null,
                        }
                    }
                }),
            },
        );

    let harness = FlowControllerTransformHarness::with_overrides(
        mock_dataset_increment_service,
        mock_flow_sensor_dispatcher,
    );

    let transform_flow = harness
        .make_transform_flow(FlowID::new(FLOW_ID), &foo_dataset_id)
        .await;

    harness
        .propagate_success(
            &transform_flow,
            PullResult::Updated {
                old_head: None,
                new_head,
            },
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowControllerTransformHarness {
    controller: Arc<FlowControllerTransform>,
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl FlowControllerTransformHarness {
    fn new() -> Self {
        Self::with_overrides(
            MockDatasetIncrementQueryService::new(),
            MockFlowSensorDispatcher::new(),
        )
    }

    fn with_overrides(
        mock_dataset_increment_service: MockDatasetIncrementQueryService,
        mock_flow_sensor_dispatcher: MockFlowSensorDispatcher,
    ) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<FlowControllerTransform>()
            .add::<InMemoryFlowEventStore>()
            .add_value(mock_dataset_increment_service)
            .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
            .add_value(mock_flow_sensor_dispatcher)
            .bind::<dyn FlowSensorDispatcher, MockFlowSensorDispatcher>();

        let catalog = b.build();
        Self {
            controller: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
        }
    }

    async fn make_transform_flow(&self, flow_id: FlowID, dataset_id: &odf::DatasetID) -> FlowState {
        let mut flow = Flow::new(
            Utc::now(),
            flow_id,
            transform_dataset_binding(dataset_id),
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

    async fn propagate_success(&self, flow_state: &FlowState, pull_result: PullResult) {
        let task_result = TaskResultDatasetUpdate { pull_result }.into_task_result();

        self.controller
            .propagate_success(flow_state, &task_result, Utc::now())
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
