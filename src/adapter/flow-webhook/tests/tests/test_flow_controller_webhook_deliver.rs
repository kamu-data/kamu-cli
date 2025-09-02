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
use kamu_accounts::{DEFAULT_ACCOUNT_ID, DEFAULT_ACCOUNT_NAME};
use kamu_adapter_flow_dataset::{DATASET_RESOURCE_TYPE, FLOW_TYPE_DATASET_INGEST};
use kamu_adapter_flow_webhook::*;
use kamu_adapter_task_webhook::LogicalPlanWebhookDeliver;
use kamu_datasets::DatasetEntry;
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_flow_system::*;
use kamu_flow_system_inmem::InMemoryFlowEventStore;
use kamu_task_system::{LogicalPlan, TaskResult};
use kamu_webhooks::{WebhookEventType, WebhookEventTypeCatalog, WebhookSubscriptionID};
use serde_json::json;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_register_sensor() {
    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let mut mock_flow_sensor_dispatcher = MockFlowSensorDispatcher::with_register_sensor_for_scope(
        FlowScopeSubscription::make_scope(subscription_id, &event_type, Some(&foo_dataset_id)),
    );
    mock_flow_sensor_dispatcher
        .expect_find_sensor()
        .times(1)
        .returning(|_| None);

    let harness = FlowControllerWebhookDeliverHarness::with_overrides(mock_flow_sensor_dispatcher);
    harness.register_dataset(&foo_dataset_id);

    let delivery_binding =
        webhook_deliver_binding(subscription_id, &event_type, Some(&foo_dataset_id));

    harness
        .controller
        .ensure_flow_sensor(&delivery_binding, Utc::now(), ReactiveRule::empty())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            None,
            vec![&new_head],
        )
        .await;

    let logical_plan = harness.build_task_logical_plan(&delivery_flow).await;
    pretty_assertions::assert_eq!(
        logical_plan,
        LogicalPlan {
            plan_type: LogicalPlanWebhookDeliver::TYPE_ID.to_string(),
            payload: json!({
                "webhook_subscription_id": subscription_id.to_string(),
                "webhook_event_type": event_type.to_string(),
                "webhook_payload": {
                    "block_ref": "head",
                    "dataset_id": foo_dataset_id.to_string(),
                    "is_breaking_change": false,
                    "new_hash": new_head.to_string(),
                    "owner_account_id": DEFAULT_ACCOUNT_ID.to_string(),
                    "version": 2,
                }

            }),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_multiple_updates() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let old_head = odf::Multihash::from_digest_sha3_256(b"old_head");
    let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");
    let new_head_2 = odf::Multihash::from_digest_sha3_256(b"new_head_2");

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&old_head),
            vec![&new_head, &new_head_2],
        )
        .await;

    let logical_plan = harness.build_task_logical_plan(&delivery_flow).await;
    pretty_assertions::assert_eq!(
        logical_plan,
        LogicalPlan {
            plan_type: LogicalPlanWebhookDeliver::TYPE_ID.to_string(),
            payload: json!({
                "webhook_subscription_id": subscription_id.to_string(),
                "webhook_event_type": event_type.to_string(),
                "webhook_payload": {
                    "block_ref": "head",
                    "dataset_id": foo_dataset_id.to_string(),
                    "is_breaking_change": false,
                    "old_hash": old_head.to_string(),
                    "new_hash": new_head_2.to_string(),
                    "owner_account_id": DEFAULT_ACCOUNT_ID.to_string(),
                    "version": 2,
                }

            }),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_breaking() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let old_head = odf::Multihash::from_digest_sha3_256(b"old_head");
    let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");

    let delivery_flow = harness
        .make_delivery_flow_breaking(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&old_head),
            &new_head,
        )
        .await;

    let logical_plan = harness.build_task_logical_plan(&delivery_flow).await;
    pretty_assertions::assert_eq!(
        logical_plan,
        LogicalPlan {
            plan_type: LogicalPlanWebhookDeliver::TYPE_ID.to_string(),
            payload: json!({
                "webhook_subscription_id": subscription_id.to_string(),
                "webhook_event_type": event_type.to_string(),
                "webhook_payload": {
                    "block_ref": "head",
                    "dataset_id": foo_dataset_id.to_string(),
                    "is_breaking_change": true,
                    "new_hash": new_head.to_string(),
                    "old_hash": old_head.to_string(),
                    "owner_account_id": DEFAULT_ACCOUNT_ID.to_string(),
                    "version": 2,
                }

            }),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_propagate_success_causes_no_interaction() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            None,
            vec![&new_head],
        )
        .await;

    harness.propagate_success(&delivery_flow).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowControllerWebhookDeliverHarness {
    controller: Arc<FlowControllerWebhookDeliver>,
    fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
    flow_event_store: Arc<dyn FlowEventStore>,
}

impl FlowControllerWebhookDeliverHarness {
    fn new() -> Self {
        Self::with_overrides(MockFlowSensorDispatcher::new())
    }

    fn with_overrides(mock_flow_sensor_dispatcher: MockFlowSensorDispatcher) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<FlowControllerWebhookDeliver>()
            .add::<InMemoryFlowEventStore>()
            .add::<FakeDatasetEntryService>()
            .add_value(mock_flow_sensor_dispatcher)
            .bind::<dyn FlowSensorDispatcher, MockFlowSensorDispatcher>();

        let catalog = b.build();
        Self {
            controller: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
        }
    }

    fn register_dataset(&self, dataset_id: &odf::DatasetID) {
        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            id: dataset_id.clone(),
            owner_id: DEFAULT_ACCOUNT_ID.clone(),
            owner_name: DEFAULT_ACCOUNT_NAME.clone(),
            name: odf::DatasetName::new_unchecked("foo"),
            created_at: Utc::now(),
            kind: odf::DatasetKind::Root,
        });
    }

    async fn make_delivery_flow_new_data(
        &self,
        flow_id: FlowID,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        dataset_id: &odf::DatasetID,
        old_head_maybe: Option<&odf::Multihash>,
        new_heads: Vec<&odf::Multihash>,
    ) -> FlowState {
        assert!(!new_heads.is_empty());

        let mut flow = Flow::new(
            Utc::now(),
            flow_id,
            webhook_deliver_binding(subscription_id, event_type, Some(dataset_id)),
            FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                changes: ResourceChanges::NewData(ResourceDataChanges {
                    blocks_added: 1,
                    records_added: 5,
                    new_watermark: None,
                }),
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                details: json!({
                    "dataset_id": dataset_id.to_string(),
                    "new_head": new_heads[0].to_string(),
                    "old_head_maybe": old_head_maybe.map(ToString::to_string),
                    "source": {
                        "UpstreamFlow": {
                            "flow_id": 0,
                            "flow_type": FLOW_TYPE_DATASET_INGEST,
                            "maybe_flow_config_snapshot": null,
                        }
                    }
                }),
            }),
            None,
            None,
        );

        for i in 1..new_heads.len() {
            flow.add_activation_cause_if_unique(
                Utc::now(),
                FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                    activation_time: Utc::now(),
                    changes: ResourceChanges::NewData(ResourceDataChanges {
                        blocks_added: 1,
                        records_added: 5,
                        new_watermark: None,
                    }),
                    resource_type: DATASET_RESOURCE_TYPE.to_string(),
                    details: json!({
                        "dataset_id": dataset_id.to_string(),
                        "new_head": new_heads[i].to_string(),
                        "old_head_maybe": new_heads[i - 1].to_string(),
                        "source": {
                            "ExternallyDetectedChange": serde_json::Value::Null,
                        }
                    }),
                }),
            )
            .unwrap();
        }

        flow.save(self.flow_event_store.as_ref()).await.unwrap();
        flow.into()
    }

    async fn make_delivery_flow_breaking(
        &self,
        flow_id: FlowID,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        dataset_id: &odf::DatasetID,
        old_head_maybe: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> FlowState {
        let mut flow = Flow::new(
            Utc::now(),
            flow_id,
            webhook_deliver_binding(subscription_id, event_type, Some(dataset_id)),
            FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                changes: ResourceChanges::Breaking,
                resource_type: DATASET_RESOURCE_TYPE.to_string(),
                details: json!({
                    "dataset_id": dataset_id.to_string(),
                    "new_head": new_head.to_string(),
                    "old_head_maybe": old_head_maybe.map(ToString::to_string),
                    "source": {
                        "UpstreamFlow": {
                            "flow_id": 0,
                            "flow_type": FLOW_TYPE_DATASET_INGEST,
                            "maybe_flow_config_snapshot": null,
                        }
                    }
                }),
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

    async fn propagate_success(&self, flow_state: &FlowState) {
        let task_result = TaskResult::empty();

        self.controller
            .propagate_success(flow_state, &task_result, Utc::now())
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
