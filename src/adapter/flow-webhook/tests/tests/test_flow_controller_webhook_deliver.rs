// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use chrono::Utc;
use internal_error::InternalError;
use kamu_accounts::{DEFAULT_ACCOUNT_NAME, TEST_ACCOUNT_ID};
use kamu_adapter_flow_dataset::{DATASET_RESOURCE_TYPE, FLOW_TYPE_DATASET_INGEST};
use kamu_adapter_flow_webhook::*;
use kamu_adapter_task_webhook::LogicalPlanWebhookDeliver;
use kamu_datasets::{DatasetEntry, DatasetIncrementQueryService, MockDatasetIncrementQueryService};
use kamu_datasets_services::testing::{FakeDatasetEntryService, FakeDatasetReferenceService};
use kamu_flow_system::*;
use kamu_flow_system_inmem::{InMemoryFlowEventStore, InMemoryFlowSystemEventBridge};
use kamu_task_system::{LogicalPlan, TaskResult};
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use kamu_webhooks::{WebhookEventType, WebhookEventTypeCatalog, WebhookSubscriptionID};
use serde_json::json;
use time_source::SystemTimeSourceDefault;
use uuid::Uuid;
use wakeup_listener::WakeupListenerMetrics;

use super::utils::{DatasetWebhookWatermarks, LinearDatasetHistory};

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
        .ensure_flow_sensor(
            &delivery_binding,
            FlowSensorActivation::CatchUp(Utc::now()),
            ReactiveRule::empty(),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan() {
    let head_1 = LinearDatasetHistory::head(1);

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_1);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            None,
            vec![&head_1],
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
                    "new_hash": head_1.to_string(),
                    "owner_account_id": TEST_ACCOUNT_ID.to_string(),
                    "version": 2,
                },
                "webhook_watermark": { "head": head_1.to_string() },
            }),
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_multiple_updates() {
    let [head_1, head_2, head_3] = [1, 2, 3].map(LinearDatasetHistory::head);

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_3);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_1),
            vec![&head_2, &head_3],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": false,
            "new_hash": head_3.to_string(),
            "old_hash": head_1.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_breaking() {
    let head_2 = LinearDatasetHistory::head(2);
    let diverged_head = LinearDatasetHistory::diverged_head();

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_2);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    let delivery_flow = harness
        .make_delivery_flow_breaking(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&diverged_head),
            &head_2,
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": true,
            "new_hash": head_2.to_string(),
            "old_hash": diverged_head.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_covers_changes_since_delivered_head() {
    let [head_1, head_2, head_3] = [1, 2, 3].map(LinearDatasetHistory::head);

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_3);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    // The receiver has 1, the delivery of 1 -> 2 was lost, so the next one reports
    // 1 -> 3
    harness.set_delivered_head(subscription_id, &head_1).await;

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(100),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_2),
            vec![&head_3],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": false,
            "new_hash": head_3.to_string(),
            "old_hash": head_1.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_reports_current_head() {
    let [head_1, head_2, head_3] = [1, 2, 3].map(LinearDatasetHistory::head);

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_3);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
    harness.set_delivered_head(subscription_id, &head_1).await;

    // Triggered by the update to 2, while the dataset has already moved to 3
    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_1),
            vec![&head_2],
        )
        .await;

    let logical_plan = harness.build_task_logical_plan(&delivery_flow).await;
    pretty_assertions::assert_eq!(
        logical_plan.payload["webhook_payload"],
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": false,
            "new_hash": head_3.to_string(),
            "old_hash": head_1.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
    pretty_assertions::assert_eq!(
        logical_plan.payload["webhook_watermark"],
        json!({ "head": head_3.to_string() }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_breaking_when_delivered_head_rewritten() {
    let [head_2, head_3] = [2, 3].map(LinearDatasetHistory::head);
    let diverged_head = LinearDatasetHistory::diverged_head();

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_3);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    // The receiver has a head that is no longer in the history
    harness
        .set_delivered_head(subscription_id, &diverged_head)
        .await;

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_2),
            vec![&head_3],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": true,
            "new_hash": head_3.to_string(),
            "old_hash": diverged_head.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_breaking_when_delivered_head_missing() {
    let [head_2, head_3] = [2, 3].map(LinearDatasetHistory::head);
    let missing_head = LinearDatasetHistory::missing_head();

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_3);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    // The receiver has a head that is not a block of the dataset
    harness
        .set_delivered_head(subscription_id, &missing_head)
        .await;

    let delivery_flow = harness
        .make_delivery_flow_new_data(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_2),
            vec![&head_3],
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": true,
            "new_hash": head_3.to_string(),
            "old_hash": missing_head.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_breaking_when_reset_before_delivered_head() {
    let [head_2, head_3] = [2, 3].map(LinearDatasetHistory::head);

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_2);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    // The receiver has 3, then the dataset is reset back to 2
    harness.set_delivered_head(subscription_id, &head_3).await;

    let delivery_flow = harness
        .make_delivery_flow_breaking(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_3),
            &head_2,
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": true,
            "new_hash": head_2.to_string(),
            "old_hash": head_3.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_reports_resets_announced_out_of_order() {
    let [head_1, head_2, head_3] = [1, 2, 3].map(LinearDatasetHistory::head);

    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);
    harness.set_dataset_head(&foo_dataset_id, &head_1);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();

    // The receiver has 3; the dataset was reset to 2 and then to 1, and only
    // the reset to 2 triggered this delivery
    harness.set_delivered_head(subscription_id, &head_3).await;

    let delivery_flow = harness
        .make_delivery_flow_breaking(
            FlowID::new(1),
            subscription_id,
            &event_type,
            &foo_dataset_id,
            Some(&head_3),
            &head_2,
        )
        .await;

    pretty_assertions::assert_eq!(
        harness.build_webhook_payload(&delivery_flow).await,
        json!({
            "block_ref": "head",
            "dataset_id": foo_dataset_id.to_string(),
            "is_breaking_change": true,
            "new_hash": head_1.to_string(),
            "old_hash": head_3.to_string(),
            "owner_account_id": TEST_ACCOUNT_ID.to_string(),
            "version": 2,
        }),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_unsupported_event_type() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);

    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventType::try_new("DATASET.UNKNOWN").unwrap();

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

    assert_matches!(
        harness.try_build_task_logical_plan(&delivery_flow).await,
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_non_dataset_resource_update() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    harness.register_dataset(&foo_dataset_id);

    let delivery_flow = harness
        .make_delivery_flow_with_update(&foo_dataset_id, "other-resource", &foo_dataset_id)
        .await;

    assert_matches!(
        harness.try_build_task_logical_plan(&delivery_flow).await,
        Err(_)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_logical_plan_update_of_other_dataset() {
    let harness = FlowControllerWebhookDeliverHarness::new();

    let foo_dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_dataset_id = odf::DatasetID::new_seeded_ed25519(b"bar");
    harness.register_dataset(&foo_dataset_id);

    let delivery_flow = harness
        .make_delivery_flow_with_update(&foo_dataset_id, DATASET_RESOURCE_TYPE, &bar_dataset_id)
        .await;

    assert_matches!(
        harness.try_build_task_logical_plan(&delivery_flow).await,
        Err(_)
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
    fake_dataset_reference_service: Arc<FakeDatasetReferenceService>,
    flow_event_store: Arc<dyn FlowEventStore>,
    watermarks: DatasetWebhookWatermarks,
}

impl FlowControllerWebhookDeliverHarness {
    fn new() -> Self {
        Self::with_overrides(MockFlowSensorDispatcher::new())
    }

    fn with_overrides(mock_flow_sensor_dispatcher: MockFlowSensorDispatcher) -> Self {
        let mut b = dill::CatalogBuilder::new();
        b.add::<FlowControllerWebhookDeliver>()
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryWakeupHub>()
            .add::<WakeupListenerMetrics>()
            .add::<SystemTimeSourceDefault>()
            .add::<FakeDatasetEntryService>()
            .add::<FakeDatasetReferenceService>()
            .add_value(mock_flow_sensor_dispatcher)
            .bind::<dyn FlowSensorDispatcher, MockFlowSensorDispatcher>()
            .add_value(LinearDatasetHistory::increments())
            .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>();
        DatasetWebhookWatermarks::register(&mut b);

        let catalog = b.build();
        Self {
            controller: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            fake_dataset_reference_service: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),
            watermarks: DatasetWebhookWatermarks::from_catalog(&catalog),
        }
    }

    async fn set_delivered_head(
        &self,
        subscription_id: WebhookSubscriptionID,
        head: &odf::Multihash,
    ) {
        self.watermarks
            .set_delivered_head(subscription_id, head)
            .await;
    }

    fn register_dataset(&self, dataset_id: &odf::DatasetID) {
        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            id: dataset_id.clone(),
            owner_id: TEST_ACCOUNT_ID.clone(),
            owner_name: DEFAULT_ACCOUNT_NAME.clone(),
            name: odf::DatasetName::new_unchecked("foo"),
            created_at: Utc::now(),
            kind: odf::DatasetKind::Root,
        });
    }

    fn set_dataset_head(&self, dataset_id: &odf::DatasetID, head: &odf::Multihash) {
        self.fake_dataset_reference_service
            .set_head(dataset_id, head);
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
        assert_ne!(new_heads.len(), 0);

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

    /// A `DATASET.REF.UPDATED` delivery flow for `scope_dataset_id`, activated
    /// by an update of `resource_type` that reports `updated_dataset_id`
    async fn make_delivery_flow_with_update(
        &self,
        scope_dataset_id: &odf::DatasetID,
        resource_type: &str,
        updated_dataset_id: &odf::DatasetID,
    ) -> FlowState {
        let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
        let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
        let new_head = odf::Multihash::from_digest_sha3_256(b"new_head");

        let mut flow = Flow::new(
            Utc::now(),
            FlowID::new(1),
            webhook_deliver_binding(subscription_id, &event_type, Some(scope_dataset_id)),
            FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                activation_time: Utc::now(),
                changes: ResourceChanges::Breaking,
                resource_type: resource_type.to_string(),
                details: json!({
                    "dataset_id": updated_dataset_id.to_string(),
                    "new_head": new_head.to_string(),
                    "old_head_maybe": null,
                    "source": {
                        "ExternallyDetectedChange": serde_json::Value::Null,
                    }
                }),
            }),
            None,
            None,
        );
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
        flow.into()
    }

    /// The webhook payload of the delivery's logical plan
    async fn build_webhook_payload(&self, flow_state: &FlowState) -> serde_json::Value {
        self.build_task_logical_plan(flow_state).await.payload["webhook_payload"].clone()
    }

    async fn build_task_logical_plan(&self, flow_state: &FlowState) -> LogicalPlan {
        self.try_build_task_logical_plan(flow_state).await.unwrap()
    }

    async fn try_build_task_logical_plan(
        &self,
        flow_state: &FlowState,
    ) -> Result<LogicalPlan, InternalError> {
        self.controller.build_task_logical_plan(flow_state).await
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
