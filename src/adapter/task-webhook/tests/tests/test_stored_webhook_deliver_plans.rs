// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_task_webhook::{
    DeliverWebhookTaskPlanner,
    LogicalPlanWebhookDeliver,
    TaskDefinitionWebhookDeliver,
};
use kamu_task_system::{LogicalPlan, TaskDefinitionPlanner, TaskEvent, TaskID};
use kamu_webhooks::{WebhookEventTypeCatalog, WebhookSubscriptionID};
use pretty_assertions::assert_eq;
use serde_json::json;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `task_events.event_payload` of a webhook delivery task created by v0.270.0,
/// before delivery plans carried a watermark. Persisted data: never edit it to
/// match a newer shape.
const TASK_CREATED_EVENT_WITHOUT_WATERMARK: &str = r#"{
    "TaskCreated": {
        "event_time": "2026-09-01T12:00:00.123456789Z",
        "task_id": 42,
        "logical_plan": {
            "DeliverWebhook": {
                "webhook_subscription_id": "51128027-f51a-469c-a5c8-4f0dac2752fd",
                "webhook_event_type": "DATASET.REF.UPDATED",
                "webhook_payload": {
                    "version": 2,
                    "dataset_id": "did:odf:fed01f987fa4da873cb3ccd79d0b1b75b3bd9d9eee67a86f2a739dd6e157e7fbbf521",
                    "owner_account_id": "did:odf:fed01f225c124918361719b451722286fe6c715b6a53250037fe459e3e1a6b335d9e5",
                    "block_ref": "head",
                    "new_hash": "f1620d6881ac3290fc54c319330b73a117ca14c06c05e3f22d26e3443f7cedee245cb",
                    "old_hash": "f16206c5f172aad848199fd2df870415e6b0669d67348d6ce651e7ab5088cd2fbd4e0",
                    "is_breaking_change": false
                }
            }
        },
        "metadata": {
            "properties": {
                "kamu.flow.id": "7"
            }
        }
    }
}"#;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_stored_plan_without_watermark_loads_as_none() {
    let harness = StoredWebhookDeliverPlanHarness::new();

    let logical_plan = harness.load_stored_logical_plan(TASK_CREATED_EVENT_WITHOUT_WATERMARK);
    assert_eq!(logical_plan.plan_type, LogicalPlanWebhookDeliver::TYPE_ID);

    let webhook_plan = LogicalPlanWebhookDeliver::from_logical_plan(&logical_plan).unwrap();
    assert_eq!(
        webhook_plan,
        LogicalPlanWebhookDeliver {
            webhook_subscription_id: harness.subscription_id(),
            webhook_event_type: WebhookEventTypeCatalog::dataset_ref_updated(),
            webhook_payload: harness.payload(),
            webhook_watermark: None,
        }
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_stored_plan_without_watermark_plans_task_without_watermark() {
    let harness = StoredWebhookDeliverPlanHarness::new();

    let logical_plan = harness.load_stored_logical_plan(TASK_CREATED_EVENT_WITHOUT_WATERMARK);
    let task_definition = harness
        .prepare_task_definition(TaskID::new(42), &logical_plan)
        .await;

    assert_eq!(task_definition.task_id, TaskID::new(42));
    assert_eq!(
        task_definition.webhook_subscription_id,
        harness.subscription_id()
    );
    assert_eq!(
        task_definition.webhook_event_type,
        WebhookEventTypeCatalog::dataset_ref_updated()
    );
    assert_eq!(task_definition.webhook_payload, harness.payload());
    assert_eq!(task_definition.webhook_watermark, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct StoredWebhookDeliverPlanHarness {
    planner: DeliverWebhookTaskPlanner,
}

impl StoredWebhookDeliverPlanHarness {
    fn new() -> Self {
        Self {
            planner: DeliverWebhookTaskPlanner {},
        }
    }

    fn load_stored_logical_plan(&self, stored_event_payload: &str) -> LogicalPlan {
        let event: TaskEvent = serde_json::from_str(stored_event_payload).unwrap();
        let TaskEvent::TaskCreated(created) = event else {
            panic!("Expected a TaskCreated event, got: {event:?}");
        };
        created.logical_plan
    }

    async fn prepare_task_definition(
        &self,
        task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> TaskDefinitionWebhookDeliver {
        let task_definition = self
            .planner
            .prepare_task_definition(task_id, logical_plan)
            .await
            .unwrap();
        *task_definition
            .downcast::<TaskDefinitionWebhookDeliver>()
            .unwrap()
    }

    fn subscription_id(&self) -> WebhookSubscriptionID {
        WebhookSubscriptionID::new(Uuid::parse_str("51128027-f51a-469c-a5c8-4f0dac2752fd").unwrap())
    }

    fn payload(&self) -> serde_json::Value {
        json!({
            "version": 2,
            "dataset_id": "did:odf:fed01f987fa4da873cb3ccd79d0b1b75b3bd9d9eee67a86f2a739dd6e157e7fbbf521",
            "owner_account_id": "did:odf:fed01f225c124918361719b451722286fe6c715b6a53250037fe459e3e1a6b335d9e5",
            "block_ref": "head",
            "new_hash": "f1620d6881ac3290fc54c319330b73a117ca14c06c05e3f22d26e3443f7cedee245cb",
            "old_hash": "f16206c5f172aad848199fd2df870415e6b0669d67348d6ce651e7ab5088cd2fbd4e0",
            "is_breaking_change": false,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
