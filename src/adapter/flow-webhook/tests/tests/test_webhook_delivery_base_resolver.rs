// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_adapter_flow_webhook::*;
use kamu_flow_system::FlowScope;
use kamu_task_system::{TaskError, TaskOutcome, TaskResult};
use kamu_webhooks::{WebhookEventTypeCatalog, WebhookSubscriptionID};
use uuid::Uuid;

use super::utils::{LinearDatasetHistory, WebhookDeliveryHistory};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_base_is_head_of_last_successful_delivery() {
    let [head_1, head_2, head_3] = [1, 2, 3].map(LinearDatasetHistory::head);

    let harness = WebhookDeliveryBaseResolverHarness::new(Some(head_1.clone())).await;
    harness
        .finish_delivery(&head_1, &head_2, TaskOutcome::Success(TaskResult::empty()))
        .await;
    harness
        .finish_delivery(
            &head_2,
            &head_3,
            TaskOutcome::Failed(TaskError::empty_recoverable()),
        )
        .await;

    assert_eq!(harness.resolve().await, Some(head_2));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_base_is_head_at_creation_before_any_success() {
    let [head_1, head_2] = [1, 2].map(LinearDatasetHistory::head);

    let harness = WebhookDeliveryBaseResolverHarness::new(Some(head_1.clone())).await;
    harness
        .finish_delivery(
            &head_1,
            &head_2,
            TaskOutcome::Failed(TaskError::empty_recoverable()),
        )
        .await;

    assert_eq!(harness.resolve().await, Some(head_1));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivery_base_unknown_without_head_at_creation() {
    let harness = WebhookDeliveryBaseResolverHarness::new(None).await;

    assert_eq!(harness.resolve().await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WebhookDeliveryBaseResolverHarness {
    resolver: Arc<WebhookDeliveryBaseResolver>,
    history: WebhookDeliveryHistory,
    webhook_flow_scope: FlowScope,
}

impl WebhookDeliveryBaseResolverHarness {
    /// A subscription to "foo", created when its head was `dataset_head`
    async fn new(dataset_head: Option<odf::Multihash>) -> Self {
        let mut b = dill::CatalogBuilder::new();
        WebhookDeliveryHistory::register(&mut b);
        let catalog = b.build();

        let webhook_flow_scope = FlowScopeSubscription::make_scope(
            WebhookSubscriptionID::new(Uuid::new_v4()),
            &WebhookEventTypeCatalog::dataset_ref_updated(),
            Some(&odf::DatasetID::new_seeded_ed25519(b"foo")),
        );
        let history = WebhookDeliveryHistory::from_catalog(&catalog);
        history
            .create_subscription(&webhook_flow_scope, dataset_head.as_ref())
            .await;

        Self {
            resolver: catalog.get_one().unwrap(),
            history,
            webhook_flow_scope,
        }
    }

    async fn resolve(&self) -> Option<odf::Multihash> {
        self.resolver
            .resolve(&self.webhook_flow_scope)
            .await
            .unwrap()
    }

    async fn finish_delivery(
        &self,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
        task_outcome: TaskOutcome,
    ) {
        self.history
            .finish_delivery(&self.webhook_flow_scope, old_head, new_head, task_outcome)
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
