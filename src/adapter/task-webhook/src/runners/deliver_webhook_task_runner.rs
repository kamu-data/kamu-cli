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
use internal_error::InternalError;
use kamu_task_system::*;
use kamu_webhooks::{WebhookDeliveryWorker, WebhookEvent, WebhookEventID, WebhookSubscriptionID};

use crate::TaskDefinitionWebhookDeliver;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionWebhookDeliver::TASK_TYPE,
})]
pub struct DeliverWebhookTaskRunner {
    webhook_event_repo: Arc<dyn kamu_webhooks::WebhookEventRepository>,
    webhook_delivery_worker: Arc<dyn WebhookDeliveryWorker>,
}

impl DeliverWebhookTaskRunner {
    #[tracing::instrument(level = "debug", skip_all, fields(?task_webhook))]
    async fn run_deliver_webhook(
        &self,
        task_webhook: TaskDefinitionWebhookDeliver,
    ) -> Result<TaskOutcome, InternalError> {
        // Create and save webhook event
        let webhook_event_id = WebhookEventID::new(uuid::Uuid::new_v4());
        let webhook_event = WebhookEvent::new(
            webhook_event_id,
            task_webhook.webhook_event_type,
            task_webhook.webhook_payload,
            Utc::now(),
        );
        self.webhook_event_repo
            .create_event(&webhook_event)
            .await
            .int_err()?;

        match self
            .webhook_delivery_worker
            .deliver_webhook(
                task_webhook.task_id,
                WebhookSubscriptionID::new(task_webhook.webhook_subscription_id),
                webhook_event_id,
            )
            .await
        {
            Ok(_) => Ok(TaskOutcome::Success(TaskResult::empty())),
            Err(err) => {
                tracing::error!(error = ?err, "Send webhook failed");
                Ok(TaskOutcome::Failed(TaskError::empty()))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for DeliverWebhookTaskRunner {
    async fn run_task(
        &self,
        task_definition: kamu_task_system::TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        let task_deliver = task_definition
            .downcast::<TaskDefinitionWebhookDeliver>()
            .expect("Mismatched task type for DeliverWebhookTaskRunner");

        self.run_deliver_webhook(*task_deliver).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
