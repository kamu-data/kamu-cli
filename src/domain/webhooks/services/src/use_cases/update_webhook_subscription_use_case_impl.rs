// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UpdateWebhookSubscriptionUseCase)]
pub struct UpdateWebhookSubscriptionUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateWebhookSubscriptionUseCase for UpdateWebhookSubscriptionUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = UpdateWebhookSubscriptionUseCaseImpl_execute,
        skip_all,
        fields(%subscription_id, %target_url, ?event_types, %label)
    )]
    async fn execute(
        &self,
        subscription_id: WebhookSubscriptionID,
        target_url: url::Url,
        event_types: Vec<WebhookEventType>,
        label: WebhookSubscriptionLabel,
    ) -> Result<(), UpdateWebhookSubscriptionError> {
        let mut subscription = crate::helpers::resolve_webhook_subscription(
            subscription_id,
            self.subscription_event_store.clone(),
            UpdateWebhookSubscriptionError::NotFound,
            UpdateWebhookSubscriptionError::Internal,
        )
        .await?;

        use super::helpers::*;
        validate_webhook_target_url(&target_url)?;
        validate_webhook_event_types(&event_types)?;

        if let Some(dataset_id) = subscription.dataset_id() {
            if subscription.label() != &label {
                // Check if the new label is unique for the dataset
                validate_webhook_subscription_label_unique_in_dataset(
                    self.subscription_event_store.as_ref(),
                    dataset_id,
                    &label,
                )
                .await
                .map_err(|e| match e {
                    ValidateWebhookSubscriptionLabelError::DuplicateLabel(e) => {
                        UpdateWebhookSubscriptionError::DuplicateLabel(e)
                    }
                    ValidateWebhookSubscriptionLabelError::Internal(e) => {
                        UpdateWebhookSubscriptionError::Internal(e)
                    }
                })?;
            }

            subscription
                .modify(target_url, label, event_types)
                .map_err(|e| UpdateWebhookSubscriptionError::Internal(e.int_err()))?;
            subscription
                .save(self.subscription_event_store.as_ref())
                .await
                .map_err(|e| UpdateWebhookSubscriptionError::Internal(e.int_err()))?;

            return Ok(());
        }

        // TODO system subscription
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
