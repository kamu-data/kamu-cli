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
#[interface(dyn CreateWebhookSubscriptionUseCase)]
pub struct CreateWebhookSubscriptionUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    webhook_secret_generator: Arc<dyn WebhookSecretGenerator>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CreateWebhookSubscriptionUseCase for CreateWebhookSubscriptionUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = CreateWebhookSubscriptionUseCaseImpl_execute,
        skip_all,
        fields(?dataset_id, %target_url, ?event_types, %label)
    )]
    async fn execute(
        &self,
        dataset_id: Option<odf::DatasetID>,
        target_url: url::Url,
        event_types: Vec<WebhookEventType>,
        label: WebhookSubscriptionLabel,
    ) -> Result<CreateWebhookSubscriptionResult, CreateWebhookSubscriptionError> {
        // TODO: check dataset exists
        // TODO: security checks

        use super::helpers::*;
        validate_webhook_target_url(&target_url)?;
        validate_webhook_event_types(&event_types)?;

        if let Some(dataset_id) = &dataset_id {
            validate_webhook_subscription_label_unique_in_dataset(
                self.subscription_event_store.as_ref(),
                dataset_id,
                &label,
            )
            .await
            .map_err(|e| match e {
                ValidateWebhookSubscriptionLabelError::DuplicateLabel(e) => {
                    CreateWebhookSubscriptionError::DuplicateLabel(e)
                }
                ValidateWebhookSubscriptionLabelError::Internal(e) => {
                    CreateWebhookSubscriptionError::Internal(e)
                }
            })?;

            let secret = self.webhook_secret_generator.generate_secret();

            let subscription_id = kamu_webhooks::WebhookSubscriptionID::new(uuid::Uuid::new_v4());

            let mut subscription = kamu_webhooks::WebhookSubscription::new(
                subscription_id,
                target_url,
                label,
                Some(dataset_id.clone()),
                event_types,
                secret.clone(),
            );
            subscription
                .enable()
                .map_err(|e| CreateWebhookSubscriptionError::Internal(e.int_err()))?;

            subscription
                .save(self.subscription_event_store.as_ref())
                .await
                .map_err(|e| CreateWebhookSubscriptionError::Internal(e.int_err()))?;

            return Ok(CreateWebhookSubscriptionResult {
                subscription_id: subscription.id(),
                secret,
            });
        }

        // TODO system subscription
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
