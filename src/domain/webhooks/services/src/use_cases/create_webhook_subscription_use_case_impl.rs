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

impl CreateWebhookSubscriptionUseCaseImpl {
    fn validate_target_url(
        &self,
        target_url: &url::Url,
    ) -> Result<(), CreateWebhookSubscriptionError> {
        if target_url.scheme() != "https" {
            return Err(CreateWebhookSubscriptionError::InvalidTargetUrl(
                target_url.clone(),
            ));
        }

        match target_url.host_str() {
            Some("localhost" | "127.0.0.1" | "::1") => Err(
                CreateWebhookSubscriptionError::InvalidTargetUrl(target_url.clone()),
            ),
            _ => Ok(()),
        }
    }

    fn validate_event_types(
        &self,
        event_types: &[WebhookEventType],
    ) -> Result<(), CreateWebhookSubscriptionError> {
        if event_types.is_empty() {
            return Err(CreateWebhookSubscriptionError::NoEventTypesProvided);
        }

        Ok(())
    }

    async fn check_label_is_unique(
        &self,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<(), CreateWebhookSubscriptionError> {
        let maybe_subscription_id = self
            .subscription_event_store
            .find_subscription_id_by_dataset_and_label(dataset_id, label)
            .await
            .map_err(|e: kamu_webhooks::FindWebhookSubscriptionError| match e {
                FindWebhookSubscriptionError::Internal(e) => {
                    CreateWebhookSubscriptionError::Internal(e)
                }
            })?;

        if maybe_subscription_id.is_some() {
            return Err(CreateWebhookSubscriptionError::DuplicateLabel(
                label.to_string(),
            ));
        }

        Ok(())
    }
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
        label: String,
    ) -> Result<CreateWebhookSubscriptionResult, CreateWebhookSubscriptionError> {
        self.validate_target_url(&target_url)?;
        self.validate_event_types(&event_types)?;

        if let Some(dataset_id) = &dataset_id {
            let label = kamu_webhooks::WebhookSubscriptionLabel::new(label);
            self.check_label_is_unique(dataset_id, &label).await?;

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
