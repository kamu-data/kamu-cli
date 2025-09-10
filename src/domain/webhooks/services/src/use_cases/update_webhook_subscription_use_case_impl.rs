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
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UpdateWebhookSubscriptionUseCase)]
pub struct UpdateWebhookSubscriptionUseCaseImpl {
    subscription_event_store: Arc<dyn WebhookSubscriptionEventStore>,
    outbox: Arc<dyn Outbox>,
}

impl UpdateWebhookSubscriptionUseCaseImpl {
    async fn issue_event_type_changes(
        &self,
        subscription: &WebhookSubscription,
        old_event_types: &[WebhookEventType],
        new_event_types: &[WebhookEventType],
    ) -> Result<(), InternalError> {
        let disabled_event_types = old_event_types
            .iter()
            .filter(|et| !new_event_types.contains(et))
            .cloned()
            .collect::<Vec<_>>();

        let enabled_event_types = new_event_types
            .iter()
            .filter(|et| !old_event_types.contains(et))
            .cloned()
            .collect::<Vec<_>>();

        for disabled_event_type in disabled_event_types {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
                    WebhookSubscriptionEventChangesMessage::event_disabled(
                        subscription.id(),
                        subscription.dataset_id(),
                        disabled_event_type,
                    ),
                )
                .await?;
        }

        for enabled_event_type in enabled_event_types {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE,
                    WebhookSubscriptionEventChangesMessage::event_enabled(
                        subscription.id(),
                        subscription.dataset_id(),
                        enabled_event_type,
                    ),
                )
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl UpdateWebhookSubscriptionUseCase for UpdateWebhookSubscriptionUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = UpdateWebhookSubscriptionUseCaseImpl_execute,
        skip_all,
        fields(subscription_id=%subscription.id()),
    )]
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
        target_url: url::Url,
        mut event_types: Vec<WebhookEventType>,
        label: WebhookSubscriptionLabel,
    ) -> Result<(), UpdateWebhookSubscriptionError> {
        use super::helpers::*;

        tracing::info!(
            %target_url,
            ?event_types,
            %label,
            "Initiating update of webhook subscription",
        );

        validate_webhook_target_url(&target_url)?;
        validate_webhook_event_types(&event_types)?;
        deduplicate_event_types(&mut event_types);

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

            let old_event_types = subscription.event_types().to_vec();

            subscription
                .modify(target_url, label, event_types)
                .map_err(|e: ProjectionError<WebhookSubscriptionState>| {
                    tracing::error!(error=?e, error_msg=%e, "Webhook subscription update failed");
                    UpdateWebhookSubscriptionError::UpdateUnexpected(
                        UpdateWebhookSubscriptionUnexpectedError {
                            status: subscription.status(),
                        },
                    )
                })?;

            subscription
                .save(self.subscription_event_store.as_ref())
                .await
                .map_err(|e| UpdateWebhookSubscriptionError::Internal(e.int_err()))?;

            self.issue_event_type_changes(
                subscription,
                &old_event_types,
                subscription.event_types(),
            )
            .await?;

            return Ok(());
        }

        // TODO system subscription
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
