// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{ComplexObject, InputObject, Interface, Object, SimpleObject};
use kamu_webhooks::LoadError;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookSubscriptionsMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookSubscriptionsMut {
    async fn resolve_subscription(
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<Option<kamu_webhooks::WebhookSubscription>> {
        let subscription_event_store =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionEventStore);

        match kamu_webhooks::WebhookSubscription::load(
            subscription_id.into(),
            subscription_event_store.as_ref(),
        )
        .await
        {
            Ok(subscription) => Ok(Some(subscription)),
            Err(LoadError::NotFound(_)) => Ok(None),
            Err(LoadError::ProjectionError(e)) => Err(GqlError::Internal(e.int_err())),
            Err(LoadError::Internal(e)) => Err(GqlError::Internal(e)),
        }
    }

    async fn check_label_is_unique(
        ctx: &Context<'_>,
        dataset_id: &odf::DatasetID,
        label: &kamu_webhooks::WebhookSubscriptionLabel,
    ) -> Result<bool> {
        let subscription_event_store =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionEventStore);

        let maybe_subscription_id = subscription_event_store
            .find_subscription_id_by_dataset_and_label(dataset_id, label)
            .await
            .map_err(|e: kamu_webhooks::FindWebhookSubscriptionError| match e {
                kamu_webhooks::FindWebhookSubscriptionError::Internal(e) => GqlError::Internal(e),
            })?;

        Ok(maybe_subscription_id.is_none())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl WebhookSubscriptionsMut {
    async fn create_subscription(
        &self,
        ctx: &Context<'_>,
        input: CreateWebhookSubscriptionInput,
    ) -> Result<CreateWebhookSubscriptionResult> {
        // TODO: security checks
        // TODO: check dataset exists

        let create_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::CreateWebhookSubscriptionUseCase);

        match create_webhook_subscription_use_case
            .execute(
                input.dataset_id.map(Into::into),
                input.target_url.0.clone(),
                input
                    .event_types
                    .into_iter()
                    .map(|et| kamu_webhooks::WebhookEventType::try_new(et.0).unwrap())
                    .collect::<Vec<_>>(),
                input.label,
            )
            .await
        {
            Ok(res) => Ok(CreateWebhookSubscriptionResult::Success(
                CreateWebhookSubscriptionResultSuccess {
                    subscription_id: res.subscription_id.to_string(),
                    secret: res.secret.to_string(),
                },
            )),

            Err(e @ kamu_webhooks::CreateWebhookSubscriptionError::InvalidTargetUrl(_)) => {
                Ok(CreateWebhookSubscriptionResult::InvalidTargetUrl(
                    WebhookSubscriptionInvalidTargetUrl {
                        inner_message: e.to_string(),
                    },
                ))
            }

            Err(kamu_webhooks::CreateWebhookSubscriptionError::NoEventTypesProvided) => {
                Ok(CreateWebhookSubscriptionResult::NoEventTypesProvided(
                    WebhookSubscriptionNoEventTypesProvided { num_event_types: 0 },
                ))
            }

            Err(kamu_webhooks::CreateWebhookSubscriptionError::DuplicateLabel(label)) => {
                Ok(CreateWebhookSubscriptionResult::DuplicateLabel(
                    WebhookSubscriptionDuplicateLabel {
                        label: label.to_string(),
                    },
                ))
            }

            Err(kamu_webhooks::CreateWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    async fn update_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
        input: UpdateWebhookSubscriptionInput,
    ) -> Result<UpdateWebhookSubscriptionResult> {
        // TODO: security checks

        let maybe_subscription = Self::resolve_subscription(ctx, subscription_id).await?;
        if let Some(mut subscription) = maybe_subscription {
            let label = kamu_webhooks::WebhookSubscriptionLabel::new(&input.label);
            if let Some(dataset_id) = subscription.dataset_id() {
                if !Self::check_label_is_unique(ctx, dataset_id, &label).await? {
                    return Ok(UpdateWebhookSubscriptionResult::DuplicateLabel(
                        WebhookSubscriptionDuplicateLabel {
                            label: label.to_string(),
                        },
                    ));
                }
            }

            // TODO: check label is unique for system subscriptions

            let event_types = input
                .event_types
                .into_iter()
                .map(|et| kamu_webhooks::WebhookEventType::try_new(et.0).unwrap())
                .collect::<Vec<_>>();

            subscription
                .modify(input.target_url.0, label, event_types)
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            let subscription_event_store =
                from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionEventStore);
            subscription
                .save(subscription_event_store.as_ref())
                .await
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            Ok(UpdateWebhookSubscriptionResult::Success(
                UpdateWebhookSubscriptionResultSuccess { updated: true },
            ))
        } else {
            Ok(UpdateWebhookSubscriptionResult::NotFound(
                WebhookSubscriptionNotFound { subscription_id },
            ))
        }
    }

    async fn pause_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<PauseWebhookSubscriptionResult> {
        // TODO: security checks

        let maybe_subscription = Self::resolve_subscription(ctx, subscription_id).await?;
        if let Some(mut subscription) = maybe_subscription {
            subscription
                .pause()
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            Ok(PauseWebhookSubscriptionResult::Success(
                PauseWebhookSubscriptionResultSuccess { paused: true },
            ))
        } else {
            Ok(PauseWebhookSubscriptionResult::NotFound(
                WebhookSubscriptionNotFound { subscription_id },
            ))
        }
    }

    async fn resume_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<ResumeWebhookSubscriptionResult> {
        // TODO: security checks

        let maybe_subscription = Self::resolve_subscription(ctx, subscription_id).await?;
        if let Some(mut subscription) = maybe_subscription {
            subscription
                .resume()
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            Ok(ResumeWebhookSubscriptionResult::Success(
                ResumeWebhookSubscriptionResultSuccess { resumed: true },
            ))
        } else {
            Ok(ResumeWebhookSubscriptionResult::NotFound(
                WebhookSubscriptionNotFound { subscription_id },
            ))
        }
    }

    async fn remove_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<RemoveWebhookSubscriptionResult> {
        // TODO: security checks

        let maybe_subscription = Self::resolve_subscription(ctx, subscription_id).await?;
        if let Some(mut subscription) = maybe_subscription {
            subscription
                .remove()
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            Ok(RemoveWebhookSubscriptionResult::Success(
                RemoveWebhookSubscriptionResultSuccess { removed: true },
            ))
        } else {
            Ok(RemoveWebhookSubscriptionResult::NotFound(
                WebhookSubscriptionNotFound { subscription_id },
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct CreateWebhookSubscriptionInput {
    dataset_id: Option<DatasetID<'static>>,
    target_url: GqlUrl,
    event_types: Vec<WebhookEventType>,
    label: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct UpdateWebhookSubscriptionInput {
    target_url: GqlUrl,
    event_types: Vec<WebhookEventType>,
    label: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateWebhookSubscriptionResult {
    Success(CreateWebhookSubscriptionResultSuccess),
    DuplicateLabel(WebhookSubscriptionDuplicateLabel),
    InvalidTargetUrl(WebhookSubscriptionInvalidTargetUrl),
    NoEventTypesProvided(WebhookSubscriptionNoEventTypesProvided),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateWebhookSubscriptionResultSuccess {
    subscription_id: String,
    secret: String,
}

#[ComplexObject]
impl CreateWebhookSubscriptionResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpdateWebhookSubscriptionResult {
    Success(UpdateWebhookSubscriptionResultSuccess),
    NotFound(WebhookSubscriptionNotFound),
    DuplicateLabel(WebhookSubscriptionDuplicateLabel),
    InvalidTargetUrl(WebhookSubscriptionInvalidTargetUrl),
    NoEventTypesProvided(WebhookSubscriptionNoEventTypesProvided),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpdateWebhookSubscriptionResultSuccess {
    updated: bool,
}

#[ComplexObject]
impl UpdateWebhookSubscriptionResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum PauseWebhookSubscriptionResult {
    Success(PauseWebhookSubscriptionResultSuccess),
    NotFound(WebhookSubscriptionNotFound),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct PauseWebhookSubscriptionResultSuccess {
    paused: bool,
}

#[ComplexObject]
impl PauseWebhookSubscriptionResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ResumeWebhookSubscriptionResult {
    Success(ResumeWebhookSubscriptionResultSuccess),
    NotFound(WebhookSubscriptionNotFound),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ResumeWebhookSubscriptionResultSuccess {
    resumed: bool,
}

#[ComplexObject]
impl ResumeWebhookSubscriptionResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RemoveWebhookSubscriptionResult {
    Success(RemoveWebhookSubscriptionResultSuccess),
    NotFound(WebhookSubscriptionNotFound),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct RemoveWebhookSubscriptionResultSuccess {
    removed: bool,
}

#[ComplexObject]
impl RemoveWebhookSubscriptionResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionNotFound {
    subscription_id: WebhookSubscriptionID,
}
#[ComplexObject]
impl WebhookSubscriptionNotFound {
    async fn message(&self) -> String {
        format!("Subscription '{}' not found", self.subscription_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionDuplicateLabel {
    label: String,
}

#[ComplexObject]
impl WebhookSubscriptionDuplicateLabel {
    async fn message(&self) -> String {
        format!("Label '{}' is not unique", self.label)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionInvalidTargetUrl {
    inner_message: String,
}

#[ComplexObject]
impl WebhookSubscriptionInvalidTargetUrl {
    async fn message(&self) -> String {
        self.inner_message.clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionEventTypeUnknown {
    event_type: String,
}

#[ComplexObject]
impl WebhookSubscriptionEventTypeUnknown {
    async fn message(&self) -> String {
        format!("Event type '{}' is unknown", self.event_type)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionNoEventTypesProvided {
    num_event_types: usize,
}

#[ComplexObject]
impl WebhookSubscriptionNoEventTypesProvided {
    async fn message(&self) -> String {
        "At least one event type must be provided".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
