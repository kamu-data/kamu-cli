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

    fn convert_event_types(
        event_types: &[String],
    ) -> std::result::Result<
        Vec<kamu_webhooks::WebhookEventType>,
        WebhookSubscriptionEventTypeUnknown,
    > {
        let mut res_event_types = Vec::with_capacity(event_types.len());
        for event_type in event_types {
            let Ok(event_type) = kamu_webhooks::WebhookEventType::try_new(event_type) else {
                return Err(WebhookSubscriptionEventTypeUnknown {
                    event_type: event_type.to_string(),
                });
            };

            if !kamu_webhooks::WebhookEventTypeCatalog::is_valid_type(&event_type) {
                return Err(WebhookSubscriptionEventTypeUnknown {
                    event_type: event_type.to_string(),
                });
            }

            res_event_types.push(event_type);
        }

        Ok(res_event_types)
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
        let subscription_event_store =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionEventStore);

        let Ok(target_url) = url::Url::parse(&input.target_url) else {
            return Ok(CreateWebhookSubscriptionResult::TargetUrlInvalid(
                WebhookSubscriptionTargetUrlInvalid {
                    target_url: input.target_url.clone(),
                },
            ));
        };

        let event_types = match Self::convert_event_types(&input.event_types) {
            Ok(event_types) => event_types,
            Err(e) => {
                return Ok(CreateWebhookSubscriptionResult::EventTypeUnknown(e));
            }
        };

        if let Some(dataset_id) = &input.dataset_id {
            let label = kamu_webhooks::WebhookSubscriptionLabel::new(&input.label);
            if !Self::check_label_is_unique(ctx, dataset_id, &label).await? {
                return Ok(CreateWebhookSubscriptionResult::LabelNotUnique(
                    WebhookSubscriptionLabelNotUnique {
                        label: label.to_string(),
                    },
                ));
            }

            let secret_generator = from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSecretGenerator);
            let secret = secret_generator.generate_secret();

            let subscription_id = kamu_webhooks::WebhookSubscriptionID::new(uuid::Uuid::new_v4());

            let mut subscription = kamu_webhooks::WebhookSubscription::new(
                subscription_id,
                target_url,
                label,
                Some(dataset_id.clone().into()),
                event_types,
                secret.clone(),
            );
            subscription
                .enable()
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            subscription
                .save(subscription_event_store.as_ref())
                .await
                .map_err(|e| GqlError::Internal(e.int_err()))?;

            return Ok(CreateWebhookSubscriptionResult::Success(
                CreateWebhookSubscriptionResultSuccess {
                    secret: secret.to_string(),
                },
            ));
        }

        // TODO system subscription
        unimplemented!()
    }

    async fn update_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
        input: UpdateWebhookSubscriptionInput,
    ) -> Result<UpdateWebhookSubscriptionResult> {
        let maybe_subscription = Self::resolve_subscription(ctx, subscription_id).await?;
        if let Some(mut subscription) = maybe_subscription {
            let label = kamu_webhooks::WebhookSubscriptionLabel::new(&input.label);
            if let Some(dataset_id) = subscription.dataset_id() {
                if !Self::check_label_is_unique(ctx, dataset_id, &label).await? {
                    return Ok(UpdateWebhookSubscriptionResult::LabelNotUnique(
                        WebhookSubscriptionLabelNotUnique {
                            label: label.to_string(),
                        },
                    ));
                }
            }

            // TODO: check label is unique for system subscriptions

            let Ok(target_url) = url::Url::parse(&input.target_url) else {
                return Ok(UpdateWebhookSubscriptionResult::TargetUrlInvalid(
                    WebhookSubscriptionTargetUrlInvalid {
                        target_url: input.target_url.clone(),
                    },
                ));
            };

            let event_types = match Self::convert_event_types(&input.event_types) {
                Ok(event_types) => event_types,
                Err(e) => {
                    return Ok(UpdateWebhookSubscriptionResult::EventTypeUnknown(e));
                }
            };

            subscription
                .modify(target_url, label, event_types)
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
    target_url: String,
    event_types: Vec<String>,
    label: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct UpdateWebhookSubscriptionInput {
    target_url: String,
    event_types: Vec<String>,
    label: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateWebhookSubscriptionResult {
    Success(CreateWebhookSubscriptionResultSuccess),
    LabelNotUnique(WebhookSubscriptionLabelNotUnique),
    TargetUrlInvalid(WebhookSubscriptionTargetUrlInvalid),
    EventTypeUnknown(WebhookSubscriptionEventTypeUnknown),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateWebhookSubscriptionResultSuccess {
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
    LabelNotUnique(WebhookSubscriptionLabelNotUnique),
    TargetUrlInvalid(WebhookSubscriptionTargetUrlInvalid),
    EventTypeUnknown(WebhookSubscriptionEventTypeUnknown),
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
pub struct WebhookSubscriptionLabelNotUnique {
    label: String,
}

#[ComplexObject]
impl WebhookSubscriptionLabelNotUnique {
    async fn message(&self) -> String {
        format!("Label '{}' is not unique", self.label)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionTargetUrlInvalid {
    target_url: String,
}

#[ComplexObject]
impl WebhookSubscriptionTargetUrlInvalid {
    async fn message(&self) -> String {
        format!("Target URL '{}' is invalid", self.target_url)
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
