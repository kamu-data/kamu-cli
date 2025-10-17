// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::Object;
use kamu_webhooks::WebhooksConfig;
use secrecy::SecretString;

use crate::mutations::*;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebhookSubscriptionMut {
    webhook_subscription: Arc<tokio::sync::Mutex<kamu_webhooks::WebhookSubscription>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl WebhookSubscriptionMut {
    #[graphql(skip)]
    pub fn new(webhook_subscription: kamu_webhooks::WebhookSubscription) -> Self {
        Self {
            webhook_subscription: Arc::new(tokio::sync::Mutex::new(webhook_subscription)),
        }
    }

    pub async fn update(
        &self,
        ctx: &Context<'_>,
        input: WebhookSubscriptionInput,
    ) -> Result<UpdateWebhookSubscriptionResult> {
        let update_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::UpdateWebhookSubscriptionUseCase);

        let mut subscription = self.webhook_subscription.lock().await;

        match update_webhook_subscription_use_case
            .execute(
                &mut subscription,
                input.target_url.0,
                input
                    .event_types
                    .into_iter()
                    .map(|et| kamu_webhooks::WebhookEventType::try_new(et.0).unwrap())
                    .collect::<Vec<_>>(),
                input.label.0.clone(),
            )
            .await
        {
            Ok(_) => Ok(UpdateWebhookSubscriptionResult::Success(
                UpdateWebhookSubscriptionResultSuccess { updated: true },
            )),

            Err(kamu_webhooks::UpdateWebhookSubscriptionError::UpdateUnexpected(e)) => {
                Ok(UpdateWebhookSubscriptionResult::UpdateUnexpected(
                    UpdateWebhookSubscriptionResultUnexpected {
                        status: e.status.into(),
                    },
                ))
            }

            Err(e @ kamu_webhooks::UpdateWebhookSubscriptionError::InvalidTargetUrl(_)) => {
                Ok(UpdateWebhookSubscriptionResult::InvalidTargetUrl(
                    WebhookSubscriptionInvalidTargetUrl {
                        inner_message: e.to_string(),
                    },
                ))
            }

            Err(kamu_webhooks::UpdateWebhookSubscriptionError::NoEventTypesProvided(_)) => {
                Ok(UpdateWebhookSubscriptionResult::NoEventTypesProvided(
                    WebhookSubscriptionNoEventTypesProvided { num_event_types: 0 },
                ))
            }

            Err(kamu_webhooks::UpdateWebhookSubscriptionError::DuplicateLabel(_)) => {
                Ok(UpdateWebhookSubscriptionResult::DuplicateLabel(
                    WebhookSubscriptionDuplicateLabel {
                        label: input.label.0.to_string(),
                    },
                ))
            }

            Err(kamu_webhooks::UpdateWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    pub async fn pause(&self, ctx: &Context<'_>) -> Result<PauseWebhookSubscriptionResult> {
        let pause_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::PauseWebhookSubscriptionUseCase);

        let mut subscription = self.webhook_subscription.lock().await;

        match pause_webhook_subscription_use_case
            .execute(&mut subscription)
            .await
        {
            Ok(_) => Ok(PauseWebhookSubscriptionResult::Success(
                PauseWebhookSubscriptionResultSuccess { paused: true },
            )),

            Err(kamu_webhooks::PauseWebhookSubscriptionError::PauseUnexpected(e)) => {
                Ok(PauseWebhookSubscriptionResult::PauseUnexpected(
                    PauseWebhookSubscriptionResultUnexpected {
                        status: e.status.into(),
                    },
                ))
            }

            Err(kamu_webhooks::PauseWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    pub async fn resume(&self, ctx: &Context<'_>) -> Result<ResumeWebhookSubscriptionResult> {
        let resume_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::ResumeWebhookSubscriptionUseCase);

        let mut subscription = self.webhook_subscription.lock().await;

        match resume_webhook_subscription_use_case
            .execute(&mut subscription)
            .await
        {
            Ok(_) => Ok(ResumeWebhookSubscriptionResult::Success(
                ResumeWebhookSubscriptionResultSuccess { resumed: true },
            )),

            Err(kamu_webhooks::ResumeWebhookSubscriptionError::ResumeUnexpected(e)) => {
                Ok(ResumeWebhookSubscriptionResult::ResumeUnexpected(
                    ResumeWebhookSubscriptionResultUnexpected {
                        status: e.status.into(),
                    },
                ))
            }

            Err(kamu_webhooks::ResumeWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    pub async fn reactivate(
        &self,
        ctx: &Context<'_>,
    ) -> Result<ReactivateWebhookSubscriptionResult> {
        let reactivate_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::ReactivateWebhookSubscriptionUseCase);

        let mut subscription = self.webhook_subscription.lock().await;

        match reactivate_webhook_subscription_use_case
            .execute(&mut subscription)
            .await
        {
            Ok(_) => Ok(ReactivateWebhookSubscriptionResult::Success(
                ReactivateWebhookSubscriptionResultSuccess { reactivated: true },
            )),

            Err(kamu_webhooks::ReactivateWebhookSubscriptionError::ReactivateUnexpected(e)) => {
                Ok(ReactivateWebhookSubscriptionResult::ReactivateUnexpected(
                    ReactivateWebhookSubscriptionResultUnexpected {
                        status: e.status.into(),
                    },
                ))
            }

            Err(kamu_webhooks::ReactivateWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    pub async fn rotate_secret(
        &self,
        ctx: &Context<'_>,
    ) -> Result<RotateWebhookSubscriptionSecretResult> {
        let (rotate_webhook_subscription_secret_use_case, webhook_config) = from_catalog_n!(
            ctx,
            dyn kamu_webhooks::RotateWebhookSubscriptionSecretUseCase,
            WebhooksConfig
        );

        let mut subscription = self.webhook_subscription.lock().await;

        match rotate_webhook_subscription_secret_use_case
            .execute(&mut subscription)
            .await
        {
            Ok(secret) => {
                let encryption_key = webhook_config
                    .secret_encryption_key
                    .as_ref()
                    .map(|key| SecretString::from(key.clone()));
                Ok(RotateWebhookSubscriptionSecretResult::Success(
                    RotateWebhookSubscriptionSecretSuccess {
                        new_secret: String::from_utf8(
                            secret
                                .get_exposed_value(encryption_key.as_ref())
                                .int_err()?,
                        )
                        .int_err()?,
                    },
                ))
            }

            Err(kamu_webhooks::RotateWebhookSubscriptionSecretError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    pub async fn remove(&self, ctx: &Context<'_>) -> Result<RemoveWebhookSubscriptionResult> {
        let remove_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::RemoveWebhookSubscriptionUseCase);

        let mut subscription = self.webhook_subscription.lock().await;

        match remove_webhook_subscription_use_case
            .execute(&mut subscription)
            .await
        {
            Ok(_) => Ok(RemoveWebhookSubscriptionResult::Success(
                RemoveWebhookSubscriptionResultSuccess { removed: true },
            )),

            Err(kamu_webhooks::RemoveWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpdateWebhookSubscriptionResult {
    Success(UpdateWebhookSubscriptionResultSuccess),
    UpdateUnexpected(UpdateWebhookSubscriptionResultUnexpected),
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
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpdateWebhookSubscriptionResultUnexpected {
    status: WebhookSubscriptionStatus,
}

#[ComplexObject]
impl UpdateWebhookSubscriptionResultUnexpected {
    async fn message(&self) -> String {
        "Updating webhook subscription is unexpected at this state".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum PauseWebhookSubscriptionResult {
    Success(PauseWebhookSubscriptionResultSuccess),
    PauseUnexpected(PauseWebhookSubscriptionResultUnexpected),
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

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct PauseWebhookSubscriptionResultUnexpected {
    status: WebhookSubscriptionStatus,
}

#[ComplexObject]
impl PauseWebhookSubscriptionResultUnexpected {
    async fn message(&self) -> String {
        "Pausing webhook subscription is unexpected at this state".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ResumeWebhookSubscriptionResult {
    Success(ResumeWebhookSubscriptionResultSuccess),
    ResumeUnexpected(ResumeWebhookSubscriptionResultUnexpected),
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

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ResumeWebhookSubscriptionResultUnexpected {
    status: WebhookSubscriptionStatus,
}

#[ComplexObject]
impl ResumeWebhookSubscriptionResultUnexpected {
    async fn message(&self) -> String {
        "Resuming webhook subscription is unexpected at this state".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ReactivateWebhookSubscriptionResult {
    Success(ReactivateWebhookSubscriptionResultSuccess),
    ReactivateUnexpected(ReactivateWebhookSubscriptionResultUnexpected),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ReactivateWebhookSubscriptionResultSuccess {
    reactivated: bool,
}

#[ComplexObject]
impl ReactivateWebhookSubscriptionResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ReactivateWebhookSubscriptionResultUnexpected {
    status: WebhookSubscriptionStatus,
}

#[ComplexObject]
impl ReactivateWebhookSubscriptionResultUnexpected {
    async fn message(&self) -> String {
        "Reactivating webhook subscription is unexpected at this state".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RotateWebhookSubscriptionSecretResult {
    Success(RotateWebhookSubscriptionSecretSuccess),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct RotateWebhookSubscriptionSecretSuccess {
    new_secret: String,
}

#[ComplexObject]
impl RotateWebhookSubscriptionSecretSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RemoveWebhookSubscriptionResult {
    Success(RemoveWebhookSubscriptionResultSuccess),
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
