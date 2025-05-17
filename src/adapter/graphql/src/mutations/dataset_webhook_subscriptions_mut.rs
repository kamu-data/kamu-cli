// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{ComplexObject, InputObject, Interface, Object, SimpleObject};

use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetWebhookSubscriptionsMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetWebhookSubscriptionsMut<'a> {
    #[graphql(skip)]
    pub async fn new_with_access_check(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        utils::check_dataset_maintain_access(ctx, dataset_request_state).await?;

        Ok(Self {
            dataset_request_state,
        })
    }

    async fn create_subscription(
        &self,
        ctx: &Context<'_>,
        input: WebhookSubscriptionInput,
    ) -> Result<CreateWebhookSubscriptionResult> {
        let create_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::CreateWebhookSubscriptionUseCase);

        match create_webhook_subscription_use_case
            .execute(
                Some(self.dataset_request_state.dataset_id().clone()),
                input.target_url.0.clone(),
                input
                    .event_types
                    .into_iter()
                    .map(|et| kamu_webhooks::WebhookEventType::try_new(et.0).unwrap())
                    .collect::<Vec<_>>(),
                kamu_webhooks::WebhookSubscriptionLabel::new(&input.label),
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

            Err(kamu_webhooks::CreateWebhookSubscriptionError::NoEventTypesProvided(_)) => {
                Ok(CreateWebhookSubscriptionResult::NoEventTypesProvided(
                    WebhookSubscriptionNoEventTypesProvided { num_event_types: 0 },
                ))
            }

            Err(kamu_webhooks::CreateWebhookSubscriptionError::DuplicateLabel(_)) => {
                Ok(CreateWebhookSubscriptionResult::DuplicateLabel(
                    WebhookSubscriptionDuplicateLabel { label: input.label },
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
        input: WebhookSubscriptionInput,
    ) -> Result<UpdateWebhookSubscriptionResult> {
        let update_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::UpdateWebhookSubscriptionUseCase);

        match update_webhook_subscription_use_case
            .execute(
                subscription_id.into(),
                input.target_url.0,
                input
                    .event_types
                    .into_iter()
                    .map(|et| kamu_webhooks::WebhookEventType::try_new(et.0).unwrap())
                    .collect::<Vec<_>>(),
                kamu_webhooks::WebhookSubscriptionLabel::new(&input.label),
            )
            .await
        {
            Ok(_) => Ok(UpdateWebhookSubscriptionResult::Success(
                UpdateWebhookSubscriptionResultSuccess { updated: true },
            )),

            Err(kamu_webhooks::UpdateWebhookSubscriptionError::NotFound(_)) => {
                Ok(UpdateWebhookSubscriptionResult::NotFound(
                    WebhookSubscriptionNotFound { subscription_id },
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
                    WebhookSubscriptionDuplicateLabel { label: input.label },
                ))
            }

            Err(kamu_webhooks::UpdateWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    async fn pause_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<PauseWebhookSubscriptionResult> {
        let pause_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::PauseWebhookSubscriptionUseCase);

        match pause_webhook_subscription_use_case
            .execute(subscription_id.into())
            .await
        {
            Ok(_) => Ok(PauseWebhookSubscriptionResult::Success(
                PauseWebhookSubscriptionResultSuccess { paused: true },
            )),

            Err(kamu_webhooks::PauseWebhookSubscriptionError::NotFound(_)) => {
                Ok(PauseWebhookSubscriptionResult::NotFound(
                    WebhookSubscriptionNotFound { subscription_id },
                ))
            }

            Err(kamu_webhooks::PauseWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    async fn resume_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<ResumeWebhookSubscriptionResult> {
        let resume_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::ResumeWebhookSubscriptionUseCase);

        match resume_webhook_subscription_use_case
            .execute(subscription_id.into())
            .await
        {
            Ok(_) => Ok(ResumeWebhookSubscriptionResult::Success(
                ResumeWebhookSubscriptionResultSuccess { resumed: true },
            )),

            Err(kamu_webhooks::ResumeWebhookSubscriptionError::NotFound(_)) => {
                Ok(ResumeWebhookSubscriptionResult::NotFound(
                    WebhookSubscriptionNotFound { subscription_id },
                ))
            }

            Err(kamu_webhooks::ResumeWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    async fn remove_subscription(
        &self,
        ctx: &Context<'_>,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<RemoveWebhookSubscriptionResult> {
        let remove_webhook_subscription_use_case =
            from_catalog_n!(ctx, dyn kamu_webhooks::RemoveWebhookSubscriptionUseCase);

        match remove_webhook_subscription_use_case
            .execute(subscription_id.into())
            .await
        {
            Ok(_) => Ok(RemoveWebhookSubscriptionResult::Success(
                RemoveWebhookSubscriptionResultSuccess { removed: true },
            )),

            Err(kamu_webhooks::RemoveWebhookSubscriptionError::NotFound(_)) => {
                Ok(RemoveWebhookSubscriptionResult::NotFound(
                    WebhookSubscriptionNotFound { subscription_id },
                ))
            }

            Err(kamu_webhooks::RemoveWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct WebhookSubscriptionInput {
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
