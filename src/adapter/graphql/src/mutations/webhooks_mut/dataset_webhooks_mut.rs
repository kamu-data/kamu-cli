// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::{ComplexObject, InputObject, Interface, Object, SimpleObject};

use crate::mutations::*;
use crate::prelude::*;
use crate::queries::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetWebhooksMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetWebhooksMut<'a> {
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
                input.label.0.clone(),
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
                    WebhookSubscriptionDuplicateLabel {
                        label: input.label.0.to_string(),
                    },
                ))
            }

            Err(kamu_webhooks::CreateWebhookSubscriptionError::Internal(e)) => {
                Err(GqlError::Internal(e))
            }
        }
    }

    /// Returns a webhook subscription management API by ID
    pub async fn subscription(
        &self,
        ctx: &Context<'_>,
        id: WebhookSubscriptionID,
    ) -> Result<Option<WebhookSubscriptionMut>> {
        utils::check_dataset_maintain_access(ctx, self.dataset_request_state).await?;

        let webhook_subscription_query_svc =
            from_catalog_n!(ctx, dyn kamu_webhooks::WebhookSubscriptionQueryService);

        match webhook_subscription_query_svc
            .find_webhook_subscription_in_dataset(
                self.dataset_request_state.dataset_id(),
                id.into(),
            )
            .await
        {
            Ok(subscription) => Ok(subscription.map(WebhookSubscriptionMut::new)),
            Err(err) => Err(err.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject, Debug)]
pub struct WebhookSubscriptionInput {
    pub target_url: GqlUrl,
    pub event_types: Vec<WebhookEventType>,
    pub label: WebhookSubscriptionLabel,
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
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionDuplicateLabel {
    pub label: String,
}

#[ComplexObject]
impl WebhookSubscriptionDuplicateLabel {
    pub async fn message(&self) -> String {
        format!("Label '{}' is not unique", self.label)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionInvalidTargetUrl {
    pub inner_message: String,
}

#[ComplexObject]
impl WebhookSubscriptionInvalidTargetUrl {
    pub async fn message(&self) -> String {
        self.inner_message.clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionEventTypeUnknown {
    pub event_type: String,
}

#[ComplexObject]
impl WebhookSubscriptionEventTypeUnknown {
    pub async fn message(&self) -> String {
        format!("Event type '{}' is unknown", self.event_type)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct WebhookSubscriptionNoEventTypesProvided {
    pub num_event_types: usize,
}

#[ComplexObject]
impl WebhookSubscriptionNoEventTypesProvided {
    pub async fn message(&self) -> String {
        "At least one event type must be provided".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
