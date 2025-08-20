// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use internal_error::InternalError;
use thiserror::Error;

use crate::{WebhookDelivery, WebhookDeliveryID, WebhookResponse, WebhookSubscriptionID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookDeliveryRepository: Send + Sync {
    async fn create(&self, delivery: WebhookDelivery) -> Result<(), CreateWebhookDeliveryError>;

    async fn update_response(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError>;

    async fn get_by_webhook_delivery_id(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError>;

    async fn list_by_subscription_id(
        &self,
        event_id: WebhookSubscriptionID,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateWebhookDeliveryError {
    #[error(transparent)]
    DeliveryExists(#[from] WebhookDeliveryAlreadyExistsError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateWebhookDeliveryError {
    #[error(transparent)]
    NotFound(WebhookDeliveryNotFoundError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetWebhookDeliveryError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ListWebhookDeliveriesError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Webhook delivery '{}' not found", webhook_delivery_id)]
pub struct WebhookDeliveryNotFoundError {
    pub webhook_delivery_id: WebhookDeliveryID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Webhook delivery '{}' already exists", webhook_delivery_id)]
pub struct WebhookDeliveryAlreadyExistsError {
    pub webhook_delivery_id: WebhookDeliveryID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
