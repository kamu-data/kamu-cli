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
use kamu_task_system as ts;

use crate::{WebhookDelivery, WebhookEventId, WebhookResponse, WebhookSubscriptionId};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookDeliveryRepository {
    async fn create(&self, delivery: WebhookDelivery) -> Result<(), CreateWebhookDeliveryError>;

    async fn update_response(
        &self,
        task_attempt_id: ts::TaskAttemptID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError>;

    async fn get_by_task_attempt_id(
        &self,
        task_attempt_id: ts::TaskAttemptID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError>;

    async fn list_by_task_id(
        &self,
        task_id: ts::TaskID,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError>;

    async fn list_by_event_id(
        &self,
        event_id: WebhookEventId,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError>;

    async fn list_by_subscription_id(
        &self,
        event_id: WebhookSubscriptionId,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CreateWebhookDeliveryError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
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

#[derive(thiserror::Error, Debug)]
pub enum GetWebhookDeliveryError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ListWebhookDeliveriesError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Webhook delivery task attempt '{}#{}' not found", task_attempt_id.task_id, task_attempt_id.attempt_number)]
pub struct WebhookDeliveryNotFoundError {
    pub task_attempt_id: ts::TaskAttemptID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
