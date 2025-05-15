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

use crate::{WebhookEvent, WebhookEventID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait WebhookEventRepository: Send + Sync {
    async fn create_event(&self, event: &WebhookEvent) -> Result<(), CreateWebhookEventError>;

    async fn get_event_by_id(
        &self,
        event_id: WebhookEventID,
    ) -> Result<WebhookEvent, GetWebhookEventError>;

    async fn list_recent_events(
        &self,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookEvent>, ListRecentWebhookEventsError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateWebhookEventError {
    #[error(transparent)]
    DuplicateId(WebhookEventDuplicateIdError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetWebhookEventError {
    #[error(transparent)]
    NotFound(WebhookEventNotFoundError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ListRecentWebhookEventsError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Webhook event id='{event_id}' already exists")]
pub struct WebhookEventDuplicateIdError {
    pub event_id: WebhookEventID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Webhook event id='{event_id}' not found")]
pub struct WebhookEventNotFoundError {
    pub event_id: WebhookEventID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
