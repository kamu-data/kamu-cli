// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{WebhookEventType, WebhookSubscriptionID, WebhookSubscriptionSecret};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CreateWebhookSubscriptionUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_id: Option<odf::DatasetID>,
        target_url: url::Url,
        event_types: Vec<WebhookEventType>,
        label: String,
    ) -> Result<CreateWebhookSubscriptionResult, CreateWebhookSubscriptionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CreateWebhookSubscriptionResult {
    pub subscription_id: WebhookSubscriptionID,
    pub secret: WebhookSubscriptionSecret,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CreateWebhookSubscriptionError {
    #[error("Expecting https:// target URLs with host not pointing to 'localhost': {0}")]
    InvalidTargetUrl(url::Url),

    #[error("At least one event type must be provided")]
    NoEventTypesProvided,

    #[error("Webhook subscription with label '{0}' already exists")]
    DuplicateLabel(String),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
