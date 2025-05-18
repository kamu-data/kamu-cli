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

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateWebhookSubscriptionUseCase: Send + Sync {
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
        target_url: url::Url,
        event_types: Vec<WebhookEventType>,
        label: WebhookSubscriptionLabel,
    ) -> Result<(), UpdateWebhookSubscriptionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum UpdateWebhookSubscriptionError {
    #[error(transparent)]
    InvalidTargetUrl(#[from] WebhookSubscriptionInvalidTargetUrlError),

    #[error(transparent)]
    NoEventTypesProvided(#[from] WebhookSubscriptionNoEventTypesProvidedError),

    #[error(transparent)]
    DuplicateLabel(#[from] WebhookSubscriptionDuplicateLabelError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
