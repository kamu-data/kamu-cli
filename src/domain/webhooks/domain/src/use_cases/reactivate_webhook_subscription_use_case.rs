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
pub trait ReactivateWebhookSubscriptionUseCase: Send + Sync {
    async fn execute(
        &self,
        subscription: &mut WebhookSubscription,
    ) -> Result<(), ReactivateWebhookSubscriptionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ReactivateWebhookSubscriptionError {
    #[error(transparent)]
    ReactivateUnexpected(#[from] ReactivateWebhookSubscriptionUnexpectedError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Webhook subscription is not in a state that allows reactivating: {status:?}")]
pub struct ReactivateWebhookSubscriptionUnexpectedError {
    pub status: WebhookSubscriptionStatus,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
