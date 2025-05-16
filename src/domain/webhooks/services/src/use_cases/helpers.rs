// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_webhooks::*;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn resolve_webhook_subscription<TError>(
    subscription_id: WebhookSubscriptionID,
    event_store: Arc<dyn WebhookSubscriptionEventStore>,
    not_found_error: impl Fn(WebhookSubscriptionNotFoundError) -> TError,
    internal_error: impl Fn(InternalError) -> TError,
) -> Result<WebhookSubscription, TError> {
    match WebhookSubscription::load(subscription_id, event_store.as_ref()).await {
        Ok(subscription) => Ok(subscription),
        Err(LoadError::NotFound(_)) => Err(not_found_error(WebhookSubscriptionNotFoundError {
            subscription_id,
        })),
        Err(LoadError::ProjectionError(e)) => Err(internal_error(e.int_err())),
        Err(LoadError::Internal(e)) => Err(internal_error(e)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_webhook_target_url(
    target_url: &url::Url,
) -> Result<(), WebhookSubscriptionInvalidTargetUrlError> {
    if target_url.scheme() != "https" {
        return Err(WebhookSubscriptionInvalidTargetUrlError {
            url: target_url.clone(),
        });
    }

    match target_url.host_str() {
        Some("localhost" | "127.0.0.1" | "::1") => Err(WebhookSubscriptionInvalidTargetUrlError {
            url: target_url.clone(),
        }),
        _ => Ok(()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_webhook_event_types(
    event_types: &[WebhookEventType],
) -> Result<(), WebhookSubscriptionNoEventTypesProvidedError> {
    if event_types.is_empty() {
        return Err(WebhookSubscriptionNoEventTypesProvidedError {});
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn validate_webhook_subscription_label_unique_in_dataset(
    subscription_event_store: &dyn kamu_webhooks::WebhookSubscriptionEventStore,
    dataset_id: &odf::DatasetID,
    label: &WebhookSubscriptionLabel,
) -> Result<(), ValidateWebhookSubscriptionLabelError> {
    let maybe_subscription_id = subscription_event_store
        .find_subscription_id_by_dataset_and_label(dataset_id, label)
        .await
        .map_err(|e: kamu_webhooks::FindWebhookSubscriptionError| match e {
            FindWebhookSubscriptionError::Internal(e) => {
                ValidateWebhookSubscriptionLabelError::Internal(e)
            }
        })?;

    if maybe_subscription_id.is_some() {
        return Err(ValidateWebhookSubscriptionLabelError::DuplicateLabel(
            WebhookSubscriptionDuplicateLabelError {
                label: label.clone(),
            },
        ));
    }

    Ok(())
}

#[derive(Debug, Error)]
pub(crate) enum ValidateWebhookSubscriptionLabelError {
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
