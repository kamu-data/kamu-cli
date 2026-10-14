// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_webhooks::*;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn validate_webhook_target_url(
    target_url: &url::Url,
    webhooks_config: &WebhooksConfig,
) -> Result<(), WebhookSubscriptionInvalidTargetUrlError> {
    let resolved_ips = resolve_target_host(target_url);

    let is_valid = if webhooks_config.loopback_targets_only {
        // An unresolvable host is rejected too
        !resolved_ips.is_empty() && resolved_ips.iter().all(std::net::IpAddr::is_loopback)
    } else {
        // Reject localhost, ::1, etc. A resolution failure is not an error
        target_url.scheme() == "https" && !resolved_ips.iter().any(std::net::IpAddr::is_loopback)
    };

    if is_valid {
        Ok(())
    } else {
        Err(WebhookSubscriptionInvalidTargetUrlError {
            url: target_url.clone(),
        })
    }
}

/// Addresses the target host resolves to, empty when it does not resolve
fn resolve_target_host(target_url: &url::Url) -> Vec<std::net::IpAddr> {
    use std::net::ToSocketAddrs;

    let Some(host) = target_url.host_str() else {
        return Vec::new();
    };
    let clean_host = host.trim_matches(['[', ']'].as_ref());

    (clean_host, target_url.port_or_known_default().unwrap_or(80))
        .to_socket_addrs()
        .map(|addrs| addrs.map(|addr| addr.ip()).collect())
        .unwrap_or_default()
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

pub(crate) fn deduplicate_event_types(event_types: &mut Vec<WebhookEventType>) {
    event_types.sort();
    event_types.dedup();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn validate_webhook_subscription_label_unique_in_dataset(
    subscription_event_store: &dyn kamu_webhooks::WebhookSubscriptionEventStore,
    dataset_id: &odf::DatasetID,
    label: &WebhookSubscriptionLabel,
) -> Result<(), ValidateWebhookSubscriptionLabelError> {
    // Ignore empty labels
    if label.as_ref().is_empty() {
        return Ok(());
    }

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
