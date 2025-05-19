// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

use crate::WebhookSubscriptionLabel;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Webhook target should use https:// and should not point at a loopback address: {url}")]
pub struct WebhookSubscriptionInvalidTargetUrlError {
    pub url: url::Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("At least one event type must be provided")]
pub struct WebhookSubscriptionNoEventTypesProvidedError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Webhook subscription with label '{label}' already exists")]
pub struct WebhookSubscriptionDuplicateLabelError {
    pub label: WebhookSubscriptionLabel,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
