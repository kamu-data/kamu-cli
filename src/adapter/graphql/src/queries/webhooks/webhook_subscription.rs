// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct WebhookSubscription {
    state: kamu_webhooks::WebhookSubscriptionState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookSubscription {
    pub fn new(state: kamu_webhooks::WebhookSubscriptionState) -> Self {
        Self { state }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl WebhookSubscription {
    /// Unique identifier of the webhook subscription
    async fn id(&self) -> WebhookSubscriptionID {
        self.state.id().into()
    }

    /// Optional label for the subscription. Maybe an empty string.
    async fn label(&self) -> String {
        self.state.label().to_string()
    }

    /// Associated dataset ID
    /// Not present for system subscriptions
    async fn dataset_id(&self) -> Option<DatasetID<'_>> {
        self.state.dataset_id().map(Into::into)
    }

    /// Target URL for the webhook
    async fn target_url(&self) -> String {
        self.state.target_url().to_string()
    }

    /// List of events that trigger the webhook
    async fn event_types(&self) -> Vec<String> {
        self.state
            .event_types()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    /// Status of the subscription
    async fn status(&self) -> WebhookSubscriptionStatus {
        self.state.status().into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
