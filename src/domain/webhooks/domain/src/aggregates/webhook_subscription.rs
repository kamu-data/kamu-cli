// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct WebhookSubscription(
    Aggregate<WebhookSubscriptionState, (dyn WebhookSubscriptionEventStore + 'static)>,
);

impl WebhookSubscription {
    /// Creates a new webhook subscription
    pub fn new(
        subscription_id: WebhookSubscriptionId,
        target_url: url::Url,
        label: WebhookSubscriptionLabel,
        dataset_id: Option<odf::DatasetID>,
        event_types: Vec<WebhookEventType>,
        secret: String,
    ) -> Self {
        Self(
            Aggregate::new(
                subscription_id,
                WebhookSubscriptionEventCreated {
                    event_time: chrono::Utc::now(),
                    subscription_id,
                    dataset_id,
                    label,
                    event_types,
                    target_url,
                    secret,
                },
            )
            .unwrap(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
