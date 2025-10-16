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
    pub fn new(
        subscription_id: WebhookSubscriptionID,
        target_url: url::Url,
        label: WebhookSubscriptionLabel,
        dataset_id: Option<odf::DatasetID>,
        event_types: Vec<WebhookEventType>,
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
                },
            )
            .unwrap(),
        )
    }

    pub fn enable(&mut self) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventEnabled {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
        })
    }

    pub fn pause(&mut self) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventPaused {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
        })
    }

    pub fn resume(&mut self) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventResumed {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
        })
    }

    pub fn mark_unreachable(&mut self) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventMarkedUnreachable {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
        })
    }

    pub fn reactivate(&mut self) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventReactivated {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
        })
    }

    pub fn modify(
        &mut self,
        target_url: url::Url,
        label: WebhookSubscriptionLabel,
        event_types: Vec<WebhookEventType>,
    ) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventModified {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
            new_target_url: target_url,
            new_label: label,
            new_event_types: event_types,
        })
    }

    pub fn create_secret(
        &mut self,
        secret: WebhookSubscriptionSecret,
    ) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventSecretCreated {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
            secret,
        })
    }

    pub fn rotate_secret(
        &mut self,
        new_secret: WebhookSubscriptionSecret,
    ) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventSecretRotated {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
            new_secret,
        })
    }

    pub fn remove(&mut self) -> Result<(), ProjectionError<WebhookSubscriptionState>> {
        self.apply(WebhookSubscriptionEventRemoved {
            event_time: chrono::Utc::now(),
            subscription_id: self.id(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
