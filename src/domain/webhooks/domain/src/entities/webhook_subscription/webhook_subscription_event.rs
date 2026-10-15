// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;
use serde::{Deserialize, Serialize};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WebhookSubscriptionEvent {
    Created(WebhookSubscriptionEventCreated),
    Enabled(WebhookSubscriptionEventEnabled),
    Paused(WebhookSubscriptionEventPaused),
    Resumed(WebhookSubscriptionEventResumed),
    MarkedUnreachable(WebhookSubscriptionEventMarkedUnreachable),
    Reactivated(WebhookSubscriptionEventReactivated),
    Modified(WebhookSubscriptionEventModified),
    SecretRotated(WebhookSubscriptionEventSecretRotated),
    Removed(WebhookSubscriptionEventRemoved),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventCreated {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
    pub dataset_id: Option<odf::DatasetID>,
    pub event_types: Vec<WebhookEventType>,
    pub target_url: url::Url,
    pub label: WebhookSubscriptionLabel,
    pub secret: WebhookSubscriptionSecret,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventEnabled {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventPaused {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventResumed {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventMarkedUnreachable {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventReactivated {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventModified {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
    pub new_target_url: url::Url,
    pub new_label: WebhookSubscriptionLabel,
    pub new_event_types: Vec<WebhookEventType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventSecretRotated {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
    pub new_secret: WebhookSubscriptionSecret,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebhookSubscriptionEventRemoved {
    pub event_time: DateTime<Utc>,
    pub subscription_id: WebhookSubscriptionID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebhookSubscriptionEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            Self::Created(_) => "WebhookSubscriptionEventCreated",
            Self::Enabled(_) => "WebhookSubscriptionEventEnabled",
            Self::Paused(_) => "WebhookSubscriptionEventPaused",
            Self::Resumed(_) => "WebhookSubscriptionEventResumed",
            Self::MarkedUnreachable(_) => "WebhookSubscriptionEventMarkedUnreachable",
            Self::Reactivated(_) => "WebhookSubscriptionEventReactivated",
            Self::Modified(_) => "WebhookSubscriptionEventUpdated",
            Self::SecretRotated(_) => "WebhookSubscriptionEventSecretRotated",
            Self::Removed(_) => "WebhookSubscriptionEventRemoved",
        }
    }

    pub fn subscription_id(&self) -> &WebhookSubscriptionID {
        match self {
            Self::Created(e) => &e.subscription_id,
            Self::Enabled(e) => &e.subscription_id,
            Self::Paused(e) => &e.subscription_id,
            Self::Resumed(e) => &e.subscription_id,
            Self::MarkedUnreachable(e) => &e.subscription_id,
            Self::Reactivated(e) => &e.subscription_id,
            Self::Modified(e) => &e.subscription_id,
            Self::SecretRotated(e) => &e.subscription_id,
            Self::Removed(e) => &e.subscription_id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::Created(e) => e.event_time,
            Self::Enabled(e) => e.event_time,
            Self::Paused(e) => e.event_time,
            Self::Resumed(e) => e.event_time,
            Self::MarkedUnreachable(e) => e.event_time,
            Self::Reactivated(e) => e.event_time,
            Self::Modified(e) => e.event_time,
            Self::SecretRotated(e) => e.event_time,
            Self::Removed(e) => e.event_time,
        }
    }

    pub fn new_status(&self, old_status: WebhookSubscriptionStatus) -> WebhookSubscriptionStatus {
        match self {
            Self::Created(_) => WebhookSubscriptionStatus::Unverified,
            Self::Enabled(_) | Self::Resumed(_) | Self::Reactivated(_) => {
                WebhookSubscriptionStatus::Enabled
            }
            Self::Paused(_) => WebhookSubscriptionStatus::Paused,
            Self::MarkedUnreachable(_) => WebhookSubscriptionStatus::Unreachable,
            Self::Modified(_) | Self::SecretRotated(_) => old_status,
            Self::Removed(_) => WebhookSubscriptionStatus::Removed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(WebhookSubscriptionEvent);
impl_enum_variant!(WebhookSubscriptionEvent::Created(
    WebhookSubscriptionEventCreated
));
impl_enum_variant!(WebhookSubscriptionEvent::Enabled(
    WebhookSubscriptionEventEnabled
));
impl_enum_variant!(WebhookSubscriptionEvent::Paused(
    WebhookSubscriptionEventPaused
));
impl_enum_variant!(WebhookSubscriptionEvent::Resumed(
    WebhookSubscriptionEventResumed
));
impl_enum_variant!(WebhookSubscriptionEvent::MarkedUnreachable(
    WebhookSubscriptionEventMarkedUnreachable
));
impl_enum_variant!(WebhookSubscriptionEvent::Reactivated(
    WebhookSubscriptionEventReactivated
));
impl_enum_variant!(WebhookSubscriptionEvent::Modified(
    WebhookSubscriptionEventModified
));
impl_enum_variant!(WebhookSubscriptionEvent::SecretRotated(
    WebhookSubscriptionEventSecretRotated
));
impl_enum_variant!(WebhookSubscriptionEvent::Removed(
    WebhookSubscriptionEventRemoved
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
