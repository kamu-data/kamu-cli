// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{WebhookEventId, WebhookEventType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WebhookEvent {
    pub id: WebhookEventId,
    pub event_type: WebhookEventType,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

impl WebhookEvent {
    pub fn new(
        id: WebhookEventId,
        event_type: WebhookEventType,
        payload: serde_json::Value,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            id,
            event_type,
            payload,
            created_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
