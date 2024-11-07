// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::OutboxMessageID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Version of outbox message which will be bumped only if the structure contains
// breaking changes in the message structure
pub const OUTBOX_MESSAGE_VERSION: i32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub message_id: OutboxMessageID,
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
    pub version: i32,
}

impl OutboxMessage {
    pub fn new(
        message_id: OutboxMessageID,
        producer_name: String,
        content_json: serde_json::Value,
        occurred_on: DateTime<Utc>,
    ) -> Self {
        Self {
            message_id,
            producer_name,
            content_json,
            occurred_on,
            version: OUTBOX_MESSAGE_VERSION,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct NewOutboxMessage {
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
    pub version: i32,
}

impl NewOutboxMessage {
    pub fn new(
        producer_name: String,
        content_json: serde_json::Value,
        occurred_on: DateTime<Utc>,
    ) -> Self {
        Self {
            producer_name,
            content_json,
            occurred_on,
            version: OUTBOX_MESSAGE_VERSION,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
