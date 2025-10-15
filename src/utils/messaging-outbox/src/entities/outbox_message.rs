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

#[derive(Debug, Clone)]
pub struct OutboxMessage {
    pub message_id: OutboxMessageID,
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
    pub version: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(sqlx::FromRow)]
pub struct OutboxMessageRow {
    pub message_id: i64,
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
    pub version: i64,
}

#[cfg(feature = "sqlx")]
impl From<OutboxMessageRow> for OutboxMessage {
    fn from(row: OutboxMessageRow) -> Self {
        OutboxMessage {
            message_id: OutboxMessageID::new(row.message_id),
            producer_name: row.producer_name,
            content_json: row.content_json,
            occurred_on: row.occurred_on,
            version: row.version.try_into().unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct NewOutboxMessage {
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
    pub version: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl NewOutboxMessage {
    pub fn as_outbox_message(&self, message_id: OutboxMessageID) -> OutboxMessage {
        OutboxMessage {
            message_id,
            producer_name: self.producer_name.clone(),
            content_json: self.content_json.clone(),
            occurred_on: self.occurred_on,
            version: self.version,
        }
    }
}
