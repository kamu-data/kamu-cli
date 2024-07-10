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
    pub message_type: String,
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct NewOutboxMessage {
    pub message_type: String,
    pub producer_name: String,
    pub content_json: serde_json::Value,
    pub occurred_on: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
