// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::EventID;

use crate::ResourceRawEventQuery;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceRawEvent {
    pub event_id: EventID,
    pub query: ResourceRawEventQuery,
    pub event_time: DateTime<Utc>,
    pub event_type: String,
    pub payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
