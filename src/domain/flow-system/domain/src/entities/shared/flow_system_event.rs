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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowSystemEvent {
    pub event_id: EventID,
    pub source_type: FlowSystemEventSourceType,
    pub source_event_id: EventID,
    pub occurred_at: DateTime<Utc>,
    pub inserted_at: DateTime<Utc>,
    pub payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum FlowSystemEventSourceType {
    FlowConfiguration,
    Flow,
    FlowTrigger,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
