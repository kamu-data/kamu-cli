// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct SqliteFlowProcessStateRowModel {
    pub paused_manual: i64,
    pub stop_policy_kind: String,
    pub stop_policy_data: Option<serde_json::Value>,
    pub consecutive_failures: i64,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub next_planned_at: Option<DateTime<Utc>>,
    pub effective_state: String,
    pub sort_key: String,
    pub updated_at: DateTime<Utc>,
    pub last_applied_trigger_event_id: i64,
    pub last_applied_flow_event_id: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
