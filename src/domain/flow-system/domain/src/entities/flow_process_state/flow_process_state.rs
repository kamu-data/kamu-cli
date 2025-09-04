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

use crate::{FlowBinding, FlowProcessEffectiveState, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowProcessState {
    pub flow_binding: FlowBinding,

    pub paused_manual: bool,
    pub stop_policy: FlowTriggerStopPolicy,

    pub consecutive_failures: u32,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub next_planned_at: Option<DateTime<Utc>>,

    pub effective_state: FlowProcessEffectiveState,

    pub updated_at: DateTime<Utc>,
    pub last_applied_trigger_event_id: EventID,
    pub last_applied_flow_event_id: EventID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
