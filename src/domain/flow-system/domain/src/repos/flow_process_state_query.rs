// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::{
    FlowBinding,
    FlowProcessEffectiveState,
    FlowProcessGroupRollup,
    FlowProcessState,
    FlowScopeQuery,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowProcessStateQuery: Send + Sync {
    /// Determines if any process state exists in general
    async fn has_any_process_states(&self) -> Result<bool, InternalError>;

    /// Load a single process
    async fn try_get_process_state(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowProcessState>, InternalError>;

    /// List processes that match a scope filter (partial JSON) and optional
    /// flow-type filter. Use for dataset page (all processes for one
    /// dataset) or account lists.
    async fn list_processes(
        &self,
        filter: FlowProcessListFilter<'_>,
        order: FlowProcessOrder,
        pagination: Option<PaginationOpts>,
    ) -> Result<FlowProcessStateListing, InternalError>;

    /// Compute rollup for matching rows.
    async fn rollup_by_scope(
        &self,
        flow_scope_query: FlowScopeQuery,
        for_flow_types: Option<&[&'static str]>,
        effective_state_in: Option<&[FlowProcessEffectiveState]>,
    ) -> Result<FlowProcessGroupRollup, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct FlowProcessStateListing {
    pub processes: Vec<FlowProcessState>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowProcessListFilter<'a> {
    /// Scope filter
    pub scope: FlowScopeQuery,

    /// Optional narrowing by flow type strings
    pub for_flow_types: Option<&'a [&'static str]>,

    /// Chips / state cuts
    pub effective_state_in: Option<&'a [FlowProcessEffectiveState]>,

    /// Time windows (UTC). Bounds inclusive.
    pub last_attempt_between: Option<(DateTime<Utc>, DateTime<Utc>)>,
    pub last_failure_since: Option<DateTime<Utc>>,
    pub next_planned_before: Option<DateTime<Utc>>,
    pub next_planned_after: Option<DateTime<Utc>>,

    /// Severity cut.
    pub min_consecutive_failures: Option<u32>,
}

impl<'a> FlowProcessListFilter<'a> {
    /// Empty filter
    pub fn all() -> Self {
        Self::for_scope(FlowScopeQuery::all())
    }

    /// Minimal filter: scope only (no extra cuts).
    pub fn for_scope(scope: FlowScopeQuery) -> Self {
        Self {
            scope,
            for_flow_types: None,
            effective_state_in: None,
            last_attempt_between: None,
            last_failure_since: None,
            next_planned_before: None,
            next_planned_after: None,
            min_consecutive_failures: None,
        }
    }

    /// Adding flow types.
    pub fn for_flow_types(self, for_flow_types: &'a [&'static str]) -> Self {
        Self {
            for_flow_types: Some(for_flow_types),
            ..self
        }
    }

    /// Adding flow types (optional).
    pub fn for_flow_types_opt(self, for_flow_types: Option<&'a [&'static str]>) -> Self {
        Self {
            for_flow_types,
            ..self
        }
    }

    /// Adding effective states.
    pub fn with_effective_states(
        self,
        effective_state_in: &'a [FlowProcessEffectiveState],
    ) -> Self {
        Self {
            effective_state_in: Some(effective_state_in),
            ..self
        }
    }

    /// Adding effective states (optional).
    pub fn with_effective_states_opt(
        self,
        effective_state_in: Option<&'a [FlowProcessEffectiveState]>,
    ) -> Self {
        Self {
            effective_state_in,
            ..self
        }
    }

    /// Adding last attempt time window filter.
    pub fn with_last_attempt_between(self, start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self {
            last_attempt_between: Some((start, end)),
            ..self
        }
    }

    /// Adding last attempt time window filter (optional).
    pub fn with_last_attempt_between_opt(
        self,
        last_attempt_between: Option<(DateTime<Utc>, DateTime<Utc>)>,
    ) -> Self {
        Self {
            last_attempt_between,
            ..self
        }
    }

    /// Adding last failure since filter.
    pub fn with_last_failure_since(self, last_failure_since: DateTime<Utc>) -> Self {
        Self {
            last_failure_since: Some(last_failure_since),
            ..self
        }
    }

    /// Adding last failure since filter (optional).
    pub fn with_last_failure_since_opt(self, last_failure_since: Option<DateTime<Utc>>) -> Self {
        Self {
            last_failure_since,
            ..self
        }
    }

    /// Adding next planned before filter.
    pub fn with_next_planned_before(self, next_planned_before: DateTime<Utc>) -> Self {
        Self {
            next_planned_before: Some(next_planned_before),
            ..self
        }
    }

    /// Adding next planned before filter (optional).
    pub fn with_next_planned_before_opt(self, next_planned_before: Option<DateTime<Utc>>) -> Self {
        Self {
            next_planned_before,
            ..self
        }
    }

    /// Adding next planned after filter.
    pub fn with_next_planned_after(self, next_planned_after: DateTime<Utc>) -> Self {
        Self {
            next_planned_after: Some(next_planned_after),
            ..self
        }
    }

    /// Adding next planned after filter (optional).
    pub fn with_next_planned_after_opt(self, next_planned_after: Option<DateTime<Utc>>) -> Self {
        Self {
            next_planned_after,
            ..self
        }
    }

    /// Adding minimum consecutive failures filter.
    pub fn with_min_consecutive_failures(self, min_consecutive_failures: u32) -> Self {
        Self {
            min_consecutive_failures: Some(min_consecutive_failures),
            ..self
        }
    }

    /// Adding minimum consecutive failures filter (optional).
    pub fn with_min_consecutive_failures_opt(self, min_consecutive_failures: Option<u32>) -> Self {
        Self {
            min_consecutive_failures,
            ..self
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
pub struct FlowProcessOrder {
    pub field: FlowProcessOrderField,
    pub desc: bool,
}

impl FlowProcessOrder {
    /// Default: “recent first”.
    pub fn recent() -> Self {
        Self {
            field: FlowProcessOrderField::LastAttemptAt,
            desc: true,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FlowProcessOrderField {
    /// Default for “recent activity”.
    LastAttemptAt,

    /// “What’s next”
    NextPlannedAt,

    /// Triage hot spots.
    LastFailureAt,

    /// Chronic issues first.
    ConsecutiveFailures,

    /// Severity bucketing
    EffectiveState,

    /// By flow type
    FlowType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
