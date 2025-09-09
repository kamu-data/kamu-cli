// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
        limit: usize,
        offset: usize,
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
    pub last_attempt_between:
        Option<(chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>,
    pub last_failure_since: Option<chrono::DateTime<chrono::Utc>>,
    pub next_planned_before: Option<chrono::DateTime<chrono::Utc>>,
    pub next_planned_after: Option<chrono::DateTime<chrono::Utc>>,

    /// Severity cut.
    pub min_consecutive_failures: Option<u32>,

    /// Name search on hierarchical sort key (case-insensitive).
    pub name_contains: Option<&'a str>,
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
            name_contains: None,
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
    pub fn with_last_attempt_between(
        self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            last_attempt_between: Some((start, end)),
            ..self
        }
    }

    /// Adding last failure since filter.
    pub fn with_last_failure_since(
        self,
        last_failure_since: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            last_failure_since: Some(last_failure_since),
            ..self
        }
    }

    /// Adding next planned before filter.
    pub fn with_next_planned_before(
        self,
        next_planned_before: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            next_planned_before: Some(next_planned_before),
            ..self
        }
    }

    /// Adding next planned after filter.
    pub fn with_next_planned_after(
        self,
        next_planned_after: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        Self {
            next_planned_after: Some(next_planned_after),
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

    /// Adding name contains filter.
    pub fn with_name_contains(self, name_contains: &'a str) -> Self {
        Self {
            name_contains: Some(name_contains),
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

    /// Alphabetical by hierarchical key (owner/dataset[/kind:label]).
    pub fn name_alpha() -> Self {
        Self {
            field: FlowProcessOrderField::NameAlpha,
            desc: false,
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

    /// A–Z over sort ke
    NameAlpha,

    /// By flow type
    FlowType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
