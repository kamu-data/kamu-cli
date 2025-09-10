// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use event_sourcing::EventStore;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowEventStore: EventStore<FlowState> {
    /// Generates new unique flow identifier
    async fn new_flow_id(&self) -> Result<FlowID, InternalError>;

    /// Attempts to access the pending (unfinished) flow ID for the given
    /// binding
    async fn try_get_pending_flow(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowID>, InternalError>;

    /// Attempts to access all the pending flow IDs for the given scope
    async fn try_get_all_scope_pending_flows(
        &self,
        flow_scope: &FlowScope,
    ) -> Result<Vec<FlowID>, InternalError>;

    /// Returns last run statistics for certain type
    async fn get_flow_run_stats(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<FlowRunStats, InternalError>;

    /// Returns current number of consecutive failures for the given flow
    async fn get_current_consecutive_flow_failures_count(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<u32, InternalError>;

    /// Returns nearest time when one or more flows are scheduled for activation
    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError>;

    /// Returns flows scheduled for activation at the given time
    async fn get_flows_scheduled_for_activation_at(
        &self,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<Vec<FlowID>, InternalError>;

    /// Returns IDs of the flows where scope matches the pattern,
    /// in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_matching_scope_query(
        &self,
        flow_scope_query: FlowScopeQuery,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns number of flows associated matching filters, if specified
    async fn get_count_flows_matching_scope_query(
        &self,
        flow_scope_query: &FlowScopeQuery,
        filters: &FlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns IDs of the flow initiators, where flow scope matches the pattern
    fn list_scoped_flow_initiators(&self, scope_query: FlowScopeQuery) -> InitiatorIDStream;

    /// Returns scopes having any flows associated with them
    async fn filter_flow_scopes_having_flows(
        &self,
        flow_scopes: &[FlowScope],
    ) -> Result<Vec<FlowScope>, InternalError>;

    /// Returns IDs of the flows of any type matching the given filters in
    /// reverse chronological order based on creation time
    fn get_all_flow_ids(
        &self,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_>;

    /// Returns number of all flows, matching filters
    async fn get_count_all_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError>;

    /// Returns stream of flow states for the given flow IDs
    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
pub struct FlowFilters {
    pub by_flow_type: Option<String>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<InitiatorFilter>,
}

#[derive(Debug, Clone)]
pub enum InitiatorFilter {
    System,
    Account(HashSet<odf::AccountID>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowRunStats {
    pub last_success_time: Option<DateTime<Utc>>,
    pub last_attempt_time: Option<DateTime<Utc>>,
}

impl FlowRunStats {
    pub fn merge(&mut self, new_stats: FlowRunStats) {
        if new_stats.last_success_time.is_some() {
            self.last_success_time = new_stats.last_success_time;
        }
        if new_stats.last_attempt_time.is_some() {
            self.last_attempt_time = new_stats.last_attempt_time;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
