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

    /// Returns nearest time when one or more flows are scheduled for activation
    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError>;

    /// Returns flows scheduled for activation at the given time
    async fn get_flows_scheduled_for_activation_at(
        &self,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<Vec<FlowID>, InternalError>;

    /// Returns IDs of the flows associated with the specified
    /// dataset in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns IDs of the flow initiators associated with the specified
    /// dataset in reverse chronological order based on creation time.
    fn get_unique_flow_initiator_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> InitiatorIDStream;

    /// Returns number of flows associated with the specified dataset and
    /// matching filters, if specified
    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        filters: &FlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns number of flows associated with the specified datasets and
    /// matching filters, if specified
    async fn get_count_flows_by_multiple_datasets(
        &self,
        dataset_ids: &[&odf::DatasetID],
        filters: &FlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns IDs of the datasets who have any flows associated with them
    async fn filter_datasets_having_flows(
        &self,
        dataset_ids: &[&odf::DatasetID],
    ) -> Result<Vec<odf::DatasetID>, InternalError>;

    /// Returns IDs of the flows associated with the specified
    /// dataset in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_by_datasets(
        &self,
        dataset_ids: &[&odf::DatasetID],
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns IDs of the system flows  in reverse chronological order based on
    /// creation time
    /// Applies filters/pagination, if specified
    fn get_all_system_flow_ids(
        &self,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns number of system flows matching filters, if specified
    async fn get_count_system_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError>;

    /// Returns IDs of the flows of any type matching the given filters in
    /// reverse chronological order based on creation time
    fn get_all_flow_ids(
        &self,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_>;

    /// Returns number of all flows, matching filters
    async fn get_count_all_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError>;

    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
pub struct FlowFilters {
    pub by_flow_type: Option<String>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<InitiatorFilter>,
}

#[derive(Default, Debug, Clone)]
pub struct AccountFlowFilters {
    pub by_dataset_ids: HashSet<odf::DatasetID>,
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
