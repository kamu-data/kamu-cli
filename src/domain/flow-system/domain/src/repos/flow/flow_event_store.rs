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
use opendatafabric::{AccountID, DatasetID};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowEventStore: EventStore<FlowState> {
    /// Generates new unique flow identifier
    async fn new_flow_id(&self) -> Result<FlowID, InternalError>;

    /// Attempts to access the pending (unfinished) flow ID for the given key
    async fn try_get_pending_flow(
        &self,
        flow_key: &FlowKey,
    ) -> Result<Option<FlowID>, InternalError>;

    /// Returns last run statistics for certain type
    async fn get_flow_run_stats(&self, flow_key: &FlowKey) -> Result<FlowRunStats, InternalError>;

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
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns IDs of the flow initiators associated with the specified
    /// dataset in reverse chronological order based on creation time.
    fn get_unique_flow_initiator_ids_by_dataset(&self, dataset_id: &DatasetID)
        -> InitiatorIDStream;

    /// Returns number of flows associated with the specified dataset and
    /// matching filters, if specified
    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns number of flows associated with the specified datasets and
    /// matching filters, if specified
    async fn get_count_flows_by_datasets(
        &self,
        dataset_ids: HashSet<DatasetID>,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns IDs of the flows associated with the specified
    /// dataset in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_by_datasets(
        &self,
        dataset_ids: HashSet<DatasetID>,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns IDs of the system flows  in reverse chronological order based on
    /// creation time
    /// Applies filters/pagination, if specified
    fn get_all_system_flow_ids(
        &self,
        filters: &SystemFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream;

    /// Returns number of system flows matching filters, if specified
    async fn get_count_system_flows(
        &self,
        filters: &SystemFlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns IDs of the flows of any type matching the given filters in
    /// reverse chronological order based on creation time
    fn get_all_flow_ids(
        &self,
        filters: &AllFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_>;

    /// Returns number of all flows, matching filters
    async fn get_count_all_flows(&self, filters: &AllFlowFilters) -> Result<usize, InternalError>;

    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone)]
pub struct DatasetFlowFilters {
    pub by_flow_type: Option<DatasetFlowType>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<InitiatorFilter>,
}

#[derive(Default, Debug, Clone)]
pub struct AccountFlowFilters {
    pub by_dataset_ids: HashSet<DatasetID>,
    pub by_flow_type: Option<DatasetFlowType>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<InitiatorFilter>,
}

#[derive(Default, Debug, Clone)]
pub struct SystemFlowFilters {
    pub by_flow_type: Option<SystemFlowType>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<InitiatorFilter>,
}

#[derive(Default, Debug, Clone)]
pub struct AllFlowFilters {
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<InitiatorFilter>,
}

#[derive(Debug, Clone)]
pub enum InitiatorFilter {
    System,
    Account(HashSet<AccountID>),
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
