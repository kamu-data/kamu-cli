// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::EventStore;
use opendatafabric::{AccountID, DatasetID};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowEventStore: EventStore<FlowState> {
    /// Generates new unique flow identifier
    fn new_flow_id(&self) -> FlowID;

    /// Returns last run statistics for the dataset flow of certain type
    async fn get_dataset_flow_run_stats(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
        account_id: &Option<AccountName>,
    ) -> Result<FlowRunStats, InternalError>;

    /// Returns last run statistics for the system flow of certain type
    async fn get_system_flow_run_stats(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<FlowRunStats, InternalError>;

    /// Returns IDs of the flows associated with the specified
    /// dataset in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: DatasetFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> FlowIDStream;

    /// Returns IDs of the flows associated with the specified
    /// account in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_by_account(
        &self,
        account_id: &AccountName,
        filters: DatasetFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> FlowIDStream;

    /// Returns number of flows associated with the specified dataset and
    /// matching filters, if specified
    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns number of flows associated with the specified account and
    /// matching filters, if specified
    async fn get_count_flows_by_account(
        &self,
        account_id: &AccountName,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns IDs of the system flows  in reverse chronological order based on
    /// creation time
    /// Applies filters/pagination, if specified
    fn get_all_system_flow_ids(
        &self,
        filters: SystemFlowFilters,
        pagination: FlowPaginationOpts,
    ) -> FlowIDStream;

    /// Returns number of system flows matching filters, if specified
    async fn get_count_system_flows(
        &self,
        filters: &SystemFlowFilters,
    ) -> Result<usize, InternalError>;

    /// Returns IDs of the flows of any type in reverse chronological order
    /// based on creation time
    /// TODO: not used yet, evaluate need in filters
    fn get_all_flow_ids(&self, pagination: FlowPaginationOpts) -> FlowIDStream<'_>;

    /// Returns number of all flows
    async fn get_count_all_flows(&self) -> Result<usize, InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub struct FlowPaginationOpts {
    pub offset: usize,
    pub limit: usize,
}

#[derive(Default, Debug, Clone)]
pub struct DatasetFlowFilters {
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

#[derive(Debug, Clone)]
pub enum InitiatorFilter {
    System,
    Account(AccountID),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, Copy)]
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

/////////////////////////////////////////////////////////////////////////////////////////
