// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventStore;
use opendatafabric::{AccountName, DatasetID};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowEventStore: EventStore<FlowState> {
    /// Generates new unique flow identifier
    fn new_flow_id(&self) -> FlowID;

    /// Returns ID of the last succeeded dataset flow of certain type
    async fn get_last_succeeded_dataset_flow_id(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<Option<FlowID>, InternalError>;

    /// Returns ID of the last succeeded system flow of certain type
    async fn get_last_succeded_system_flow_id(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<Option<FlowID>, InternalError>;

    /// Returns IDs of the flows associated with the specified
    /// dataset in reverse chronological order based on creation time.
    /// Applies filters/pagination, if specified
    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
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
    Account(AccountName), // TODO: replace on AccountID
}

/////////////////////////////////////////////////////////////////////////////////////////
