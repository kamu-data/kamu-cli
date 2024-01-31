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

    /// Returns ID of the last dataset flow of certain type
    fn get_last_dataset_flow_id_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<FlowID>;

    /// Returns ID of the last system flow of certain type
    fn get_last_system_flow_id_of_type(&self, flow_type: SystemFlowType) -> Option<FlowID>;

    /// Returns IDs of the flows associated with the specified
    /// dataset in reverse chronological order based on creation time.
    /// Applies filters, if specified
    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: DatasetFlowFilters,
    ) -> FlowIDStream;

    /// Returns IDs of the system flows  in reverse chronological order based on
    /// creation time
    /// Applies filters, if specified///
    fn get_all_system_flow_ids(&self, filters: SystemFlowFilters) -> FlowIDStream;

    /// Returns IDs of the flows of any type in reverse chronological order
    /// based on creation time
    fn get_all_flow_ids(&self) -> FlowIDStream<'_>;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug)]
pub struct DatasetFlowFilters {
    pub by_flow_type: Option<DatasetFlowType>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<AccountName>, // TODO: replace on AccountID
}

#[derive(Default, Debug)]
pub struct SystemFlowFilters {
    pub by_flow_type: Option<SystemFlowType>,
    pub by_flow_status: Option<FlowStatus>,
    pub by_initiator: Option<AccountName>, // TODO: replace on AccountID
}

/////////////////////////////////////////////////////////////////////////////////////////
