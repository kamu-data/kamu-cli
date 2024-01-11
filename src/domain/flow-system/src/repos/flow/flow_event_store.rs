// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::EventStore;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowEventStore: EventStore<FlowState> {
    /// Generates new unique flow identifier
    fn new_flow_id(&self) -> FlowID;

    /// Returns the last dataset flow of certain type
    fn get_last_dataset_flow_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<FlowID>;

    /// Returns the last system flow of certain type
    fn get_last_system_flow_of_type(&self, flow_type: SystemFlowType) -> Option<FlowID>;

    /// Returns the flows of certain type associated with the specified dataset
    /// in reverse chronological order based on creation time
    fn get_flows_by_dataset_of_type<'a>(
        &'a self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> FlowIDStream<'a>;

    /// Returns the flows of certain type in reverse chronological order based
    /// on creation time
    fn get_system_flows_of_type(&self, flow_type: SystemFlowType) -> FlowIDStream<'_>;

    /// Returns the flows of any type associated with the specified dataset
    /// in reverse chronological order based on creation time
    fn get_all_flows_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> FlowIDStream<'a>;

    /// Returns the flows of any type in reverse chronological order
    /// based on creation time
    fn get_all_flows(&self) -> FlowIDStream<'_>;
}

/////////////////////////////////////////////////////////////////////////////////////////
