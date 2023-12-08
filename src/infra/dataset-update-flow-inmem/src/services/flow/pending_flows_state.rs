// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_dataset_update_flow::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

use crate::dataset_flow_key::*;
use crate::AnyFlowID;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct PendingFlowsState {
    pending_dataset_flows: HashMap<OwnedDatasetFlowKey, DatasetFlowID>,
    pending_system_flows: HashMap<SystemFlowType, SystemFlowID>,
    pending_flows_by_tasks: HashMap<TaskID, AnyFlowID>,
}

impl PendingFlowsState {
    pub fn add_dataset_pending_flow(
        &mut self,
        dataset_id: DatasetID,
        flow_type: DatasetFlowType,
        flow_id: DatasetFlowID,
    ) {
        self.pending_dataset_flows
            .insert(OwnedDatasetFlowKey::new(dataset_id, flow_type), flow_id);
    }

    pub fn add_system_pending_flow(&mut self, flow_type: SystemFlowType, flow_id: SystemFlowID) {
        self.pending_system_flows.insert(flow_type, flow_id);
    }

    pub fn track_dataset_flow_task(&mut self, flow_id: DatasetFlowID, task_id: TaskID) {
        self.pending_flows_by_tasks
            .insert(task_id, AnyFlowID::Dataset(flow_id));
    }

    pub fn track_system_flow_task(&mut self, flow_id: SystemFlowID, task_id: TaskID) {
        self.pending_flows_by_tasks
            .insert(task_id, AnyFlowID::System(flow_id));
    }

    pub fn drop_dataset_flow(
        &mut self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<DatasetFlowID> {
        self.pending_dataset_flows
            .remove(BorrowedDatasetFlowKey::new(dataset_id, flow_type).as_trait())
    }

    pub fn drop_system_flow(&mut self, flow_type: SystemFlowType) -> Option<SystemFlowID> {
        self.pending_system_flows.remove(&flow_type)
    }

    pub fn untrack_flow_by_task(&mut self, task_id: TaskID) {
        self.pending_flows_by_tasks.remove(&task_id);
    }

    pub fn try_get_dataset_pending_flow(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<DatasetFlowID> {
        self.pending_dataset_flows
            .get(BorrowedDatasetFlowKey::new(&dataset_id, flow_type).as_trait())
            .cloned()
    }

    pub fn try_get_system_pending_flow(&self, flow_type: SystemFlowType) -> Option<SystemFlowID> {
        self.pending_system_flows.get(&flow_type).cloned()
    }

    pub fn try_get_flow_id_by_task(&self, task_id: TaskID) -> Option<AnyFlowID> {
        self.pending_flows_by_tasks.get(&task_id).cloned()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
