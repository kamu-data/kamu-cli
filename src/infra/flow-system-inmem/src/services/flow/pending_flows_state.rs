// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_flow_system::*;
use kamu_task_system::*;
use opendatafabric::DatasetID;

use crate::dataset_flow_key::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct PendingFlowsState {
    pending_dataset_flows: HashMap<FlowKeyDataset, FlowID>,
    pending_system_flows: HashMap<SystemFlowType, FlowID>,
    pending_flows_by_tasks: HashMap<TaskID, FlowID>,
}

impl PendingFlowsState {
    pub fn add_pending_flow(&mut self, flow_key: FlowKey, flow_id: FlowID) {
        match flow_key {
            FlowKey::Dataset(flow_key) => {
                self.pending_dataset_flows.insert(flow_key, flow_id);
            }
            FlowKey::System(flow_key) => {
                self.pending_system_flows
                    .insert(flow_key.flow_type, flow_id);
            }
        }
    }

    pub fn track_flow_task(&mut self, flow_id: FlowID, task_id: TaskID) {
        self.pending_flows_by_tasks.insert(task_id, flow_id);
    }

    pub fn drop_pending_flow(&mut self, flow_key: &FlowKey) -> Option<FlowID> {
        match flow_key {
            FlowKey::Dataset(flow_key) => {
                self.drop_dataset_pending_flow(&flow_key.dataset_id, flow_key.flow_type)
            }
            FlowKey::System(flow_key) => self.pending_system_flows.remove(&flow_key.flow_type),
        }
    }

    pub fn drop_dataset_pending_flow(
        &mut self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<FlowID> {
        self.pending_dataset_flows
            .remove(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
    }

    pub fn untrack_flow_by_task(&mut self, task_id: TaskID) {
        self.pending_flows_by_tasks.remove(&task_id);
    }

    pub fn try_get_pending_flow(&self, flow_key: &FlowKey) -> Option<FlowID> {
        match flow_key {
            FlowKey::Dataset(flow_key) => self
                .pending_dataset_flows
                .get(
                    BorrowedFlowKeyDataset::new(&flow_key.dataset_id, flow_key.flow_type)
                        .as_trait(),
                )
                .cloned(),
            FlowKey::System(flow_key) => {
                self.pending_system_flows.get(&flow_key.flow_type).cloned()
            }
        }
    }

    pub fn try_get_flow_id_by_task(&self, task_id: TaskID) -> Option<FlowID> {
        self.pending_flows_by_tasks.get(&task_id).cloned()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
