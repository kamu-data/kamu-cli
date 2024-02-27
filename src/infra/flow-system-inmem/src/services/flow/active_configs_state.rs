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
use opendatafabric::DatasetID;

use crate::dataset_flow_key::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct ActiveConfigsState {
    dataset_schedules: HashMap<FlowKeyDataset, Schedule>,
    system_schedules: HashMap<SystemFlowType, Schedule>,
    dataset_batching_rules: HashMap<FlowKeyDataset, BatchingRule>,
}

impl ActiveConfigsState {
    pub fn add_dataset_flow_config(
        &mut self,
        flow_key: &FlowKeyDataset,
        rule: FlowConfigurationRule,
    ) {
        let key = flow_key.clone();
        match rule {
            FlowConfigurationRule::Schedule(schedule) => {
                self.dataset_schedules.insert(key, schedule);
            }
            FlowConfigurationRule::BatchingRule(batching) => {
                self.dataset_batching_rules.insert(key, batching);
            }
        }
    }

    pub fn add_system_flow_config(&mut self, flow_type: SystemFlowType, schedule: Schedule) {
        self.system_schedules.insert(flow_type, schedule);
    }

    pub fn drop_dataset_configs(&mut self, dataset_id: &DatasetID) {
        for flow_type in DatasetFlowType::all() {
            self.drop_dataset_flow_config(BorrowedFlowKeyDataset::new(dataset_id, *flow_type));
        }
    }

    pub fn drop_flow_config(&mut self, flow_key: &FlowKey) {
        match flow_key {
            FlowKey::Dataset(flow_key) => {
                self.drop_dataset_flow_config(flow_key.borrowed_key());
            }
            FlowKey::System(flow_key) => {
                self.system_schedules.remove(&flow_key.flow_type);
            }
        }
    }

    fn drop_dataset_flow_config(&mut self, flow_key: BorrowedFlowKeyDataset) {
        self.dataset_schedules.remove(flow_key.as_trait());
        self.dataset_batching_rules.remove(flow_key.as_trait());
    }

    pub fn try_get_flow_schedule(&self, flow_key: &FlowKey) -> Option<Schedule> {
        match flow_key {
            FlowKey::Dataset(flow_key) => self
                .dataset_schedules
                .get(
                    BorrowedFlowKeyDataset::new(&flow_key.dataset_id, flow_key.flow_type)
                        .as_trait(),
                )
                .cloned(),
            FlowKey::System(flow_key) => self.system_schedules.get(&flow_key.flow_type).cloned(),
        }
    }

    pub fn try_get_dataset_batching_rule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<BatchingRule> {
        self.dataset_batching_rules
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .copied()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
