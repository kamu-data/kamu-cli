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
use opendatafabric::DatasetID;

use crate::dataset_flow_key::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct ActiveConfigsState {
    dataset_schedules: HashMap<OwnedDatasetFlowKey, Schedule>,
    system_schedules: HashMap<SystemFlowType, Schedule>,
    dataset_start_conditions: HashMap<OwnedDatasetFlowKey, StartConditionConfiguration>,
}

impl ActiveConfigsState {
    pub fn add_dataset_flow_config(
        &mut self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
        rule: FlowConfigurationRule,
    ) {
        let key = OwnedDatasetFlowKey::new(dataset_id.clone(), flow_type);
        match rule {
            FlowConfigurationRule::Schedule(schedule) => {
                self.dataset_schedules.insert(key, schedule);
            }
            FlowConfigurationRule::StartCondition(condition) => {
                self.dataset_start_conditions.insert(key, condition);
            }
        }
    }

    pub fn add_system_flow_config(&mut self, flow_type: SystemFlowType, schedule: Schedule) {
        self.system_schedules.insert(flow_type, schedule);
    }

    pub fn drop_dataset_configs(&mut self, dataset_id: &DatasetID) {
        for flow_type in DatasetFlowType::iterator() {
            self.drop_dataset_flow_config(dataset_id, flow_type);
        }
    }

    pub fn drop_dataset_flow_config(&mut self, dataset_id: &DatasetID, flow_type: DatasetFlowType) {
        let key = BorrowedDatasetFlowKey::new(dataset_id, flow_type);
        self.dataset_schedules.remove(key.as_trait());
        self.dataset_start_conditions.remove(key.as_trait());
    }

    pub fn drop_system_flow_config(&mut self, flow_type: SystemFlowType) {
        self.system_schedules.remove(&flow_type);
    }

    pub fn try_get_dataset_schedule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<Schedule> {
        self.dataset_schedules
            .get(BorrowedDatasetFlowKey::new(&dataset_id, flow_type).as_trait())
            .cloned()
    }

    pub fn try_get_dataset_start_condition(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<StartConditionConfiguration> {
        self.dataset_start_conditions
            .get(BorrowedDatasetFlowKey::new(&dataset_id, flow_type).as_trait())
            .cloned()
    }

    pub fn try_get_system_schedule(&self, flow_type: SystemFlowType) -> Option<Schedule> {
        self.system_schedules.get(&flow_type).cloned()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
