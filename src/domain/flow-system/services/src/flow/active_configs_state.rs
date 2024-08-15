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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub(crate) struct ActiveConfigsState {
    system_schedules: HashMap<SystemFlowType, Schedule>,
    dataset_transform_rules: HashMap<FlowKeyDataset, TransformRule>,
    dataset_reset_rules: HashMap<FlowKeyDataset, ResetRule>,
    dataset_compaction_rules: HashMap<FlowKeyDataset, CompactionRule>,
    dataset_ingest_rules: HashMap<FlowKeyDataset, IngestRule>,
}

impl ActiveConfigsState {
    pub fn add_dataset_flow_config(
        &mut self,
        flow_key: &FlowKeyDataset,
        rule: FlowConfigurationRule,
    ) {
        let key = flow_key.clone();
        match rule {
            FlowConfigurationRule::Schedule(_) => {
                unreachable!()
            }
            FlowConfigurationRule::IngestRule(ingest_rule) => {
                self.dataset_ingest_rules.insert(key, ingest_rule);
            }
            FlowConfigurationRule::ResetRule(reset) => {
                self.dataset_reset_rules.insert(key, reset);
            }
            FlowConfigurationRule::TransformRule(transform) => {
                self.dataset_transform_rules.insert(key, transform);
            }
            FlowConfigurationRule::CompactionRule(compaction) => {
                self.dataset_compaction_rules.insert(key, compaction);
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
        self.dataset_ingest_rules.remove(flow_key.as_trait());
        self.dataset_transform_rules.remove(flow_key.as_trait());
        self.dataset_compaction_rules.remove(flow_key.as_trait());
        self.dataset_reset_rules.remove(flow_key.as_trait());
    }

    pub fn try_get_flow_schedule(&self, flow_key: &FlowKey) -> Option<Schedule> {
        match flow_key {
            FlowKey::Dataset(flow_key) => self
                .dataset_ingest_rules
                .get(
                    BorrowedFlowKeyDataset::new(&flow_key.dataset_id, flow_key.flow_type)
                        .as_trait(),
                )
                .map(|ingest_rule| ingest_rule.schedule_condition.clone()),
            FlowKey::System(flow_key) => self.system_schedules.get(&flow_key.flow_type).cloned(),
        }
    }

    pub fn try_get_dataset_transform_rule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<TransformRule> {
        self.dataset_transform_rules
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .copied()
    }

    pub fn try_get_dataset_ingest_rule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<IngestRule> {
        self.dataset_ingest_rules
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .cloned()
    }

    pub fn try_get_dataset_compaction_rule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<CompactionRule> {
        self.dataset_compaction_rules
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .copied()
    }

    pub fn try_get_dataset_reset_rule(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<ResetRule> {
        self.dataset_reset_rules
            .get(BorrowedFlowKeyDataset::new(dataset_id, flow_type).as_trait())
            .cloned()
    }

    pub fn try_get_config_snapshot_by_key(
        &self,
        flow_key: &FlowKey,
    ) -> Option<FlowConfigurationSnapshot> {
        match flow_key {
            FlowKey::System(_) => self
                .try_get_flow_schedule(flow_key)
                .map(FlowConfigurationSnapshot::Schedule),
            FlowKey::Dataset(dataset_flow_key) => match dataset_flow_key.flow_type {
                DatasetFlowType::ExecuteTransform => self
                    .try_get_dataset_transform_rule(
                        &dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .map(FlowConfigurationSnapshot::Transform),
                DatasetFlowType::Ingest => self
                    .try_get_dataset_ingest_rule(
                        &dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .map(FlowConfigurationSnapshot::Ingest),
                DatasetFlowType::Reset => self
                    .try_get_dataset_reset_rule(
                        &dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .map(FlowConfigurationSnapshot::Reset),
                DatasetFlowType::HardCompaction => self
                    .try_get_dataset_compaction_rule(
                        &dataset_flow_key.dataset_id,
                        dataset_flow_key.flow_type,
                    )
                    .map(FlowConfigurationSnapshot::Compaction),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
