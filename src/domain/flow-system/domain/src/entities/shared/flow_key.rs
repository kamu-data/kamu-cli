// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetID;

use crate::{AnyFlowType, DatasetFlowType, SystemFlowType};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum FlowKey {
    Dataset(FlowKeyDataset),
    System(FlowKeySystem),
}

impl FlowKey {
    pub fn get_type(&self) -> AnyFlowType {
        match self {
            Self::Dataset(fk_dataset) => AnyFlowType::Dataset(fk_dataset.flow_type),
            Self::System(fk_system) => AnyFlowType::System(fk_system.flow_type),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FlowKeyDataset {
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
}

impl FlowKeyDataset {
    pub fn new(dataset_id: DatasetID, flow_type: DatasetFlowType) -> Self {
        Self {
            dataset_id,
            flow_type,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct FlowKeySystem {
    pub flow_type: SystemFlowType,
}

impl FlowKeySystem {
    pub fn new(flow_type: SystemFlowType) -> Self {
        Self { flow_type }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<FlowKeyDataset> for FlowKey {
    fn from(value: FlowKeyDataset) -> Self {
        Self::Dataset(value)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<SystemFlowType> for FlowKey {
    fn from(value: SystemFlowType) -> Self {
        Self::System(FlowKeySystem::new(value))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
