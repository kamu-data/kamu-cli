// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetID;

use crate::{DatasetFlowType, SystemFlowType};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum FlowKey {
    Dataset(FlowKeyDataset),
    System(FlowKeySystem),
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
