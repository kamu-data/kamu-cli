// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetID;

use crate::{DatasetFlowID, DatasetFlowType, FlowStrategy};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct DatasetFlowStrategy {}

impl FlowStrategy for DatasetFlowStrategy {
    type FlowID = DatasetFlowID;
    type FlowKey = DatasetFlowKey;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct DatasetFlowKey {
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
}

impl DatasetFlowKey {
    pub fn new(dataset_id: DatasetID, flow_type: DatasetFlowType) -> Self {
        Self {
            dataset_id,
            flow_type,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
