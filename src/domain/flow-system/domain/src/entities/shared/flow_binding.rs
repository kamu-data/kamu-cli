// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{DatasetFlowType, FlowKey, SystemFlowType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct FlowBinding {
    pub flow_type: String,
    pub scope: FlowScope,
}

impl FlowBinding {
    pub fn new_dataset(dataset_id: odf::DatasetID, flow_type: &str) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::Dataset { dataset_id },
        }
    }

    pub fn new_system(flow_type: &str) -> Self {
        Self {
            flow_type: flow_type.to_string(),
            scope: FlowScope::System,
        }
    }

    pub fn dataset_id_or_die(&self) -> Result<odf::DatasetID, InternalError> {
        let FlowScope::Dataset { dataset_id } = &self.scope else {
            return InternalError::bail("Expecting dataset flow binding scope");
        };
        Ok(dataset_id.clone())
    }
}

impl From<&FlowKey> for FlowBinding {
    fn from(flow_key: &FlowKey) -> Self {
        let flow_type = map_flow_type(flow_key).to_string();
        match flow_key {
            FlowKey::Dataset(fk_dataset) => FlowBinding {
                flow_type,
                scope: FlowScope::Dataset {
                    dataset_id: fk_dataset.dataset_id.clone(),
                },
            },
            FlowKey::System(_) => FlowBinding {
                flow_type,
                scope: FlowScope::System,
            },
        }
    }
}

impl From<&FlowBinding> for FlowKey {
    fn from(flow_binding: &FlowBinding) -> Self {
        match &flow_binding.scope {
            FlowScope::Dataset { dataset_id } => FlowKey::dataset(
                dataset_id.clone(),
                match flow_binding.flow_type.as_str() {
                    "dev.kamu.flow.dataset.ingest" => DatasetFlowType::Ingest,
                    "dev.kamu.flow.dataset.compact" => DatasetFlowType::HardCompaction,
                    "dev.kamu.flow.dataset.transform" => DatasetFlowType::ExecuteTransform,
                    "dev.kamu.flow.dataset.reset" => DatasetFlowType::Reset,
                    _ => panic!("Unknown dataset flow type: {}", flow_binding.flow_type),
                },
            ),
            FlowScope::System => FlowKey::system(match flow_binding.flow_type.as_str() {
                "dev.kamu.flow.system.gc" => SystemFlowType::GC,
                _ => panic!("Unknown system flow type: {}", flow_binding.flow_type),
            }),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(tag = "type", rename_all = "PascalCase")]
pub enum FlowScope {
    Dataset { dataset_id: odf::DatasetID },
    System,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: temporary mapping until FlowKey is fully replaced
pub fn map_flow_type(flow_key: &FlowKey) -> &'static str {
    match flow_key {
        FlowKey::Dataset(fk_dataset) => map_dataset_flow_type(fk_dataset.flow_type),
        FlowKey::System(fk_system) => match fk_system.flow_type {
            SystemFlowType::GC => "dev.kamu.flow.system.gc",
        },
    }
}

pub fn map_dataset_flow_type(dataset_flow_type: DatasetFlowType) -> &'static str {
    match dataset_flow_type {
        DatasetFlowType::Ingest => "dev.kamu.flow.dataset.ingest",
        DatasetFlowType::HardCompaction => "dev.kamu.flow.dataset.compact",
        DatasetFlowType::ExecuteTransform => "dev.kamu.flow.dataset.transform",
        DatasetFlowType::Reset => "dev.kamu.flow.dataset.reset",
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
