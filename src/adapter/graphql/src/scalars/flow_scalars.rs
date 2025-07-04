// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
};
use kamu_flow_system::{self as fs, FLOW_TYPE_SYSTEM_GC};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_scalar!(FlowID, fs::FlowID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Eq, PartialEq)]
pub enum FlowKey {
    Dataset(FlowKeyDataset),
    System(FlowKeySystem),
}

impl From<fs::FlowBinding> for FlowKey {
    fn from(value: fs::FlowBinding) -> Self {
        match value.scope {
            fs::FlowScope::Dataset { dataset_id } => Self::Dataset(FlowKeyDataset {
                dataset_id: dataset_id.into(),
                flow_type: match value.flow_type.as_str() {
                    FLOW_TYPE_DATASET_INGEST => DatasetFlowType::Ingest,
                    FLOW_TYPE_DATASET_TRANSFORM => DatasetFlowType::ExecuteTransform,
                    FLOW_TYPE_DATASET_COMPACT => DatasetFlowType::HardCompaction,
                    FLOW_TYPE_DATASET_RESET => DatasetFlowType::Reset,
                    _ => panic!("Unexpected dataset flow type: {:?}", value.flow_type),
                },
            }),
            fs::FlowScope::System => Self::System(FlowKeySystem {
                flow_type: match value.flow_type.as_str() {
                    FLOW_TYPE_SYSTEM_GC => SystemFlowType::GC,
                    _ => panic!("Unexpected system flow type: {:?}", value.flow_type),
                },
            }),
        }
    }
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowKeyDataset {
    pub dataset_id: DatasetID<'static>,
    pub flow_type: DatasetFlowType,
}

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowKeySystem {
    pub flow_type: SystemFlowType,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct FlowTimingRecords {
    /// Recorded time of last task scheduling
    awaiting_executor_since: Option<DateTime<Utc>>,

    /// Recorded start of running (Running state seen at least once)
    running_since: Option<DateTime<Utc>>,

    /// Recorded time of finish (successful or failed after retry) or abortion
    /// (Finished state seen at least once)
    finished_at: Option<DateTime<Utc>>,
}

impl From<fs::FlowTimingRecords> for FlowTimingRecords {
    fn from(value: fs::FlowTimingRecords) -> Self {
        Self {
            awaiting_executor_since: value.awaiting_executor_since,
            running_since: value.running_since,
            finished_at: value.finished_at,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowStatus")]
pub enum FlowStatus {
    Waiting,
    Running,
    Finished,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
pub enum DatasetFlowType {
    Ingest,
    ExecuteTransform,
    HardCompaction,
    Reset,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum SystemFlowType {
    GC,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_dataset_flow_type(dataset_flow_type: DatasetFlowType) -> &'static str {
    match dataset_flow_type {
        DatasetFlowType::Ingest => FLOW_TYPE_DATASET_INGEST,
        DatasetFlowType::ExecuteTransform => FLOW_TYPE_DATASET_TRANSFORM,
        DatasetFlowType::HardCompaction => FLOW_TYPE_DATASET_COMPACT,
        DatasetFlowType::Reset => FLOW_TYPE_DATASET_RESET,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
