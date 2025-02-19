// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_scalar!(FlowID, fs::FlowID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Clone, Eq, PartialEq)]
pub enum FlowKey {
    Dataset(FlowKeyDataset),
    System(FlowKeySystem),
}

impl From<fs::FlowKey> for FlowKey {
    fn from(value: fs::FlowKey) -> Self {
        match value {
            fs::FlowKey::Dataset(fk_dataset) => Self::Dataset(fk_dataset.into()),
            fs::FlowKey::System(fk_system) => Self::System(fk_system.into()),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowKeyDataset {
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
}

impl From<fs::FlowKeyDataset> for FlowKeyDataset {
    fn from(value: fs::FlowKeyDataset) -> Self {
        Self {
            dataset_id: value.dataset_id.into(),
            flow_type: value.flow_type.into(),
        }
    }
}

#[derive(SimpleObject, Clone, PartialEq, Eq)]
pub struct FlowKeySystem {
    pub flow_type: SystemFlowType,
}

impl From<fs::FlowKeySystem> for FlowKeySystem {
    fn from(value: fs::FlowKeySystem) -> Self {
        Self {
            flow_type: value.flow_type.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
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

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowStatus")]
pub enum FlowStatus {
    Waiting,
    Running,
    Finished,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::DatasetFlowType")]
pub enum DatasetFlowType {
    Ingest,
    ExecuteTransform,
    HardCompaction,
    Reset,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::SystemFlowType")]
pub enum SystemFlowType {
    GC,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
