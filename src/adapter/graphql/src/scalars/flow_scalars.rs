// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use chrono::{DateTime, Utc};
use kamu_flow_system as fs;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowID(fs::FlowID);

/////////////////////////////////////////////////////////////////////////////////////////

impl From<fs::FlowID> for FlowID {
    fn from(value: fs::FlowID) -> Self {
        FlowID(value)
    }
}

impl From<FlowID> for fs::FlowID {
    fn from(val: FlowID) -> Self {
        val.0
    }
}

impl Deref for FlowID {
    type Target = fs::FlowID;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for FlowID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            match s.parse() {
                Ok(i) => Ok(Self(fs::FlowID::new(i))),
                Err(_) => Err(InputValueError::expected_type(value)),
            }
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct FlowTimingRecords {
    /// Planned activation time (at least, Queued state)
    activate_at: Option<DateTime<Utc>>,

    /// Recorded start of running (Running state seen at least once)
    running_since: Option<DateTime<Utc>>,

    /// Recorded time of finish (succesfull or failed after retry) or abortion
    /// (Finished state seen at least once)
    finished_at: Option<DateTime<Utc>>,
}

impl From<fs::FlowTimingRecords> for FlowTimingRecords {
    fn from(value: fs::FlowTimingRecords) -> Self {
        Self {
            activate_at: value.activate_at,
            running_since: value.running_since,
            finished_at: value.finished_at,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowStatus")]
pub enum FlowStatus {
    Waiting,
    Queued,
    Scheduled,
    Running,
    Finished,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowOutcome")]
pub enum FlowOutcome {
    Success,
    Failed,
    Cancelled,
    Aborted,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::DatasetFlowType")]
pub enum DatasetFlowType {
    Ingest,
    ExecuteQuery,
    Compaction,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::SystemFlowType")]
pub enum SystemFlowType {
    GC,
}

/////////////////////////////////////////////////////////////////////////////////////////
