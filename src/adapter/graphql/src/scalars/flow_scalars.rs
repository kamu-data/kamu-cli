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
use kamu_flow_system::{self as fs};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_scalar!(FlowID, fs::FlowID);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct FlowTimingRecords {
    /// Initiation time
    pub initiated_at: DateTime<Utc>,

    /// First scheduling time
    pub first_attempt_scheduled_at: Option<DateTime<Utc>>,

    /// Planned scheduling time (different than first in case of retries)
    pub scheduled_at: Option<DateTime<Utc>>,

    /// Recorded time of last task scheduling
    pub awaiting_executor_since: Option<DateTime<Utc>>,

    /// Recorded start of running (Running state seen at least once)
    pub running_since: Option<DateTime<Utc>>,

    /// Recorded time of finish (successful or failed after retry) or abortion
    /// (Finished state seen at least once)
    pub last_attempt_finished_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowStatus")]
pub enum FlowStatus {
    Waiting,
    Running,
    Retrying,
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

#[derive(SimpleObject, PartialEq, Eq)]
pub struct TimeDelta {
    pub every: i64,
    pub unit: TimeUnit,
}

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    Minutes,
    Hours,
    Days,
    Weeks,
}

impl From<chrono::Duration> for TimeDelta {
    fn from(value: chrono::Duration) -> Self {
        assert!(
            value.num_seconds() >= 0,
            "Positive or zero interval expected, but received [{value}]"
        );

        if value.is_zero() {
            return Self {
                every: 0,
                unit: TimeUnit::Minutes,
            };
        }

        let num_weeks = value.num_weeks();
        if (value - chrono::Duration::try_weeks(num_weeks).unwrap()).is_zero() {
            return Self {
                every: num_weeks,
                unit: TimeUnit::Weeks,
            };
        }

        let num_days = value.num_days();
        if (value - chrono::Duration::try_days(num_days).unwrap()).is_zero() {
            return Self {
                every: num_days,
                unit: TimeUnit::Days,
            };
        }

        let num_hours = value.num_hours();
        if (value - chrono::Duration::try_hours(num_hours).unwrap()).is_zero() {
            return Self {
                every: num_hours,
                unit: TimeUnit::Hours,
            };
        }

        let num_minutes = value.num_minutes();
        if (value - chrono::Duration::try_minutes(num_minutes).unwrap()).is_zero() {
            return Self {
                every: num_minutes,
                unit: TimeUnit::Minutes,
            };
        }

        panic!("Expecting intervals that are clearly dividable by unit, but received [{value}]");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
