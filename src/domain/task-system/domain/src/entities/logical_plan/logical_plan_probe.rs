// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use serde::{Deserialize, Serialize};

use crate::{LogicalPlan, TaskOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task that can be used for testing the scheduling system
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LogicalPlanProbe {
    /// ID of the dataset this task should be associated with
    pub dataset_id: Option<odf::DatasetID>,
    pub busy_time: Option<std::time::Duration>,
    pub end_with_outcome: Option<TaskOutcome>,
}

impl LogicalPlanProbe {
    pub const TYPE_ID: &str = "Probe";

    pub fn into_logical_plan(self) -> LogicalPlan {
        LogicalPlan {
            plan_type: Self::TYPE_ID.to_string(),
            payload: serde_json::to_value(self)
                .expect("Failed to serialize impl LogicalPlanProbe into JSON"),
        }
    }

    pub fn from_logical_plan(logical_plan: &LogicalPlan) -> Result<Self, InternalError> {
        serde_json::from_value(logical_plan.payload.clone()).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
