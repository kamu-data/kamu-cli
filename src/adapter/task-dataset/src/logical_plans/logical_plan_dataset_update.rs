// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_task_system as ts;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task to perform the resetting of a dataset
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LogicalPlanDatasetUpdate {
    pub dataset_id: odf::DatasetID,
    pub fetch_uncacheable: bool,
}

impl LogicalPlanDatasetUpdate {
    pub const TYPE_ID: &str = "UpdateDataset";

    pub fn into_logical_plan(self) -> ts::LogicalPlan {
        ts::LogicalPlan {
            plan_type: Self::TYPE_ID.to_string(),
            payload: serde_json::to_value(self)
                .expect("Failed to serialize LogicalPlanDatasetUpdate into JSON"),
        }
    }

    pub fn from_logical_plan(logical_plan: &ts::LogicalPlan) -> Result<Self, InternalError> {
        serde_json::from_value(logical_plan.payload.clone()).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
