// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::ResetResult;
use kamu_task_system as ts;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResultDatasetReset {
    pub reset_result: ResetResult,
}

impl TaskResultDatasetReset {
    pub const TYPE_ID: &str = "ResetDatasetResult";

    pub fn into_task_result(self) -> ts::TaskResult {
        ts::TaskResult {
            result_type: Self::TYPE_ID.to_string(),
            payload: serde_json::to_value(self)
                .expect("Failed to serialize TaskResultDatasetReset into JSON"),
        }
    }

    pub fn from_task_result(task_result: &ts::TaskResult) -> Result<Self, InternalError> {
        serde_json::from_value(task_result.payload.clone()).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
