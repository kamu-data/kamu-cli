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
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskErrorDatasetUpdate {
    InputDatasetCompacted(InputDatasetCompactedError),
}

impl TaskErrorDatasetUpdate {
    pub const TYPE_ID: &str = "UpdateDatasetError";

    pub fn into_task_error(self) -> ts::TaskError {
        ts::TaskError {
            error_type: Self::TYPE_ID.to_string(),
            payload: serde_json::to_value(self)
                .expect("Failed to serialize TaskErrorDatasetUpdate into JSON"),
        }
    }

    pub fn from_task_error(task_error: &ts::TaskError) -> Result<Self, InternalError> {
        serde_json::from_value(task_error.payload.clone()).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputDatasetCompactedError {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
