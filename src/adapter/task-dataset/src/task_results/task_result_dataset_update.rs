// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{PullResult, PullResultUpToDate};
use kamu_task_system as ts;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResultDatasetUpdate {
    pub pull_result: PullResult,
}

impl TaskResultDatasetUpdate {
    pub const TYPE_ID: &str = "UpdateDatasetResult";

    pub fn into_task_result(self) -> ts::TaskResult {
        ts::TaskResult {
            result_type: Self::TYPE_ID.to_string(),
            payload: serde_json::to_value(self)
                .expect("Failed to serialize TaskResultDatasetUpdate into JSON"),
        }
    }

    pub fn from_task_result(task_result: &ts::TaskResult) -> Result<Self, InternalError> {
        serde_json::from_value(task_result.payload.clone()).int_err()
    }

    pub fn try_as_increment(&self) -> Option<(Option<&odf::Multihash>, &odf::Multihash)> {
        match &self.pull_result {
            PullResult::Updated { old_head, new_head } => Some((old_head.as_ref(), new_head)),
            PullResult::UpToDate(_) => None,
        }
    }

    pub fn try_as_up_to_date(&self) -> Option<&PullResultUpToDate> {
        match &self.pull_result {
            PullResult::UpToDate(up_to_date) => Some(up_to_date),
            PullResult::Updated { .. } => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
