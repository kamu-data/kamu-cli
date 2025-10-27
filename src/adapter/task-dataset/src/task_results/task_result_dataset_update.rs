// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{PullResult, PullResultUpToDate};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_task_system::task_result_struct! {
    pub struct TaskResultDatasetUpdate {
        pub pull_result: PullResult,
    }
    => "UpdateDatasetResult"
}

impl TaskResultDatasetUpdate {
    pub fn try_as_increment(&self) -> Option<(Option<&odf::Multihash>, &odf::Multihash)> {
        match &self.pull_result {
            PullResult::Updated {
                old_head, new_head, ..
            } => Some((old_head.as_ref(), new_head)),
            PullResult::UpToDate(_) => None,
        }
    }

    pub fn try_as_up_to_date(&self) -> Option<&PullResultUpToDate> {
        match &self.pull_result {
            PullResult::UpToDate(up_to_date) => Some(up_to_date),
            PullResult::Updated { .. } => None,
        }
    }

    pub fn has_more(&self) -> bool {
        match &self.pull_result {
            PullResult::Updated { has_more, .. } => *has_more,
            PullResult::UpToDate(_) => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
