// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

use crate::scalars::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone, PartialEq, Eq)]
#[graphql(
    field(name = "id", method = "id", type = "&TaskID"),
    field(name = "status", method = "status", type = "TaskStatus")
)]
pub enum Task {
    PullDataset(PullDatasetTask),
}

impl Task {
    // TODO: MOCK
    pub(crate) fn mock() -> Self {
        Self::PullDataset(PullDatasetTask::new(TaskID::from("1")))
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullDatasetTask {
    task_id: TaskID,
}

#[Object]
impl PullDatasetTask {
    #[graphql(skip)]
    pub fn new(task_id: TaskID) -> Self {
        Self { task_id }
    }

    /// Unique and stable identitfier of this user account
    async fn id(&self) -> &TaskID {
        &self.task_id
    }

    async fn status(&self) -> TaskStatus {
        TaskStatus::Queued
    }
}
