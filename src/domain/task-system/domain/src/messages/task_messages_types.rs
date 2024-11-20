// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::{TaskID, TaskMetadata, TaskOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskProgressMessage {
    Running(TaskProgressMessageRunning),
    Finished(TaskProgressMessageFinished),
}

impl TaskProgressMessage {
    pub fn running(
        event_time: DateTime<Utc>,
        task_id: TaskID,
        task_metadata: TaskMetadata,
    ) -> Self {
        Self::Running(TaskProgressMessageRunning {
            event_time,
            task_id,
            task_metadata,
        })
    }

    pub fn finished(
        event_time: DateTime<Utc>,
        task_id: TaskID,
        task_metadata: TaskMetadata,
        outcome: TaskOutcome,
    ) -> Self {
        Self::Finished(TaskProgressMessageFinished {
            event_time,
            task_id,
            task_metadata,
            outcome,
        })
    }
}

impl Message for TaskProgressMessage {
    fn version(&self) -> u32 {
        1
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgressMessageRunning {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub task_metadata: TaskMetadata,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgressMessageFinished {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub task_metadata: TaskMetadata,
    pub outcome: TaskOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
