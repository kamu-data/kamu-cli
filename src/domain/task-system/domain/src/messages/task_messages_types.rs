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

const TASK_PROGRESS_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents messages related to the progress of a task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskProgressMessage {
    /// Message indicating that a task is currently running
    Running(TaskProgressMessageRunning),

    /// Message indicating that a task has finished execution
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
    fn version() -> u32 {
        TASK_PROGRESS_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Contains details about a running task
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgressMessageRunning {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the task
    pub task_id: TaskID,

    /// Metadata associated with the task
    pub task_metadata: TaskMetadata,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskProgressMessageFinished {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the task
    pub task_id: TaskID,

    /// Metadata associated with the task
    pub task_metadata: TaskMetadata,

    /// The outcome of the task execution
    pub outcome: TaskOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
