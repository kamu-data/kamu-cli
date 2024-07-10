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

use crate::{TaskID, TaskOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MESSAGE_KAMU_TASK_RUNNING: &str = "dev.kamu.task.running";
pub const MESSAGE_KAMU_TASK_FINISHED: &str = "dev.kamu.task.finished";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunningMessage {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
}

impl Message for TaskRunningMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_TASK_RUNNING
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskFinishedMessage {
    pub event_time: DateTime<Utc>,
    pub task_id: TaskID,
    pub outcome: TaskOutcome,
}

impl Message for TaskFinishedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_TASK_FINISHED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
