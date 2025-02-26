// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskAgent: Sync + Send {
    /// Runs the agent main loop
    async fn run(&self) -> Result<(), InternalError>;

    /// Runs single task only, blocks until it is available (for tests only!)
    async fn run_single_task(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskAgentConfig {
    /// Defines interval between task executor checks whether there are any new
    /// tasks
    pub task_checking_interval: chrono::Duration,
}

impl TaskAgentConfig {
    pub fn new(task_checking_interval: chrono::Duration) -> Self {
        Self {
            task_checking_interval,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
