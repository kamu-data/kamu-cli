// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;

use kamu_core::{PullOptions, PullPlanIterationJob};
use kamu_task_system as ts;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct TaskDefinitionDatasetUpdate {
    pub pull_options: PullOptions,
    pub pull_job: PullPlanIterationJob,
}

impl TaskDefinitionDatasetUpdate {
    pub const TASK_TYPE: &'static str = "dev.kamu.tasks.dataset.update";
}

#[async_trait::async_trait]
impl ts::TaskDefinitionBody for TaskDefinitionDatasetUpdate {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    fn task_type(&self) -> &'static str {
        Self::TASK_TYPE
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
