// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{LogicalPlan, TaskDefinition, TaskID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TaskDefinitionPlanner: Send + Sync {
    async fn prepare_task_definition(
        &self,
        task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct TaskDefinitionPlannerMeta {
    pub logic_plan_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
