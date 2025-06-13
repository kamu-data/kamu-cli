// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
pub struct ProbeTaskPlanner {}

impl ProbeTaskPlanner {
    #[tracing::instrument(level = "debug", skip_all, fields(?probe_plan))]
    async fn plan_probe(
        &self,
        probe_plan: &LogicalPlanProbe,
    ) -> Result<TaskDefinition, InternalError> {
        Ok(TaskDefinition::new(TaskDefinitionProbe {
            probe: probe_plan.clone(),
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for ProbeTaskPlanner {
    fn supported_task_type(&self) -> &str {
        TaskDefinitionProbe::TASK_TYPE
    }

    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        let kamu_task_system::LogicalPlan::Probe(probe_plan) = logical_plan else {
            panic!("ProbeTaskPlanner received an unsupported logical plan type: {logical_plan:?}",);
        };

        self.plan_probe(probe_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
