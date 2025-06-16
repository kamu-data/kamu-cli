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
#[dill::interface(dyn TaskRunner)]
pub struct ProbeTaskRunner {}

impl ProbeTaskRunner {
    #[tracing::instrument(level = "debug", skip_all, fields(?task_probe))]
    async fn run_probe(
        &self,
        task_probe: TaskDefinitionProbe,
    ) -> Result<TaskOutcome, InternalError> {
        if let Some(busy_time) = task_probe.probe.busy_time {
            tokio::time::sleep(busy_time).await;
        }
        Ok(task_probe
            .probe
            .end_with_outcome
            .clone()
            .unwrap_or(TaskOutcome::Success(TaskResult::empty())))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for ProbeTaskRunner {
    fn supported_task_type(&self) -> &str {
        TaskDefinitionProbe::TASK_TYPE
    }

    async fn run_task(
        &self,
        task_definition: kamu_task_system::TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        let task_probe = task_definition
            .downcast::<TaskDefinitionProbe>()
            .expect("Mismatched task type for ProbeTaskRunner");

        self.run_probe(*task_probe).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
