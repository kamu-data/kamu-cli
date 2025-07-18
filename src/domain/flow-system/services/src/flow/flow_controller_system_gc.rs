// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use kamu_flow_system::*;
use kamu_task_system as ts;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn FlowController)]
#[dill::meta(FlowControllerMeta {
    flow_type: FLOW_TYPE_SYSTEM_GC,
})]
pub struct FlowControllerSystemGC {}

#[async_trait::async_trait]
impl FlowController for FlowControllerSystemGC {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_SYSTEM_GC
    }

    async fn build_task_logical_plan(
        &self,
        flow_binding: &FlowBinding,
        _maybe_config_snapshot: Option<&FlowConfigurationRule>,
        _maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        if !matches!(flow_binding.scope, FlowScope::System) {
            return InternalError::bail("Expecting system flow binding scope for GC dispatcher");
        }

        // TODO: replace on correct logical plan
        Ok(ts::LogicalPlanProbe {
            dataset_id: None,
            busy_time: Some(std::time::Duration::from_secs(20)),
            end_with_outcome: Some(ts::TaskOutcome::Success(ts::TaskResult::empty())),
        }
        .into_logical_plan())
    }

    async fn propagate_success(
        &self,
        _: &FlowState,
        _: &ts::TaskResult,
        _: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        // No propagation needed for system GC dispatcher
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
