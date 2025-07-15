// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_flow_system::*;
use kamu_task_system as ts;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn FlowDispatcher)]
#[dill::meta(FlowDispatcherMeta {
    flow_type: FLOW_TYPE_SYSTEM_GC,
})]
pub struct FlowDispatcherSystemGC {}

#[async_trait::async_trait]
impl FlowDispatcher for FlowDispatcherSystemGC {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &FlowBinding,
        _maybe_config_snapshot: Option<&FlowConfigurationRule>,
        _maybe_flow_run_arguments: Option<&FlowRunArguments>,
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
        _flow_binding: &FlowBinding,
        _activation_cause: FlowActivationCause,
        _maybe_config_snapshot: Option<FlowConfigurationRule>,
    ) -> Result<(), InternalError> {
        // No propagation needed for system GC dispatcher
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
