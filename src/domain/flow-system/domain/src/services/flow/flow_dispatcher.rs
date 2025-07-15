// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_task_system as ts;

use crate::{
    FlowActivationCause,
    FlowBinding,
    FlowConfigurationRule,
    FlowRunArguments,
    RetryPolicy,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowDispatcher: Send + Sync {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &FlowBinding,
        maybe_config_snapshot: Option<&FlowConfigurationRule>,
        maybe_flow_run_arguments: Option<&FlowRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError>;

    async fn propagate_success(
        &self,
        flow_binding: &FlowBinding,
        activation_cause: FlowActivationCause,
        maybe_config_snapshot: Option<FlowConfigurationRule>,
    ) -> Result<(), InternalError>;

    fn default_retry_policy(&self) -> Option<RetryPolicy> {
        None
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowDispatcherMeta {
    pub flow_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_TYPE_SYSTEM_GC: &str = "dev.kamu.flow.system.gc";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
