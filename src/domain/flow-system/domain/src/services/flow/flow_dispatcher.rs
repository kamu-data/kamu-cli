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
use kamu_task_system as ts;

use crate::{FlowBinding, FlowConfigurationRule, FlowState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowDispatcher: Send + Sync {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &FlowBinding,
        maybe_config_snapshot: Option<&FlowConfigurationRule>,
        maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError>;

    async fn propagate_success(
        &self,
        success_flow_state: &FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowDispatcherMeta {
    pub flow_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_TYPE_SYSTEM_GC: &str = "dev.kamu.flow.system.gc";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
