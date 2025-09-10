// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_task_system as ts;

use crate::{FlowBinding, FlowState, ReactiveRule};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowController: Send + Sync {
    fn flow_type(&self) -> &'static str;

    async fn ensure_flow_sensor(
        &self,
        flow_binding: &FlowBinding,
        _activation_time: DateTime<Utc>,
        _reactive_rule: ReactiveRule,
    ) -> Result<(), InternalError> {
        tracing::error!(
            "{} does not expect flow sensors, flow_binding: {:?}",
            self.flow_type(),
            flow_binding
        );

        Ok(())
    }

    async fn build_task_logical_plan(
        &self,
        flow: &FlowState,
    ) -> Result<ts::LogicalPlan, InternalError>;

    async fn propagate_success(
        &self,
        success_flow_state: &FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError>;

    async fn make_flow_sort_key(&self, flow_binding: &FlowBinding)
    -> Result<String, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_flow_controller_from_catalog(
    target_catalog: &dill::Catalog,
    flow_type: &str,
) -> Result<Arc<dyn FlowController>, InternalError> {
    // Find a controller for this flow type in dependency catalog
    target_catalog
        .builders_for_with_meta::<dyn FlowController, _>(|meta: &FlowControllerMeta| {
            meta.flow_type == flow_type
        })
        .next()
        .map(|builder| builder.get(target_catalog))
        .transpose()
        .int_err()?
        .ok_or_else(|| {
            InternalError::new(format!("Flow controller for type '{flow_type}' not found",))
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowControllerMeta {
    pub flow_type: &'static str,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const FLOW_TYPE_SYSTEM_GC: &str = "dev.kamu.flow.system.gc";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
