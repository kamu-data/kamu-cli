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

use crate::{FlowConfigurationRule, FlowKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait FlowTaskFactory: Send + Sync {
    async fn build_task_logical_plan(
        &self,
        flow_key: &FlowKey,
        maybe_config_snapshot: Option<&FlowConfigurationRule>,
    ) -> Result<ts::LogicalPlan, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
