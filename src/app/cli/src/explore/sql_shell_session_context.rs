// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::dataframe::DataFrame;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::LogicalPlan;
use datafusion::object_store::ObjectStore;
use datafusion::prelude::SessionContext;
use kamu::domain::SqlPolicy;
use kamu_datafusion_cli::cli_context::CliSessionContext;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Session of the interactive SQL shell: every statement is checked against
/// the session's [`SqlPolicy`] before it runs
pub struct SqlShellSessionContext {
    ctx: SessionContext,
}

impl SqlShellSessionContext {
    pub fn new(ctx: SessionContext) -> Self {
        Self { ctx }
    }
}

#[async_trait::async_trait]
impl CliSessionContext for SqlShellSessionContext {
    fn task_ctx(&self) -> Arc<TaskContext> {
        self.ctx.task_ctx()
    }

    fn session_state(&self) -> SessionState {
        self.ctx.state()
    }

    // Object stores come from the workspace's registry, which does not accept
    // stores at runtime. A location it does not know fails when it is read or
    // written
    fn register_object_store(
        &self,
        _url: &url::Url,
        _object_store: Arc<dyn ObjectStore>,
    ) -> Option<Arc<dyn ObjectStore + 'static>> {
        None
    }

    fn register_table_options_extension_from_scheme(&self, scheme: &str) {
        CliSessionContext::register_table_options_extension_from_scheme(&self.ctx, scheme);
    }

    async fn execute_logical_plan(&self, plan: LogicalPlan) -> Result<DataFrame, DataFusionError> {
        SqlPolicy::execute_logical_plan(&self.ctx, plan).await
    }

    // Rejects statements before the shell registers object stores for their
    // locations, so a rejected `COPY` or external table has no side effects
    fn check_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        let policy = SqlPolicy::of(self.ctx.state_ref().read().config());
        policy.check_plan(plan)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
