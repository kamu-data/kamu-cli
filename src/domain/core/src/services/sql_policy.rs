// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::common::plan_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{DdlStatement, LogicalPlan};
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Kinds of SQL statements a session accepts besides read-only queries.
///
/// A session carries its policy as a [`SessionConfig`] extension. A session
/// without one is read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SqlPolicy {
    /// `CREATE [TEMP] VIEW` and `DROP VIEW`. Views live only as long as the
    /// session and cannot replace a dataset
    pub allow_session_views: bool,
    /// Session statements such as `SET`, `PREPARE` and transactions
    pub allow_session_statements: bool,
    /// Statements that write or register data sources: `COPY ... TO`,
    /// `INSERT`, `CREATE [EXTERNAL] TABLE`, schemas, catalogs and functions
    pub allow_writes: bool,
}

impl SqlPolicy {
    /// Only queries: for API callers, transformations and ingest
    pub const READ_ONLY: Self = Self {
        allow_session_views: false,
        allow_session_statements: false,
        allow_writes: false,
    };

    /// Queries, session views and session statements: for the local user's
    /// interactive SQL shell
    pub const LOCAL_INTERACTIVE: Self = Self {
        allow_session_views: true,
        allow_session_statements: true,
        allow_writes: false,
    };

    /// Attaches this policy to sessions created with the config
    pub fn attach_to(self, config: &mut SessionConfig) {
        config.set_extension(Arc::new(self));
    }

    /// Returns the policy attached to a session, or [`Self::READ_ONLY`]
    pub fn of(config: &SessionConfig) -> Self {
        config
            .get_extension::<Self>()
            .map_or(Self::READ_ONLY, |policy| *policy)
    }

    /// Plans a single SQL statement and executes it under the session's policy
    pub async fn sql(ctx: &SessionContext, sql: &str) -> Result<DataFrame, DataFusionError> {
        let plan = ctx.state().create_logical_plan(sql).await?;
        Self::execute_logical_plan(ctx, plan).await
    }

    /// Executes a plan under the session's policy
    pub async fn execute_logical_plan(
        ctx: &SessionContext,
        plan: LogicalPlan,
    ) -> Result<DataFrame, DataFusionError> {
        let policy = Self::of(ctx.state_ref().read().config());
        ctx.execute_logical_plan(policy.check_plan(plan)?).await
    }

    /// Fails if the plan contains a statement this policy does not allow.
    ///
    /// Every view in a session is session-scoped, so a `CREATE TEMP VIEW`,
    /// which `DataFusion` does not support, is returned as a plain view.
    pub fn check_plan(self, mut plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
        plan.apply_with_subqueries(|node| self.check_node(node))?;

        if self.allow_session_views
            && let LogicalPlan::Ddl(DdlStatement::CreateView(create_view)) = &mut plan
        {
            create_view.temporary = false;
        }

        Ok(plan)
    }

    // Every variant is listed, so that a new kind of plan has to be classified
    // here before it can be executed. Messages follow `DataFusion`'s own
    // `SQLOptions` checks
    fn check_node(self, node: &LogicalPlan) -> Result<TreeNodeRecursion, DataFusionError> {
        let denied = match node {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Window(_)
            | LogicalPlan::Aggregate(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::Join(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Union(_)
            | LogicalPlan::TableScan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Limit(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::Analyze(_)
            | LogicalPlan::Extension(_)
            | LogicalPlan::Distinct(_)
            | LogicalPlan::DescribeTable(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::RecursiveQuery(_) => None,
            LogicalPlan::Ddl(ddl) => {
                let allowed = match ddl {
                    DdlStatement::CreateView(_) | DdlStatement::DropView(_) => {
                        self.allow_session_views
                    }
                    DdlStatement::CreateExternalTable(_)
                    | DdlStatement::CreateMemoryTable(_)
                    | DdlStatement::CreateCatalogSchema(_)
                    | DdlStatement::CreateCatalog(_)
                    | DdlStatement::CreateIndex(_)
                    | DdlStatement::DropTable(_)
                    | DdlStatement::DropCatalogSchema(_)
                    | DdlStatement::CreateFunction(_)
                    | DdlStatement::DropFunction(_) => self.allow_writes,
                };
                (!allowed).then(|| format!("DDL not supported: {}", ddl.name()))
            }
            LogicalPlan::Dml(dml) => {
                (!self.allow_writes).then(|| format!("DML not supported: {}", dml.op))
            }
            LogicalPlan::Copy(_) => {
                (!self.allow_writes).then(|| "DML not supported: COPY".to_string())
            }
            LogicalPlan::Statement(statement) => (!self.allow_session_statements)
                .then(|| format!("Statement not supported: {}", statement.name())),
        };

        match denied {
            None => Ok(TreeNodeRecursion::Continue),
            Some(message) => plan_err!("{message}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
