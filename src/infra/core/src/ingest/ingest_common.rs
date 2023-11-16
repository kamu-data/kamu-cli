// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::prelude::*;
use internal_error::*;
use kamu_core::engine::*;
use kamu_core::ObjectStoreRegistry;
use opendatafabric::*;

///////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "info", skip_all)]
pub(crate) async fn preprocess(
    ctx: &SessionContext,
    transform: Transform,
    df: DataFrame,
) -> Result<DataFrame, EngineError> {
    let Transform::Sql(transform) = transform;

    // TODO: Support other engines
    assert_eq!(transform.engine.to_lowercase(), "datafusion");

    let transform = transform.normalize_queries(Some("output".to_string()));

    // Setup input
    ctx.register_table("input", df.into_view()).int_err()?;

    // Setup queries
    for query_step in transform.queries.unwrap_or_default() {
        register_view_for_step(ctx, &query_step).await?;
    }

    // Get result's execution plan
    let df = ctx.table("output").await.int_err()?;

    tracing::debug!(
        schema = ?df.schema(),
        logical_plan = ?df.logical_plan(),
        "Performing preprocess step",
    );

    Ok(df)
}

///////////////////////////////////////////////////////////////////////////////

async fn register_view_for_step(
    ctx: &SessionContext,
    step: &SqlQueryStep,
) -> Result<(), EngineError> {
    use datafusion::logical_expr::*;
    use datafusion::sql::TableReference;

    let name = step.alias.as_ref().unwrap();

    tracing::debug!(
        %name,
        query = %step.query,
        "Creating view for a query",
    );

    let logical_plan = match ctx.state().create_logical_plan(&step.query).await {
        Ok(plan) => plan,
        Err(error) => {
            tracing::error!(
                error = &error as &dyn std::error::Error,
                query = %step.query,
                "Error when setting up query"
            );
            return Err(InvalidQueryError::new(error.to_string(), Vec::new()).into());
        }
    };

    let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
        name: TableReference::bare(step.alias.as_ref().unwrap()).to_owned_reference(),
        input: Arc::new(logical_plan),
        or_replace: false,
        definition: Some(step.query.clone()),
    }));

    ctx.execute_logical_plan(create_view).await.int_err()?;
    Ok(())
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn new_session_context(
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
) -> SessionContext {
    use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
    use datafusion::prelude::*;

    let config = SessionConfig::new().with_default_catalog_and_schema("kamu", "kamu");

    let runtime_config = RuntimeConfig {
        object_store_registry: object_store_registry.as_datafusion_registry(),
        ..RuntimeConfig::default()
    };

    let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());

    SessionContext::new_with_config_rt(config, runtime)
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn next_operation_id() -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut name = String::with_capacity(16);
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from),
    );

    name
}

///////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_random_cache_key(prefix: &str) -> String {
    use rand::distributions::Alphanumeric;
    use rand::Rng;

    let mut name = String::with_capacity(10 + prefix.len());
    name.push_str(prefix);
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from),
    );
    name
}
