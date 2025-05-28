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
use kamu_core::ResolvedDatasetsMap;
use kamu_core::engine::*;

/// An in-process engine using Apache Arrow Datafusion framework.
///
/// Being in-process, this engine is not properly versioned and ODF-compliant.
/// We use it only for ingest preprocessing queries, as ingestion is
/// fundamentally non-verifiable / non-reproducible.
pub struct EngineDatafusionInproc;

impl EngineDatafusionInproc {
    const OUTPUT_VIEW_ALIAS: &'static str = "__output__";

    pub fn new() -> Self {
        Self {}
    }

    async fn register_view(
        &self,
        ctx: &SessionContext,
        alias: &str,
        query: &str,
    ) -> Result<(), EngineError> {
        use datafusion::logical_expr::*;
        use datafusion::sql::TableReference;

        tracing::debug!(
            %alias,
            %query,
            "Creating view for a query",
        );

        let logical_plan = match ctx.state().create_logical_plan(query).await {
            Ok(plan) => plan,
            Err(error) => {
                tracing::error!(
                    error = &error as &dyn std::error::Error,
                    %query,
                    "Error when setting up query"
                );
                return Err(InvalidQueryError::new(error.to_string(), Vec::new()).into());
            }
        };

        let create_view = LogicalPlan::Ddl(DdlStatement::CreateView(CreateView {
            name: TableReference::bare(alias),
            input: Arc::new(logical_plan),
            or_replace: false,
            definition: Some(query.to_string()),
            temporary: false,
        }));

        ctx.execute_logical_plan(create_view).await.int_err()?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Engine for EngineDatafusionInproc {
    #[tracing::instrument(level = "info", skip_all)]
    async fn execute_raw_query(
        &self,
        request: RawQueryRequestExt,
    ) -> Result<RawQueryResponseExt, EngineError> {
        let odf::metadata::Transform::Sql(transform) = request.transform;
        assert_eq!(transform.engine.to_lowercase(), "datafusion");

        // Setup input
        request
            .ctx
            .register_table("input", request.input_data.into_view())
            .int_err()?;

        // Setup queries
        for query_step in transform.queries.unwrap_or_default() {
            self.register_view(
                &request.ctx,
                query_step
                    .alias
                    .as_deref()
                    .unwrap_or(Self::OUTPUT_VIEW_ALIAS),
                query_step.query.as_str(),
            )
            .await?;
        }

        // Get result's execution plan
        let output_data = request.ctx.table(Self::OUTPUT_VIEW_ALIAS).await.int_err()?;

        tracing::debug!(
            schema = ?output_data.schema(),
            logical_plan = ?output_data.logical_plan(),
            "Prepared raw query plan",
        );

        Ok(RawQueryResponseExt {
            output_data: Some(output_data.into()),
        })
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn execute_transform(
        &self,
        _request: TransformRequestExt,
        _datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformResponseExt, EngineError> {
        unimplemented!(
            "Derivative transformations must be executed by a versioned out-of-process engine"
        )
    }
}
