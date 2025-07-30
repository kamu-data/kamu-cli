// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::convert::TryFrom;

use kamu_core as domain;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DataQueries;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl DataQueries {
    const DEFAULT_QUERY_LIMIT: u64 = 100;

    /// Executes a specified query and returns its result
    #[tracing::instrument(level = "info", name = DataQueries_query, skip_all)]
    async fn query(
        &self,
        ctx: &Context<'_>,
        query: String,
        query_dialect: QueryDialect,
        data_format: Option<DataBatchFormat>,
        schema_format: Option<DataSchemaFormat>,
        skip: Option<u64>,
        limit: Option<u64>,
    ) -> Result<DataQueryResult> {
        tracing::debug!(
            %query,
            ?query_dialect,
            ?data_format,
            ?schema_format,
            ?skip,
            ?limit,
            "Query",
        );

        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);

        // TODO: Default to JsonSoA format once implemented
        let data_format = data_format.unwrap_or(DataBatchFormat::Json);
        let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
        let limit = limit.unwrap_or(Self::DEFAULT_QUERY_LIMIT);

        let query_result = match query_dialect {
            QueryDialect::SqlDataFusion => {
                match query_svc
                    .sql_statement(&query, domain::QueryOptions::default())
                    .await
                {
                    Ok(r) => r,
                    Err(err) => return DataQueryResult::from_query_error(err),
                }
            }
            _ => {
                return Ok(DataQueryResult::invalid_sql(format!(
                    "Dialect {query_dialect:?} is not yet supported"
                )));
            }
        };

        let df = query_result
            .df
            // TODO: Sanity limits
            .limit(
                usize::try_from(skip.unwrap_or(0)).unwrap(),
                Some(usize::try_from(limit).unwrap()),
            )
            .int_err()?;

        let schema = DataSchema::from_data_frame_schema(df.schema(), schema_format)?;

        let record_batches = match df.collect().await {
            Ok(rb) => rb,
            Err(err) => return DataQueryResult::from_query_error(err.into()),
        };

        let data = DataBatch::from_records(&record_batches, data_format)?;
        let datasets = DatasetState::from_query_state(query_result.state);

        Ok(DataQueryResult::success(schema, data, datasets, limit))
    }

    /// Lists engines known to the system and recommended for use
    #[tracing::instrument(level = "info", name = DataQueries_known_engines, skip_all)]
    async fn known_engines(&self, ctx: &Context<'_>) -> Result<Vec<EngineDesc>> {
        let query_svc = from_catalog_n!(ctx, dyn domain::QueryService);
        Ok(query_svc
            .get_known_engines()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
