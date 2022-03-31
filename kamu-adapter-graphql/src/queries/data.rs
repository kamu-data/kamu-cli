// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use kamu::domain;

///////////////////////////////////////////////////////////////////////////////

pub struct DataQueries;

#[Object]
impl DataQueries {
    const DEFAULT_QUERY_LIMIT: u64 = 100;

    /// Executes a specified query and returns its result
    async fn query(
        &self,
        ctx: &Context<'_>,
        query: String,
        query_dialect: QueryDialect,
        data_format: Option<DataBatchFormat>,
        schema_format: Option<DataSchemaFormat>,
        limit: Option<u64>,
    ) -> Result<DataQueryResult> {
        // TODO: Default to JsonSoA format once implemented
        let data_format = data_format.unwrap_or(DataBatchFormat::Json);
        let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
        let limit = limit.unwrap_or(Self::DEFAULT_QUERY_LIMIT);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();

        let df = match query_dialect {
            QueryDialect::DataFusion => {
                query_svc
                    .sql_statement(&query, domain::QueryOptions::default())
                    .await?
            }
        }
        .limit(limit as usize)?;

        let record_batches = df.collect().await?;
        let schema = DataSchema::from_data_frame_schema(df.schema(), schema_format)?;
        let data = DataBatch::from_records(&record_batches, data_format)?;

        Ok(DataQueryResult {
            schema,
            data,
            limit,
        })
    }
}
