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
        skip: Option<u64>,
        limit: Option<u64>,
    ) -> Result<DataQueryResult> {
        // TODO: Default to JsonSoA format once implemented
        let data_format = data_format.unwrap_or(DataBatchFormat::Json);
        let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
        let limit = limit.unwrap_or(Self::DEFAULT_QUERY_LIMIT);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();

        let df = match query_dialect {
            QueryDialect::SqlDataFusion => {
                let sql_result = query_svc
                    .sql_statement(&query, domain::QueryOptions::default())
                    .await;
                match sql_result {
                    Ok(r) => r,
                    Err(e) => return Ok(e.into()),
                }
            }
            _ => unimplemented!(),
        }
        // TODO: Sanity limits
        .limit(
            usize::try_from(skip.unwrap_or(0)).unwrap(),
            Some(usize::try_from(limit).unwrap()),
        )
        .int_err()?;

        let schema = DataSchema::from_data_frame_schema(df.schema(), schema_format)?;
        let record_batches = match df.collect().await {
            Ok(rb) => rb,
            Err(e) => return Ok(e.into()),
        };
        let data = DataBatch::from_records(&record_batches, data_format)?;

        Ok(DataQueryResult::success(Some(schema), data, limit))
    }

    /// Lists engines known to the system and recommended for use
    async fn known_engines(&self, ctx: &Context<'_>) -> Result<Vec<EngineDesc>> {
        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();
        Ok(query_svc
            .get_known_engines()
            .await?
            .into_iter()
            .map(Into::into)
            .collect())
    }
}
