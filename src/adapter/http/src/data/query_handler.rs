// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use axum::extract::{Extension, Query};
use axum::response::Json;
use datafusion::arrow::array::RecordBatch;
use datafusion::dataframe::DataFrame;
use kamu::domain::*;
use kamu_data_utils::data::format::*;

use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    query: String,
    skip: Option<u64>,
    limit: Option<u64>,
}

impl QueryParams {
    const DEFAULT_LIMIT: u64 = 100;
}

// TODO: This endpoint is temporary to enable some demo integrations
// it should be properly re-designed in future to allow for different query
// dialects, returning schema, error handling etc.
pub async fn dataset_query_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Query(params): Query<QueryParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // TODO: Default to JsonSoA format once implemented
    // let data_format = data_format.unwrap_or(DataBatchFormat::Json);
    // let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
    let skip = params.skip.unwrap_or(0);
    let limit = params.limit.unwrap_or(QueryParams::DEFAULT_LIMIT);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let df = query_svc
        .sql_statement(&params.query, QueryOptions::default())
        .await
        .int_err()
        .api_err()?
        .limit(
            usize::try_from(skip).unwrap(),
            Some(usize::try_from(limit).unwrap()),
        )
        .int_err()
        .api_err()?;

    records_to_json(df).await
}

/////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with a serializable type
pub(crate) async fn records_to_json(df: DataFrame) -> Result<Json<serde_json::Value>, ApiError> {
    let record_batches = df.collect().await.int_err().api_err()?;

    let num_records: usize = record_batches.iter().map(RecordBatch::num_rows).sum();

    let mut buf = Vec::new();
    if num_records != 0 {
        let mut writer = JsonArrayWriter::new(&mut buf);
        for batch in record_batches {
            writer.write_batch(&batch).int_err().api_err()?;
        }
        writer.finish().int_err().api_err()?;
    } else {
        // HACK: JsonArrayWriter should be producing [] when there are no rows
        write!(buf, "[]").int_err().api_err()?;
    }

    // TODO: Avoid double serialization
    let array: serde_json::Value = serde_json::from_slice(&buf).int_err().api_err()?;

    Ok(Json(serde_json::json!({
        "data": array,
    })))
}

/////////////////////////////////////////////////////////////////////////////////
