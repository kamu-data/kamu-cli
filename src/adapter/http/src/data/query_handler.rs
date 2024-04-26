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

use std::fmt::Debug;

use axum::extract::{Extension, Query};
use axum::response::Json;
use datafusion::dataframe::DataFrame;
use kamu::domain::*;
use kamu_data_utils::data::format::*;

use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

// TODO: Externalize
const MAX_SOA_BUFFER_SIZE: usize = 100_000_000;

/////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SubFormat {
    #[default]
    JsonAos,
    JsonSoa,
    JsonAoa,
}

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryParams {
    query: String,
    #[serde(default)]
    skip: u64,
    #[serde(default = "QueryParams::default_limit")]
    limit: u64,
    #[serde(default)]
    format: SubFormat,
    #[serde(default = "QueryParams::default_schema")]
    schema: bool,
}

impl QueryParams {
    fn default_limit() -> u64 {
        100
    }

    fn default_schema() -> bool {
        true
    }
}

// TODO: This endpoint is temporary to enable some demo integrations
// it should be properly re-designed in future to allow for different query
// dialects, returning schema, error handling etc.
pub async fn dataset_query_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Query(params): Query<QueryParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let df = query_svc
        .sql_statement(&params.query, QueryOptions::default())
        .await
        .int_err()
        .api_err()?
        .limit(
            usize::try_from(params.skip).unwrap(),
            Some(usize::try_from(params.limit).unwrap()),
        )
        .int_err()
        .api_err()?;

    records_to_json(df, params.format, params.schema).await
}

/////////////////////////////////////////////////////////////////////////////////

// TODO: Replace with a serializable type
pub(crate) async fn records_to_json(
    df: DataFrame,
    fmt: SubFormat,
    with_schema: bool,
) -> Result<Json<serde_json::Value>, ApiError> {
    let schema: datafusion::arrow::datatypes::Schema = df.schema().into();
    let record_batches = df.collect().await.int_err().api_err()?;

    // TODO: Streaming?
    let mut buf = Vec::new();
    {
        let mut writer = get_writer(fmt, &mut buf);
        for batch in record_batches {
            writer.write_batch(&batch).int_err().api_err()?;
        }
        writer.finish().int_err().api_err()?;
    }

    let raw_data =
        serde_json::value::RawValue::from_string(String::from_utf8(buf).unwrap()).unwrap();

    Ok(Json(if with_schema {
        serde_json::json!({
            "schema": schema,
            "data": raw_data,
        })
    } else {
        serde_json::json!({
            "data": raw_data,
        })
    }))
}

pub fn get_writer<'a, W: std::io::Write + 'a>(fmt: SubFormat, w: W) -> Box<dyn RecordsWriter + 'a> {
    match fmt {
        SubFormat::JsonAos => Box::new(JsonArrayOfStructsWriter::new(w)),
        SubFormat::JsonSoa => Box::new(JsonStructOfArraysWriter::new(w, MAX_SOA_BUFFER_SIZE)),
        SubFormat::JsonAoa => Box::new(JsonArrayOfArraysWriter::new(w)),
    }
}

/////////////////////////////////////////////////////////////////////////////////
