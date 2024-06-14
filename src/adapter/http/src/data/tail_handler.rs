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

use axum::extract::{Extension, Query};
use axum::response::Json;
use kamu::domain::*;
use opendatafabric::DatasetRef;

use super::query_handler::{DataFormat, SchemaFormat};
use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

// TODO: This endpoint is temporary to enable some demo integrations
// it should be properly re-designed in future to allow for different query
// dialects, returning schema, error handling etc.
pub async fn dataset_tail_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Extension(dataset_ref): Extension<DatasetRef>,
    Query(params): Query<TailRequestParams>,
) -> Result<Json<TailResponseBody>, ApiError> {
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let df = query_svc
        .tail(&dataset_ref, params.skip, params.limit)
        .await
        .int_err()
        .api_err()?;

    let schema = if params.include_schema {
        Some(super::query_handler::serialize_schema(df.schema(), params.schema_format).api_err()?)
    } else {
        None
    };

    let record_batches = df.collect().await.int_err().api_err()?;
    let json =
        super::query_handler::serialize_data(&record_batches, params.data_format).api_err()?;
    let data = serde_json::value::RawValue::from_string(json).unwrap();

    Ok(Json(TailResponseBody { data, schema }))
}

/////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailRequestParams {
    #[serde(default)]
    skip: u64,
    #[serde(default = "TailRequestParams::default_limit")]
    limit: u64,
    #[serde(alias = "format")]
    #[serde(default)]
    data_format: DataFormat,
    #[serde(default)]
    schema_format: SchemaFormat,
    #[serde(alias = "schema")]
    #[serde(default = "TailRequestParams::default_include_schema")]
    include_schema: bool,
}

impl TailRequestParams {
    fn default_limit() -> u64 {
        100
    }

    fn default_include_schema() -> bool {
        true
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TailResponseBody {
    data: Box<serde_json::value::RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
}

/////////////////////////////////////////////////////////////////////////////////
