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

use super::query_handler::SubFormat;
use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailParams {
    #[serde(default)]
    skip: u64,
    #[serde(default = "TailParams::default_limit")]
    limit: u64,
    #[serde(default)]
    format: SubFormat,
    #[serde(default = "TailParams::default_schema")]
    schema: bool,
}

impl TailParams {
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
pub async fn dataset_tail_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Extension(dataset_ref): Extension<DatasetRef>,
    Query(params): Query<TailParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let df = query_svc
        .tail(&dataset_ref, params.skip, params.limit)
        .await
        .int_err()
        .api_err()?;

    super::query_handler::records_to_json(df, params.format, params.schema).await
}

/////////////////////////////////////////////////////////////////////////////////
