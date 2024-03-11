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

use crate::api_error::*;

/////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TailParams {
    skip: Option<u64>,
    limit: Option<u64>,
}

impl TailParams {
    const DEFAULT_LIMIT: u64 = 100;
}

// TODO: This endpoint is temporary to enable some demo integrations
// it should be properly re-designed in future to allow for different query
// dialects, returning schema, error handling etc.
pub async fn dataset_tail_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Extension(dataset_ref): Extension<DatasetRef>,
    Query(params): Query<TailParams>,
) -> Result<Json<serde_json::Value>, ApiError> {
    // TODO: Default to JsonSoA format once implemented
    // let data_format = data_format.unwrap_or(DataBatchFormat::Json);
    // let schema_format = schema_format.unwrap_or(DataSchemaFormat::Parquet);
    let skip = params.skip.unwrap_or(0);
    let limit = params.limit.unwrap_or(TailParams::DEFAULT_LIMIT);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let df = query_svc
        .tail(&dataset_ref, skip, limit)
        .await
        .int_err()
        .api_err()?;

    super::query_handler::records_to_json(df).await
}

/////////////////////////////////////////////////////////////////////////////////
