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
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;

use super::query_types::{DataFormat, Schema, SchemaFormat};
use crate::DatasetAliasInPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get a sample of latest events
#[utoipa::path(
    get,
    path = "/tail",
    params(DatasetTailParams, DatasetAliasInPath),
    responses((status = OK, body = DatasetTailResponse)),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
#[tracing::instrument(level = "info", skip_all, fields(%dataset_ref))]
pub async fn dataset_tail_handler(
    Extension(catalog): Extension<Catalog>,
    Extension(dataset_ref): Extension<odf::DatasetRef>,
    Query(params): Query<DatasetTailParams>,
) -> Result<Json<DatasetTailResponse>, ApiError> {
    tracing::debug!(request = ?params, "Tail");

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let df = query_svc
        .tail(&dataset_ref, params.skip, params.limit)
        .await
        .map_err(|e| match e {
            QueryError::DatasetNotFound(e) => ApiError::not_found(e),
            QueryError::DatasetBlockNotFound(_) | QueryError::BadQuery(_) => e.int_err().api_err(),
            QueryError::DatasetSchemaNotAvailable(e) => ApiError::no_content(e),
            QueryError::Access(_) => ApiError::not_found_without_reason(),
            QueryError::Internal(e) => e.api_err(),
        })?;

    let schema = params
        .schema_format
        .map(|fmt| Schema::new(df.schema().inner().clone(), fmt));

    let record_batches = df.collect().await.int_err().api_err()?;
    let json = super::query_types::serialize_data(&record_batches, params.data_format).api_err()?;
    let data = serde_json::value::RawValue::from_string(json).unwrap();

    Ok(Json(DatasetTailResponse {
        data,
        data_format: params.data_format,
        schema,
        schema_format: params.schema_format,
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Sanity limits
#[derive(Debug, serde::Deserialize, utoipa::IntoParams)]
#[serde(rename_all = "camelCase")]
#[into_params(parameter_in = Query)]
pub struct DatasetTailParams {
    /// Number of leading records to skip when returning result (used for
    /// pagination)
    #[serde(default)]
    pub skip: u64,

    /// Maximum number of records to return (used for pagination)
    #[serde(default = "DatasetTailParams::default_limit")]
    pub limit: u64,

    /// How the output data should be encoded
    #[serde(default)]
    pub data_format: DataFormat,

    /// How to encode the schema of the result
    #[param(value_type = SchemaFormat)]
    pub schema_format: Option<SchemaFormat>,
}

impl DatasetTailParams {
    fn default_limit() -> u64 {
        100
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct DatasetTailResponse {
    /// Resulting data
    #[schema(value_type = Object)]
    pub data: Box<serde_json::value::RawValue>,

    /// How data is laid out in the response
    pub data_format: DataFormat,

    /// Schema of the resulting data
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Schema)]
    pub schema: Option<Schema>,

    /// What representation is used for the schema
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = SchemaFormat)]
    pub schema_format: Option<SchemaFormat>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
