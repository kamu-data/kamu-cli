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
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use internal_error::*;
use kamu_core::*;
use opendatafabric as odf;

use super::query_types::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn dataset_query_handler_post(
    Extension(catalog): Extension<Catalog>,
    Json(body): Json<RequestBody>,
) -> Result<Json<ResponseBody>, ApiError> {
    match body {
        RequestBody::V1(body) => dataset_query_handler_post_v1(catalog, body).await,
        RequestBody::V2(body) => dataset_query_handler_post_v2(catalog, body).await,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn dataset_query_handler_post_v2(
    catalog: Catalog,
    mut body: RequestBodyV2,
) -> Result<Json<ResponseBody>, ApiError> {
    let identity = catalog.get_one::<IdentityConfig>().ok();
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let res = query_svc
        .sql_statement(&body.query, body.to_options())
        .await
        .map_err(map_query_error)?;

    // Apply pagination limits
    let df = res
        .df
        .limit(
            usize::try_from(body.skip).unwrap(),
            Some(usize::try_from(body.limit).unwrap()),
        )
        .int_err()
        .api_err()?;

    let (schema, schema_format) = if body.include_schema {
        (
            Some(serialize_schema(df.schema(), body.schema_format).api_err()?),
            Some(body.schema_format),
        )
    } else {
        (None, None)
    };

    let record_batches = df.collect().await.int_err().api_err()?;
    let json = serialize_data(&record_batches, body.data_format).api_err()?;

    // TODO: PERF: Avoid re-serializing data
    let data = serde_json::from_str(&json).unwrap();

    let output = Outputs {
        data,
        data_format: body.data_format,
        schema,
        schema_format,
    };

    let sign = body.sign;
    let input = if !body.include_input && !body.sign {
        None
    } else {
        body.datasets = Some(RequestBodyV2::query_state_to_datasets(res.state));
        Some(body)
    };

    let response = if !sign {
        ResponseBody::V2(ResponseBodyV2 { input, output })
    } else if let Some(identity) = identity {
        use ed25519_dalek::Signer;

        let sub_queries = Vec::new();

        // TODO: PERF: There is a large avenue for improvements to avoid
        // re-serialization. We could potentially always serialize signed
        // responses in canonical JSON format to avoid trascoding.
        let commitment = Commitment {
            input_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(&input)),
            output_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(&output)),
            sub_queries_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(
                &sub_queries,
            )),
        };

        let signature = identity.private_key.sign(&to_canonical_json(&commitment));

        ResponseBody::V2Signed(ResponseBodyV2Signed {
            input: input.unwrap(),
            output,
            sub_queries,
            commitment,
            proof: Proof {
                r#type: ProofType::Ed25519Signature2020,
                verification_method: identity.did(),
                proof_value: signature.into(),
            },
        })
    } else {
        Err(ApiError::not_implemented(ResponseSigningNotCongigured))?
    };

    Ok(Json(response))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn dataset_query_handler_post_v1(
    catalog: Catalog,
    body: RequestBodyV1,
) -> Result<Json<ResponseBody>, ApiError> {
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    let res = query_svc
        .sql_statement(&body.query, body.to_options())
        .await
        .map_err(map_query_error)?;

    // Apply pagination limits
    let df = res
        .df
        .limit(
            usize::try_from(body.skip).unwrap(),
            Some(usize::try_from(body.limit).unwrap()),
        )
        .int_err()
        .api_err()?;

    let arrow_schema = df.schema().inner().clone();

    let schema = if body.include_schema {
        Some(serialize_schema(df.schema(), body.schema_format).api_err()?)
    } else {
        None
    };

    let state = if body.include_state {
        Some(res.state.into())
    } else {
        None
    };

    let record_batches = df.collect().await.int_err().api_err()?;
    let json = serialize_data(&record_batches, body.data_format).api_err()?;
    let data = serde_json::value::RawValue::from_string(json).unwrap();

    let data_hash = if body.include_data_hash {
        Some(kamu_data_utils::data::hash::get_batches_logical_hash(
            &arrow_schema,
            &record_batches,
        ))
    } else {
        None
    };

    Ok(Json(ResponseBody::V1(ResponseBodyV1 {
        data,
        schema,
        state,
        data_hash,
    })))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_query_handler(
    catalog: Extension<Catalog>,
    Query(params): Query<RequestParams>,
) -> Result<Json<ResponseBody>, ApiError> {
    dataset_query_handler_post(catalog, Json(params.into())).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn to_canonical_json<T: serde::Serialize>(val: &T) -> Vec<u8> {
    // TODO: PERF: Avoid double-serialization
    // Because `canonical_json` needs `serde_json::Value` to be able to sort keys
    // before serializing into a buffer we end up paying double for allocations.
    // A better approach would be to use a macro to enforce alphabetic order of
    // struct fields to avoid the need of sorting and then use a canonical formatter
    // to write directly to a buffer.
    let json = serde_json::to_value(val).unwrap();
    let str = canonical_json::to_string(&json).unwrap();
    str.into_bytes()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_query_error(error: QueryError) -> ApiError {
    match error {
        QueryError::DatasetNotFound(err) => ApiError::not_found(err),
        QueryError::DataFusionError(DataFusionError {
            source: datafusion::error::DataFusionError::SQL(err, _),
            ..
        }) => ApiError::bad_request(err),
        QueryError::DataFusionError(DataFusionError {
            source: err @ datafusion::error::DataFusionError::Plan(_),
            ..
        }) => ApiError::bad_request(err),
        _ => error.int_err().api_err(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("Response signing is not enabled by the node operator")]
struct ResponseSigningNotCongigured;
