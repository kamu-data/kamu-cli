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

use super::query_types::{QueryResponse, *};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Execute a batch query
#[utoipa::path(
    post,
    path = "/query",
    request_body = QueryRequest,
    responses((status = OK, body = QueryResponse)),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn query_handler_post(
    Extension(catalog): Extension<Catalog>,
    Json(mut body): Json<QueryRequest>,
) -> Result<Json<QueryResponse>, ApiError> {
    tracing::debug!(request = ?body, "Query");

    // Automatically add `Input` if proof is requested, as proof depends on input
    // for verifiability
    if body.include.contains(&Include::Proof) {
        body.include.insert(Include::Input);
    }
    // Automatically add `Schema` if user specified schema format
    if body.schema_format.is_some() {
        body.include.insert(Include::Schema);
    }

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

    let (schema, schema_format) = if body.include.contains(&Include::Schema) {
        let schema_format = body.schema_format.unwrap_or_default();
        (
            Some(Schema::new(df.schema().inner().clone(), schema_format)),
            Some(schema_format),
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

    let include_proof = body.include.contains(&Include::Proof);

    let input =
        if !body.include.contains(&Include::Input) && !body.include.contains(&Include::Proof) {
            None
        } else {
            body.datasets = Some(QueryRequest::query_state_to_datasets(res.state));
            body.schema_format = schema_format;
            Some(body)
        };

    let response = if !include_proof {
        QueryResponse {
            input,
            output,
            sub_queries: None,
            commitment: None,
            proof: None,
        }
    } else if let Some(identity) = identity {
        use ed25519_dalek::Signer;

        let sub_queries = Vec::new();

        // TODO: PERF: There is a large avenue for improvements to avoid
        // re-serialization. We could potentially always serialize signed
        // responses in canonical JSON format to avoid transcoding.
        let commitment = Commitment {
            input_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(&input)),
            output_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(&output)),
            sub_queries_hash: odf::Multihash::from_digest_sha3_256(&to_canonical_json(
                &sub_queries,
            )),
        };

        let signature = identity.private_key.sign(&to_canonical_json(&commitment));

        QueryResponse {
            input,
            output,
            sub_queries: Some(sub_queries),
            commitment: Some(commitment),
            proof: Some(Proof {
                r#type: ProofType::Ed25519Signature2020,
                verification_method: identity.did(),
                proof_value: signature.into(),
            }),
        }
    } else {
        Err(ApiError::not_implemented(ResponseSigningNotConfigured))?
    };

    Ok(Json(response))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Execute a batch query
#[utoipa::path(
    get,
    path = "/query",
    params(QueryParams),
    responses((status = OK, body = QueryResponse)),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn query_handler(
    catalog: Extension<Catalog>,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, ApiError> {
    query_handler_post(catalog, Json(params.into())).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
#[error("Response signing is not enabled by the node operator")]
struct ResponseSigningNotConfigured;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
