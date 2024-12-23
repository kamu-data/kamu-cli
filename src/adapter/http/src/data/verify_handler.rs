// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::error::Error;

use axum::extract::Extension;
use axum::response::Json;
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use kamu_core::QueryError;

use super::{query_handler, query_types as query};
use crate::data::verify_types::{
    DatasetBlockNotFound,
    DatasetNotFound,
    InvalidRequestBadSignature,
    InvalidRequestInputHash,
    InvalidRequestSubQueriesHash,
    OutputMismatch,
    ValidationError,
    VerifyRequest,
    VerifyResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Verify query commitment
///
/// A query proof can be stored long-term and then disputed at a later point
/// using this endpoint.
///
/// Example request:
/// ```json
/// {
///     "input": {
///         "query": "select event_time, from, to, close from \"kamu/eth-to-usd\"",
///         "queryDialect": "SqlDataFusion",
///         "dataFormat": "JsonAoA",
///         "include": ["Input", "Proof", "Schema"],
///         "schemaFormat": "ArrowJson",
///         "datasets": [{
///             "id": "did:odf:fed0..26c4",
///             "alias": "kamu/eth-to-usd",
///             "blockHash": "f162..9a1a"
///         }],
///         "skip": 0,
///         "limit": 3
///     },
///     "subQueries": [],
///     "commitment": {
///         "inputHash": "f162..2efc",
///         "outputHash": "f162..b088",
///         "subQueriesHash": "f162..d210"
///     },
///     "proof": {
///         "type": "Ed25519Signature2020",
///         "verificationMethod": "did:key:z6Mk..fwZp",
///         "proofValue": "uJfY..seCg"
///     }
/// }
/// ```
///
/// Example response:
/// ```json
/// {
///     "ok": false,
///     "error": {
///         "kind": "VerificationFailed::OutputMismatch",
///         "actual_hash": "f162..c12a",
///         "expected_hash": "f162..2a2d",
///         "message": "Query was reproduced but resulted in output hash different from expected.
///                     This means that the output was either falsified, or the query
///                     reproducibility was not guaranteed by the system.",
///     }
/// }
/// ```
///
/// See [commitments documentation](https://docs.kamu.dev/node/commitments) for details.
#[utoipa::path(
    post,
    path = "/verify",
    request_body = VerifyRequest,
    responses(
        (
            status = OK, body = VerifyResponse, examples(
                ("Success" = (summary = "Verified successfully", value = json!(
                    VerifyResponse { ok: true, error: None}
                ))),
                ("Invalid commitment" = (value = json!(
                    VerifyResponse {
                        ok: false,
                        error: Some(ValidationError::InvalidRequest(
                            InvalidRequestBadSignature::new("bad signature").into()
                        ))
                    }
                ))),
                ("Verification fail" = (value = json!(
                    VerifyResponse {
                        ok: false,
                        error: Some(
                            ValidationError::VerificationFailed(
                            OutputMismatch::new(
                                odf::Multihash::from_digest_sha3_256(b"a"),
                                odf::Multihash::from_digest_sha3_256(b"b")).into()
                        ))
                    }
                ))),
            )
        ),
        (status = BAD_REQUEST, body = ApiErrorResponse),
    ),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
#[tracing::instrument(level = "info", skip_all)]
#[transactional_handler]
pub async fn verify_handler(
    Extension(catalog): Extension<Catalog>,
    Json(request): Json<VerifyRequest>,
) -> Result<VerifyResponse, ApiError> {
    tracing::debug!(?request, "Verification request");
    let response = verify(catalog, request).await?;
    tracing::debug!(?response, "Verification response");
    Ok(response)
}

async fn verify(catalog: Catalog, request: VerifyRequest) -> Result<VerifyResponse, ApiError> {
    // 1. Validate request
    match request.proof.r#type {
        query::ProofType::Ed25519Signature2020 => {}
    }

    if request.commitment.input_hash
        != odf::Multihash::from_digest_sha3_256(&query::to_canonical_json(&request.input))
    {
        return Ok(VerifyResponse::from(InvalidRequestInputHash::new()));
    }

    if request.commitment.sub_queries_hash
        != odf::Multihash::from_digest_sha3_256(&query::to_canonical_json(&request.sub_queries))
    {
        return Ok(VerifyResponse::from(InvalidRequestSubQueriesHash::new()));
    }

    let did = request.proof.verification_method;
    let signature = request.proof.proof_value;
    if let Err(err) = did.verify(&query::to_canonical_json(&request.commitment), &signature) {
        return Ok(VerifyResponse::from(InvalidRequestBadSignature::new(
            &err.source()
                .map(std::string::ToString::to_string)
                .unwrap_or_default(),
        )));
    }

    // 2. Reproduce the query and compare data hashes
    let mut data_request = request.input.clone();

    // We are only interested in the output
    data_request.include.clear();

    let query_result = match query_handler::query_handler_impl(catalog, data_request).await {
        Ok(Json(v)) => Ok(v),
        Err(err) => match err.source().unwrap().downcast_ref::<QueryError>() {
            Some(QueryError::DatasetNotFound(err)) => {
                return Ok(VerifyResponse::from(DatasetNotFound::new(
                    err.dataset_ref.id().unwrap().clone(),
                )));
            }
            Some(QueryError::DatasetBlockNotFound(err)) => {
                return Ok(VerifyResponse::from(DatasetBlockNotFound::new(
                    err.dataset_id.clone(),
                    err.block_hash.clone(),
                )));
            }
            _ => Err(err),
        },
    }?;

    let output_hash_actual =
        odf::Multihash::from_digest_sha3_256(&query::to_canonical_json(&query_result.output));

    if request.commitment.output_hash != output_hash_actual {
        return Ok(VerifyResponse::from(OutputMismatch::new(
            request.commitment.output_hash,
            output_hash_actual,
        )));
    }

    Ok(VerifyResponse::success())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
