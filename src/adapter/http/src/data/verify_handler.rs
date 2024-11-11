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

use std::error::Error;

use axum::extract::Extension;
use axum::response::Json;
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use kamu_core::QueryError;
use opendatafabric as odf;

use super::{query_handler, query_types as query};
use crate::data::verify_types::{
    DatasetBlockNotFound,
    DatasetNotFound,
    InvalidRequestBadSignature,
    InvalidRequestInputHash,
    InvalidRequestSubQueriesHash,
    OutputMismatch,
    VerifyRequest,
    VerifyResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Verify query commitment
///
/// See [commitments documentation](https://docs.kamu.dev/node/commitments/) for details.
#[utoipa::path(
    post,
    path = "/verify",
    request_body = VerifyRequest,
    responses(
        (status = OK, body = VerifyResponse),
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

    assert_eq!(
        request.proof.r#type,
        query::ProofType::Ed25519Signature2020,
        "UnsupportedProofType",
    );

    let response = verify(catalog, request).await?;
    tracing::debug!(?response, "Verification response");
    Ok(response)
}

async fn verify(catalog: Catalog, request: VerifyRequest) -> Result<VerifyResponse, ApiError> {
    // 1. Validate request
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

    let query_result =
        match query_handler::query_handler_post(axum::Extension(catalog), Json(data_request)).await
        {
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
