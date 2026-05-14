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
use http_common::{ApiError, ApiErrorResponse};
use kamu_signing::use_cases::SignEip712Response;

use super::sign_eip712_types::{Eip712TypedDataSchema, SignEip712QueryParams};
use crate::data::query_handler::ResponseSigningNotConfigured;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sign EIP-712 typed data
///
/// This endpoint authorizes that the caller has signing permissions for a
/// specified managed key and signs the message with it.
///
/// The optional `includeNodeProof` flag is used to additionally sign the
/// resulting signature with the node's own secp256k1 key, allowing for on-chain
/// verification.
#[utoipa::path(
    post,
    path = "/sign/eip712",
    params(SignEip712QueryParams),
    request_body = Eip712TypedDataSchema,
    responses(
        (status = OK, body = SignEip712Response),
        (status = NOT_FOUND, body = ApiErrorResponse),
    ),
    tag = "odf-sign",
    security(
        (),
        ("api_key" = [])
    )
)]
#[transactional_handler]
pub async fn sign_eip712_handler(
    Extension(catalog): Extension<dill::Catalog>,
    Query(params): Query<SignEip712QueryParams>,
    Json(request): Json<crypto_eip712_utils::Eip712TypedData>,
) -> Result<Json<SignEip712Response>, ApiError> {
    use kamu_signing::use_cases::*;

    tracing::debug!(params = ?params, request = ?request, "EIP-712 sign request");

    let use_case = catalog.get_one::<dyn SignEip712UseCase>().unwrap();

    use_case
        .execute(
            params.key,
            request,
            SignEip712UseCaseOptions::builder()
                .include_node_proof(params.include_node_proof)
                .build(),
        )
        .await
        .map(Json)
        .map_err(|e| match e {
            SignEip712UseCaseError::NotConfigured => {
                ApiError::not_implemented(ResponseSigningNotConfigured)
            }
            SignEip712UseCaseError::SecretKeyNotFound { .. } => {
                ApiError::not_found_without_reason()
            }
            SignEip712UseCaseError::Internal(e) => e.into(),
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
