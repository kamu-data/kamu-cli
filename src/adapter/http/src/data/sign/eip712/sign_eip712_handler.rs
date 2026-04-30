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

use super::sign_eip712_types::{SignEip712QueryParams, SignEip712Request, SignEip712Response};

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
    request_body = SignEip712Request,
    responses(
        (status = OK, body = SignEip712Response),
        (status = BAD_REQUEST, body = ApiErrorResponse),
        (status = UNAUTHORIZED, body = ApiErrorResponse),
        (status = FORBIDDEN, body = ApiErrorResponse),
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
    Json(request): Json<SignEip712Request>,
) -> Result<Json<SignEip712Response>, ApiError> {
    let _ = (catalog, params, request);
    todo!("Authorize managed key access and sign EIP-712 payload")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
