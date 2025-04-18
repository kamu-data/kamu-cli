// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::{Extension, Json};
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::{ApiError, ApiErrorResponse, IntoApiError};
use kamu_accounts::LoginResponse;
use serde::{Deserialize, Serialize};

use crate::axum_utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginRequestBody {
    pub login_method: String,
    pub login_credentials_json: String,
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LoginResponseBody {
    pub access_token: String,
}

/// Authenticate with the node
#[utoipa::path(
    post,
    path = "/login",
    request_body = LoginRequestBody,
    responses(
        (status = OK, body = LoginResponseBody),
        (status = BAD_REQUEST, body = ApiErrorResponse),
        (status = UNAUTHORIZED, body = ApiErrorResponse),
    ),
    tag = "kamu",
    security(())
)]
#[transactional_handler]
pub async fn login_handler(
    Extension(catalog): Extension<Catalog>,
    Json(payload): Json<LoginRequestBody>,
) -> Result<Json<LoginResponseBody>, ApiError> {
    let authentication_service = from_catalog_n!(catalog, dyn kamu_accounts::AuthenticationService);

    authentication_service
        .login(
            payload.login_method.as_str(),
            payload.login_credentials_json,
            None,
        )
        .await
        .map(|LoginResponse { access_token, .. }| Json(LoginResponseBody { access_token }))
        .map_err(|e| {
            use kamu_accounts::LoginError as E;
            match e {
                E::UnsupportedMethod(e) => ApiError::bad_request(e),
                E::InvalidCredentials(e) => ApiError::new_unauthorized_from(e),
                E::RejectedCredentials(e) => ApiError::new_unauthorized_from(e),
                E::NoPrimaryEmail(e) => ApiError::new_unauthorized_from(e),
                E::DuplicateCredentials => ApiError::bad_request(e),
                E::Internal(e) => e.api_err(),
            }
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
