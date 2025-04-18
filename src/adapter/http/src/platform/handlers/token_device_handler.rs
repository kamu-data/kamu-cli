// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::{Extension, Form, Json};
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::{ApiError, IntoApiError, ResultIntoApiError};
use kamu_accounts::{DeviceCode, DeviceToken, OAUTH_DEVICE_ACCESS_TOKEN_GRANT_TYPE};
use serde::{Deserialize, Serialize};

use crate::axum_utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.4>
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct DeviceAccessTokenRequest {
    /// REQUIRED.  Value MUST be set to
    /// "urn:ietf:params:oauth:grant-type:device_code".
    pub grant_type: String,
    /// REQUIRED.  The device verification code, "`device_code`" from the
    /// device authorization response, defined in [Section 3.2](https://datatracker.ietf.org/doc/html/rfc8628#section-3.2).
    pub client_id: String,
    /// REQUIRED if the client is not authenticating with the
    /// authorization server as described in [Section 3.2.1. of RFC6749](https://datatracker.ietf.org/doc/html/rfc6749#section-3.2.1).
    /// The client identifier as described in [Section 2.2 of RFC6749](https://datatracker.ietf.org/doc/html/rfc6749#section-2.2).
    pub device_code: String,
}

/// <https://datatracker.ietf.org/doc/html/rfc6749#section-5.1>
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DeviceAccessTokenResponse {
    /// REQUIRED.  The access token issued by the authorization server.
    pub access_token: String,
    /// REQUIRED.  The type of the token issued as described in
    /// [Section 7.1](https://datatracker.ietf.org/doc/html/rfc6749#section-7.1).  Value is case insensitive.
    pub token_type: String,
    /// RECOMMENDED.  The lifetime in seconds of the access token.  For
    /// example, the value "3600" denotes that the access token will
    /// expire in one hour from the time the response was generated.
    /// If omitted, the authorization server SHOULD provide the
    /// expiration time via other means or document the default value.
    #[schema(minimum = 1, example = 3600)]
    pub expires_in: usize,
    /// OPTIONAL.  The refresh token, which can be used to obtain new
    /// access tokens using the same authorization grant as described
    /// in [Section 6](https://datatracker.ietf.org/doc/html/rfc6749#section-6).
    pub refresh_token: Option<String>,
    /// OPTIONAL.  The scope of the access request as described by
    /// [Section 3.3](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3).  The requested scope MUST NOT include any scope
    /// not originally granted by the resource owner, and if omitted is
    /// treated as equal to the scope originally granted by the
    /// resource owner.
    pub score: Option<String>,
}

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.5>
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum DeviceAccessTokenErrorStatus {
    /*
     * Main errors:
     * OAuth 2.0 Device Authorization Grant
     * https://datatracker.ietf.org/doc/html/rfc8628#section-3.5
     */
    /// The authorization request is still pending as the end user hasn't
    /// yet completed the user-interaction steps (Section 3.3).  The
    /// client SHOULD repeat the access token request to the token
    /// endpoint (a process known as polling).  Before each new request,
    /// the client MUST wait at least the number of seconds specified by
    /// the "interval" parameter of the device authorization response (see
    /// Section 3.2), or 5 seconds if none was provided, and respect any
    /// increase in the polling interval required by the "`slow_down`"
    /// error.
    AuthorizationPending,
    /// A variant of "`authorization_pending`", the authorization request is
    /// still pending and polling should continue, but the interval MUST
    /// be increased by 5 seconds for this and all subsequent requests.
    SlowDown,
    /// The authorization request was denied.
    AccessDenied,
    /// The "`device_code`" has expired, and the device authorization
    /// session has concluded.  The client MAY commence a new device
    /// authorization request but SHOULD wait for user interaction before
    /// restarting to avoid unnecessary polling.
    ExpiredToken,

    /*
     * Common errors:
     * The OAuth 2.0 Authorization Framework
     * https://datatracker.ietf.org/doc/html/rfc6749#section-5.2
     */
    /// The provided authorization grant (e.g., authorization
    /// code, resource owner credentials) or refresh token is
    /// invalid, expired, revoked, does not match the redirection
    /// URI used in the authorization request, or was issued to
    /// another client.
    InvalidGrant,
}

#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DeviceAccessTokenError {
    pub message: DeviceAccessTokenErrorStatus,
}

impl DeviceAccessTokenError {
    pub fn new(status: DeviceAccessTokenErrorStatus) -> Self {
        Self { message: status }
    }
}

impl IntoApiError for DeviceAccessTokenError {
    fn api_err(self) -> ApiError {
        ApiError::bad_request_with_message(self.message.to_string().as_str())
    }
}

/// Polling to obtain a token for the device
#[utoipa::path(
    post,
    path = "/token/device",
    request_body(
        content = DeviceAccessTokenRequest,
        content_type = "application/x-www-form-urlencoded"
    ),
    responses(
        (status = OK, body = DeviceAccessTokenResponse),
        (status = BAD_REQUEST, body = DeviceAccessTokenError)
    ),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[transactional_handler]
pub async fn token_device_handler(
    Extension(catalog): Extension<Catalog>,
    Form(request): Form<DeviceAccessTokenRequest>,
) -> Result<Json<DeviceAccessTokenResponse>, ApiError> {
    if request.grant_type != OAUTH_DEVICE_ACCESS_TOKEN_GRANT_TYPE {
        return Err(
            DeviceAccessTokenError::new(DeviceAccessTokenErrorStatus::InvalidGrant).api_err(),
        );
    }

    let (oauth_device_code_service, jwt_token_issuer) = from_catalog_n!(
        catalog,
        dyn kamu_accounts::OAuthDeviceCodeService,
        dyn kamu_accounts::JwtTokenIssuer
    );

    let device_code = DeviceCode::try_new(&request.device_code)
        .map_err(|_| ApiError::bad_request_with_message("Invalid device_code"))?;

    use kamu_accounts::DeviceCodeServiceExt;

    let maybe_device_token = oauth_device_code_service
        .try_find_device_token_by_device_code(&device_code)
        .await
        .api_err()?;

    let Some(device_token) = maybe_device_token else {
        // Token not found, deny access
        return Err(
            DeviceAccessTokenError::new(DeviceAccessTokenErrorStatus::AccessDenied).api_err(),
        );
    };

    match device_token {
        DeviceToken::DeviceCodeCreated(..) => Err(DeviceAccessTokenError::new(
            DeviceAccessTokenErrorStatus::AuthorizationPending,
        )
        .api_err()),
        DeviceToken::DeviceCodeWithIssuedToken(d) => {
            let expires_in = d.token_params_part.expires_in();
            let access_token = jwt_token_issuer
                .make_access_token_from_device_token_params_part(d.token_params_part)
                .api_err()?;

            Ok(Json(DeviceAccessTokenResponse {
                access_token: access_token.into_inner(),
                token_type: "Bearer".to_string(),
                expires_in,
                // Reserved
                refresh_token: None,
                // Reserved
                score: None,
            }))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
