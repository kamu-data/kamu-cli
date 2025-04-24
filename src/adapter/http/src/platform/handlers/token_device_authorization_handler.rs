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
use http_common::{ApiError, ResultIntoApiError};
use internal_error::ResultIntoInternal;
use kamu_accounts::DeviceClientId;
use serde::{Deserialize, Serialize};

use crate::axum_utils::from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.1>
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct DeviceAuthorizationRequest {
    /// REQUIRED if the client is not authenticating with the
    /// authorization server as described in [Section 3.2.1. of RFC6749](https://datatracker.ietf.org/doc/html/rfc6749#section-3.2.1).
    /// The client identifier as described in [Section 2.2 of RFC6749](https://datatracker.ietf.org/doc/html/rfc6749#section-2.2).
    pub client_id: String,
    /// OPTIONAL.  The scope of the access request as defined by
    /// [Section 3.3 of RFC6749](https://datatracker.ietf.org/doc/html/rfc6749#section-3.3).
    pub scope: Option<String>,
}

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.2>
#[derive(Debug, Serialize, Deserialize, utoipa::ToSchema)]
pub struct DeviceAuthorizationResponse {
    /// REQUIRED.  The device verification code.
    pub device_code: String,
    /// REQUIRED.  The end-user verification code.
    pub user_code: String,
    /// REQUIRED.  The end-user verification URI on the authorization
    /// server.  The URI should be short and easy to remember as end users
    /// will be asked to manually type it into their user agent.
    pub verification_uri: String,
    /// OPTIONAL.  A verification URI that includes the "`user_code`" (or
    /// other information with the same function as the "`user_code`"),
    /// which is designed for non-textual transmission.
    pub verification_uri_complete: Option<String>,
    /// REQUIRED.  The lifetime in seconds of the "`device_code`" and
    /// "`user_code`".
    #[schema(minimum = 1, example = 300)]
    pub expires_in: u64,
    /// OPTIONAL.  The minimum amount of time in seconds that the client
    /// SHOULD wait between polling requests to the token endpoint.  If no
    /// value is provided, clients MUST use 5 as the default.
    #[schema(minimum = 1, example = 5)]
    pub interval: Option<u64>,
}

impl DeviceAuthorizationResponse {
    const DEFAULT_INTERVAL: u64 = 5;

    pub fn interval_or_default(&self) -> u64 {
        self.interval.unwrap_or(Self::DEFAULT_INTERVAL)
    }

    pub fn verification_uri_with_device_code(&self) -> String {
        format!("{}?deviceCode={}", self.verification_uri, self.device_code)
    }
}

/// Authorization of a device
#[utoipa::path(
    post,
    path = "/token/device/authorization",
    request_body(
        content = DeviceAuthorizationRequest,
        content_type = "application/x-www-form-urlencoded"
    ),
    responses(
        (status = OK, body = DeviceAuthorizationResponse),
    ),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[transactional_handler]
pub async fn token_device_authorization_handler(
    Extension(catalog): Extension<Catalog>,
    Form(request): Form<DeviceAuthorizationRequest>,
) -> Result<Json<DeviceAuthorizationResponse>, ApiError> {
    let (oauth_device_code_service, url_config) = from_catalog_n!(
        catalog,
        dyn kamu_accounts::OAuthDeviceCodeService,
        kamu_core::ServerUrlConfig
    );

    let client_id = DeviceClientId::try_new(request.client_id)
        .map_err(|_| ApiError::bad_request_with_message("Invalid client_id"))?;
    let device_code_created = oauth_device_code_service
        .create_device_code(&client_id)
        .await
        .int_err()
        .api_err()?;
    let verification_uri = url_config
        .protocols
        .base_url_platform
        .join("v/login")
        .unwrap()
        .to_string();
    let expires_in = (device_code_created.expires_at - device_code_created.created_at)
        .num_seconds()
        .try_into()
        .unwrap();

    Ok(Json(DeviceAuthorizationResponse {
        device_code: device_code_created.device_code.to_string(),
        // Reserved
        user_code: String::new(),
        verification_uri,
        // Reserved
        verification_uri_complete: None,
        interval: Some(DeviceAuthorizationResponse::DEFAULT_INTERVAL),
        expires_in,
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
