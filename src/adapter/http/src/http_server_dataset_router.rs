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
use http_common::{ApiError, ApiErrorResponse, IntoApiError, ResultIntoApiError};
use internal_error::ResultIntoInternal;
use kamu_accounts::{DeviceClientId, DeviceCode};
use kamu_core::TenancyConfig;
use serde::{Deserialize, Serialize};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use crate::axum_utils::ensure_authenticated_account;
use crate::simple_protocol::*;
use crate::DatasetResolverLayer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extractor of dataset identity for single-tenant smart transfer protocol
#[derive(serde::Deserialize)]
struct DatasetByName {
    dataset_name: odf::DatasetName,
}

/// Extractor of account + dataset identity for multi-tenant smart transfer
/// protocol
#[derive(serde::Deserialize)]
struct DatasetByAccountAndName {
    account_name: odf::AccountName,
    dataset_name: odf::DatasetName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn smart_transfer_protocol_router() -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(dataset_refs_handler))
        .routes(routes!(dataset_blocks_handler))
        .routes(routes!(dataset_data_get_handler, dataset_data_put_handler))
        .routes(routes!(
            dataset_checkpoints_get_handler,
            dataset_checkpoints_put_handler
        ))
        .routes(routes!(dataset_pull_ws_upgrade_handler))
        .routes(routes!(dataset_push_ws_upgrade_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn add_dataset_resolver_layer(
    dataset_router: OpenApiRouter,
    tenancy_config: TenancyConfig,
) -> OpenApiRouter {
    use axum::extract::Path;

    match tenancy_config {
        TenancyConfig::MultiTenant => dataset_router.layer(DatasetResolverLayer::new(
            |Path(p): Path<DatasetByAccountAndName>| {
                odf::DatasetAlias::new(Some(p.account_name), p.dataset_name).into_local_ref()
            },
            is_dataset_optional_for_request,
        )),
        TenancyConfig::SingleTenant => dataset_router.layer(DatasetResolverLayer::new(
            |Path(p): Path<DatasetByName>| p.dataset_name.as_local_ref(),
            is_dataset_optional_for_request,
        )),
    }
}

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
    path = "/platform/login",
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
pub async fn platform_login_handler(
    Extension(catalog): Extension<Catalog>,
    Json(payload): Json<LoginRequestBody>,
) -> Result<Json<LoginResponseBody>, ApiError> {
    let authentication_service = catalog
        .get_one::<dyn kamu_accounts::AuthenticationService>()
        .unwrap();

    let login_result = authentication_service
        .login(
            payload.login_method.as_str(),
            payload.login_credentials_json,
        )
        .await;

    match login_result {
        Ok(login_response) => {
            let response_body = LoginResponseBody {
                access_token: login_response.access_token,
            };
            Ok(Json(response_body))
        }
        Err(e) => Err(match e {
            kamu_accounts::LoginError::UnsupportedMethod(e) => ApiError::bad_request(e),
            kamu_accounts::LoginError::InvalidCredentials(e) => ApiError::new_unauthorized_from(e),
            kamu_accounts::LoginError::RejectedCredentials(e) => ApiError::new_unauthorized_from(e),
            kamu_accounts::LoginError::NoPrimaryEmail(e) => ApiError::new_unauthorized_from(e),
            kamu_accounts::LoginError::DuplicateCredentials => ApiError::bad_request(e),
            kamu_accounts::LoginError::Internal(e) => e.api_err(),
        }),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Validate auth token
#[utoipa::path(
    get,
    path = "/platform/token/validate",
    responses(
        (status = OK, body = ()),
        (status = UNAUTHORIZED, body = ApiErrorResponse)
    ),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[expect(clippy::unused_async)]
pub async fn platform_token_validate_handler(catalog: Extension<Catalog>) -> Result<(), ApiError> {
    ensure_authenticated_account(&catalog).api_err()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.1>
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct StartDeviceFlowRequest {
    /// Reserved: not used
    pub client_id: String,
}

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.4>
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct PollingDeviceTokenRequest {
    /// Reserved: not used
    pub grant_type: String,
    /// Reserved: not used
    pub client_id: String,
    pub device_code: String,
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub enum TokenDeviceRequest {
    StartDeviceFlow(StartDeviceFlowRequest),
    PollingTokenDevice(PollingDeviceTokenRequest),
}

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.2>
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct StartDeviceFlowResponse {
    pub device_code: String,
    /// Reserved: not used
    pub user_code: String,
    /// Reserved: not used
    pub verification_uri: String,
    #[schema(minimum = 1, example = 5)]
    pub interval: u64,
    #[schema(minimum = 1, example = 300)]
    pub expires_in: u64,
}

/// <https://datatracker.ietf.org/doc/html/rfc8628#section-3.5>
#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct FinishDeviceFlowResponse {
    pub access_token: String,
    /// Reserved: not used
    pub refresh_token: String,
    pub token_type: String,
    /// Reserved: not used
    #[schema(minimum = 1, example = 3600)]
    pub expires_in: u64,
    #[schema(minimum = 1, example = 5)]
    /// Reserved: not used
    pub score: String,
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub enum TokenDeviceResponse {
    StartDeviceFlow(StartDeviceFlowResponse),
    FinishDeviceFlow(FinishDeviceFlowResponse),
}

#[derive(Serialize, Deserialize, utoipa::ToSchema, strum::Display)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum TokenDeviceErrorStatus {
    /// The device is polling too frequently
    SlowDown,
    /// The user has not either allowed or denied the request yet
    AuthorizationPending,
    /// The user denies the request
    AccessDenied,
    /// The device code has expired
    ExpiredToken,
}

#[derive(Serialize, Deserialize, utoipa::ToSchema)]
pub struct TokenDeviceError {
    pub message: TokenDeviceErrorStatus,
}

impl IntoApiError for TokenDeviceError {
    fn api_err(self) -> ApiError {
        ApiError::bad_request_with_message(self.message.to_string().as_str())
    }
}

/// Generating and receiving a token according to [OAuth 2.0 Device Authorization Grant](https://oauth.net/2/device-flow/)
#[utoipa::path(
    post,
    path = "/platform/token/device",
    request_body(
        content = TokenDeviceRequest,
        content_type = "application/x-www-form-urlencoded"
    ),
    responses(
        (status = OK, body = TokenDeviceResponse),
        (status = BAD_REQUEST, body = TokenDeviceError)
    ),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
#[expect(clippy::unused_async)]
pub async fn platform_token_device_handler(
    catalog: Extension<Catalog>,
    Form(request): Form<TokenDeviceRequest>,
) -> Result<Json<TokenDeviceResponse>, ApiError> {
    let device_access_token_service = catalog
        .get_one::<dyn kamu_accounts::DeviceAccessTokenService>()
        .unwrap();

    match request {
        TokenDeviceRequest::StartDeviceFlow(request) => {
            let client_id = DeviceClientId::try_new(request.client_id)
                .int_err()
                .api_err()?;
            let device_code = device_access_token_service
                .create_device_code(&client_id)
                .api_err()?;

            Ok(Json(TokenDeviceResponse::StartDeviceFlow(
                StartDeviceFlowResponse {
                    device_code: device_code.into_inner(),
                    // Reserved
                    user_code: String::new(),
                    // Reserved
                    verification_uri: String::new(),
                    // TODO: Device Flow: remove magic numbers
                    interval: 5,
                    expires_in: 300,
                },
            )))
        }
        TokenDeviceRequest::PollingTokenDevice(request) => {
            let device_code = DeviceCode::try_new(request.device_code)
                .int_err()
                .api_err()?;
            let maybe_access_token = device_access_token_service
                .find_access_token_by_device_code(&device_code)
                .await
                .api_err()?;

            if let Some(access_token) = maybe_access_token {
                Ok(Json(TokenDeviceResponse::FinishDeviceFlow(
                    FinishDeviceFlowResponse {
                        access_token: access_token.into_inner(),
                        // Reserved
                        refresh_token: String::new(),
                        token_type: "Bearer".to_string(),
                        // TODO: Device Flow: read from the found entity
                        expires_in: 0,
                        // Reserved
                        score: String::new(),
                    },
                )))
            } else {
                // Token not found, deny access
                Err(TokenDeviceError {
                    message: TokenDeviceErrorStatus::AccessDenied,
                }
                .api_err())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn is_dataset_optional_for_request(request: &http::Request<axum::body::Body>) -> bool {
    request.uri().path() == "/push"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
