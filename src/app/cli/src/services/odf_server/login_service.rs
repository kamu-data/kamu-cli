// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Duration;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{OAUTH_DEVICE_ACCESS_TOKEN_GRANT_TYPE, PROVIDER_PASSWORD};
use kamu_adapter_http::platform::{
    DeviceAccessTokenError,
    DeviceAccessTokenErrorStatus,
    DeviceAccessTokenResponse,
    DeviceAuthorizationResponse,
    LoginRequestBody,
};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use time_source::SystemTimeSource;
use url::Url;

use crate::odf_server;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_ODF_FRONTEND_URL: &str = "https://platform.demo.kamu.dev";
pub const DEFAULT_ODF_BACKEND_URL: &str = "https://api.demo.kamu.dev";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
pub struct LoginService {
    time_source: Arc<dyn SystemTimeSource>,

    #[dill::component(explicit)]
    is_e2e_testing: bool,
}

impl LoginService {
    // We do not use this value, but we are required to pass it.
    const DEVICE_FLOW_DEFAULT_CLIENT_ID_PARAM: (&'static str, &'static str) = ("client_id", "kamu");

    pub async fn login_interactive(
        &self,
        odf_server_frontend_url: &Url,
        predefined_odf_server_backend_url: Option<&Url>,
        login_polling_started_callback: impl Fn(&str) + Send,
    ) -> Result<LoginInteractiveResponse, LoginError> {
        // TODO: Replace with REST API client
        let client = reqwest::Client::new();

        // Getting the backend address (API)
        let odf_server_backend_url =
            if let Some(odf_server_backend_url) = predefined_odf_server_backend_url {
                odf_server_backend_url.clone()
            } else {
                self.odf_server_backend_url(&client, odf_server_frontend_url)
                    .await?
            };

        tracing::debug!(url = %odf_server_backend_url, "ODF server backend URL");

        // Authorize the device before we start polling
        let device_authorization_response = self
            .device_authorization(&client, &odf_server_backend_url)
            .await?;

        tracing::debug!(response = ?device_authorization_response, "Authorization response");

        let polling_start_time = self.time_source.now();
        let polling_interval = Duration::seconds(
            device_authorization_response
                .interval_or_default()
                .try_into()
                .unwrap(),
        );
        let polling_url = odf_server_backend_url
            .join("platform/token/device")
            .unwrap();

        // Notify the user and open the browser
        {
            let login_url = device_authorization_response.verification_uri_with_device_code();

            login_polling_started_callback(&login_url);

            if !self.is_e2e_testing {
                let _ = webbrowser::open(&login_url);
            }
        }

        // Start polling
        let expires_in_seconds = if self.is_e2e_testing {
            10
        } else {
            device_authorization_response.expires_in
        };

        loop {
            let device_access_token_response = self
                .device_access_token(
                    &client,
                    &polling_url,
                    &device_authorization_response.device_code,
                )
                .await?;

            tracing::debug!(response = ?device_access_token_response, "Device access token response");

            match device_access_token_response {
                DeviceAccessTokenResult::Success(r) => {
                    return Ok(LoginInteractiveResponse {
                        access_token: r.access_token,
                        backend_url: odf_server_backend_url,
                    });
                }
                DeviceAccessTokenResult::Error(r) => {
                    use DeviceAccessTokenErrorStatus as Status;
                    match r.message {
                        Status::AuthorizationPending | Status::SlowDown => {
                            // Login in progress, continue
                        }
                        Status::AccessDenied | Status::ExpiredToken | Status::InvalidGrant => {
                            return Err(LoginErrorAccessFailed {
                                reason: format!(
                                    "Unexpected device access token error: {}",
                                    r.message
                                ),
                            }
                            .into());
                        }
                    }
                }
            }

            let elapsed_time_in_seconds: u64 = (self.time_source.now() - polling_start_time)
                .num_seconds()
                .try_into()
                .unwrap();

            if elapsed_time_in_seconds >= expires_in_seconds {
                return Err(LoginErrorAccessFailed {
                    reason: format!(
                        "Device authorization expired after {expires_in_seconds} seconds",
                    ),
                }
                .into());
            }

            self.time_source.sleep(polling_interval).await;
        }
    }

    pub async fn login_oauth(
        &self,
        odf_server_backend_url: &Url,
        oauth_login_method: &str,
        access_token: &str,
    ) -> Result<BackendLoginResponse, LoginError> {
        let login_credentials_json = json!({
            "accessToken": access_token,
        })
        .to_string();

        self.invoke_login_method(
            odf_server_backend_url,
            oauth_login_method,
            login_credentials_json,
        )
        .await
    }

    pub async fn login_password(
        &self,
        odf_server_backend_url: &Url,
        login: &str,
        password: &str,
    ) -> Result<BackendLoginResponse, LoginError> {
        let login_credentials_json = json!({
            "login": login,
            "password": password
        })
        .to_string();

        self.invoke_login_method(
            odf_server_backend_url,
            PROVIDER_PASSWORD,
            login_credentials_json,
        )
        .await
    }

    async fn invoke_login_method(
        &self,
        odf_server_backend_url: &Url,
        login_method: &str,
        login_credentials_json: String,
    ) -> Result<BackendLoginResponse, LoginError> {
        // TODO: Replace with REST API client
        let client = reqwest::Client::new();

        let login_url = odf_server_backend_url.join("platform/login").unwrap();
        tracing::info!(?login_url, "Login request");

        let response = client
            .post(login_url)
            .json(&LoginRequestBody {
                login_method: String::from(login_method),
                login_credentials_json,
            })
            .send()
            .await
            .int_err()?;

        match response.status() {
            http::StatusCode::OK => Ok(response.json::<BackendLoginResponse>().await.int_err()?),
            _ => Err(LoginError::AccessFailed(LoginErrorAccessFailed {
                reason: format!(
                    "Status {} {}",
                    response.status().as_str(),
                    response.text().await.unwrap()
                ),
            })),
        }
    }

    pub async fn validate_access_token(
        &self,
        odf_server_backend_url: &Url,
        access_token: &odf_server::AccessToken,
    ) -> Result<(), ValidateAccessTokenError> {
        // TODO: Replace with REST API client
        let client = reqwest::Client::new();

        let validation_url = odf_server_backend_url
            .join("platform/token/validate")
            .unwrap();

        tracing::info!(?validation_url, "Token validation request");

        let response = client
            .get(validation_url)
            .bearer_auth(access_token.access_token.clone())
            .send()
            .await
            .int_err()?;

        match response.status() {
            http::StatusCode::OK => Ok(()),
            http::StatusCode::UNAUTHORIZED => {
                Err(ValidateAccessTokenError::ExpiredToken(ExpiredTokenError {
                    odf_server_backend_url: odf_server_backend_url.clone(),
                }))
            }
            http::StatusCode::BAD_REQUEST => {
                Err(ValidateAccessTokenError::InvalidToken(InvalidTokenError {
                    odf_server_backend_url: odf_server_backend_url.clone(),
                }))
            }
            unexpected_status => panic!(
                "Unexpected validation status code: {}, details: {}",
                unexpected_status.as_u16(),
                response.text().await.unwrap()
            ),
        }
    }

    async fn odf_server_backend_url(
        &self,
        client: &reqwest::Client,
        odf_server_frontend_url: &Url,
    ) -> Result<Url, InternalError> {
        let response = client
            .get(
                odf_server_frontend_url
                    .join("assets/runtime-config.json")
                    .unwrap(),
            )
            .send()
            .await
            .int_err()?;

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            api_server_http_url: Url,
        }

        match response.status() {
            http::StatusCode::OK => Ok(response
                .json::<Response>()
                .await
                .int_err()?
                .api_server_http_url),
            unexpected_status => panic!(
                "Unexpected runtime-config status code: {}, details: {}",
                unexpected_status.as_u16(),
                response.text().await.unwrap()
            ),
        }
    }

    async fn device_authorization(
        &self,
        client: &reqwest::Client,
        odf_server_backend_url: &Url,
    ) -> Result<DeviceAuthorizationResponse, InternalError> {
        let response = client
            .post(
                odf_server_backend_url
                    .join("platform/token/device/authorization")
                    .unwrap(),
            )
            .form(&[Self::DEVICE_FLOW_DEFAULT_CLIENT_ID_PARAM])
            .send()
            .await
            .int_err()?;

        match response.status() {
            http::StatusCode::OK => Ok(response.json().await.int_err()?),
            unexpected_status => panic!(
                "Unexpected device authorization status code: {}, details: {}",
                unexpected_status.as_u16(),
                response.text().await.unwrap()
            ),
        }
    }
    async fn device_access_token(
        &self,
        client: &reqwest::Client,
        login_polling_url: &Url,
        device_code: &str,
    ) -> Result<DeviceAccessTokenResult, InternalError> {
        let response = client
            .post(login_polling_url.clone())
            .form(&[
                Self::DEVICE_FLOW_DEFAULT_CLIENT_ID_PARAM,
                ("device_code", device_code),
                ("grant_type", OAUTH_DEVICE_ACCESS_TOKEN_GRANT_TYPE),
            ])
            .send()
            .await
            .int_err()?;

        match response.status() {
            http::StatusCode::OK => Ok(DeviceAccessTokenResult::Success(
                response.json().await.int_err()?,
            )),
            http::StatusCode::BAD_REQUEST => Ok(DeviceAccessTokenResult::Error(
                response.json().await.int_err()?,
            )),
            unexpected_status => panic!(
                "Unexpected device access status code: {}, details: {}",
                unexpected_status.as_u16(),
                response.text().await.unwrap()
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
enum DeviceAccessTokenResult {
    Success(DeviceAccessTokenResponse),
    Error(DeviceAccessTokenError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInteractiveResponse {
    pub access_token: String,
    pub backend_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackendLoginResponse {
    pub access_token: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum LoginError {
    #[error(transparent)]
    AccessFailed(#[from] LoginErrorAccessFailed),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Did not obtain access token. Reason: {reason}")]
pub struct LoginErrorAccessFailed {
    reason: String,
}

impl From<InternalError> for LoginError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ValidateAccessTokenError {
    #[error(transparent)]
    ExpiredToken(ExpiredTokenError),

    #[error(transparent)]
    InvalidToken(InvalidTokenError),

    #[error(transparent)]
    Internal(InternalError),
}

impl From<InternalError> for ValidateAccessTokenError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

#[derive(Debug, Error)]
#[error("Access token for '{odf_server_backend_url}' ODF server expired.")]
pub struct ExpiredTokenError {
    odf_server_backend_url: Url,
}

#[derive(Debug, Error)]
#[error("Access token for '{odf_server_backend_url}' ODF server are invalid.")]
pub struct InvalidTokenError {
    odf_server_backend_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
