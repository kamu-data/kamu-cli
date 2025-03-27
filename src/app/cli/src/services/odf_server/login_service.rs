// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::PROVIDER_PASSWORD;
use kamu_adapter_http::LoginRequestBody;
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;
use tokio::sync::Notify;
use url::Url;

use crate::odf_server;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_ODF_FRONTEND_URL: &str = "https://platform.demo.kamu.dev";
pub const DEFAULT_ODF_BACKEND_URL: &str = "https://api.demo.kamu.dev";

struct WebServer {
    server_future: Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Send>>,
    local_addr: SocketAddr,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct LoginService {}

#[component(pub)]
impl LoginService {
    pub fn new() -> Self {
        Self {}
    }

    #[tracing::instrument(skip_all, fields(%body))]
    async fn post_handler(
        axum::extract::State(response_tx): axum::extract::State<
            tokio::sync::mpsc::Sender<FrontendLoginCallbackResponse>,
        >,
        body: String,
    ) -> impl axum::response::IntoResponse {
        let response_result = serde_json::from_str::<FrontendLoginCallbackResponse>(body.as_str());
        match response_result {
            Ok(response) => {
                response_tx.send(response).await.unwrap();

                axum::response::Response::builder()
                    .status(200)
                    .body("{}".to_string())
                    .unwrap()
            }
            Err(e) => axum::response::Response::builder()
                .status(400)
                .body(e.to_string())
                .unwrap(),
        }
    }

    async fn initialize_cli_web_server(
        &self,
        server_frontend_url: &Url,
        response_tx: tokio::sync::mpsc::Sender<FrontendLoginCallbackResponse>,
    ) -> Result<WebServer, InternalError> {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));
        let listener = tokio::net::TcpListener::bind(addr).await.int_err()?;
        let local_addr = listener.local_addr().unwrap();

        let redirect_url = format!(
            "{}?callbackUrl=http://{}/",
            server_frontend_url.join("/v/login").unwrap(),
            local_addr
        );

        let app = axum::Router::new()
            .route(
                "/",
                axum::routing::get(|| async move {
                    axum::response::Redirect::permanent(redirect_url.as_str())
                })
                .post(Self::post_handler),
            )
            .layer(
                tower::ServiceBuilder::new()
                    .layer(tower_http::trace::TraceLayer::new_for_http())
                    .layer(
                        tower_http::cors::CorsLayer::new()
                            .allow_origin(tower_http::cors::Any)
                            .allow_methods(vec![http::Method::GET, http::Method::POST])
                            .allow_headers(tower_http::cors::Any),
                    ),
            )
            .with_state(response_tx);

        let server_future = Box::pin(axum::serve(listener, app.into_make_service()).into_future());

        Ok(WebServer {
            server_future,
            local_addr,
        })
    }

    async fn obtain_callback_response(
        cli_web_server: WebServer,
        mut response_rx: tokio::sync::mpsc::Receiver<FrontendLoginCallbackResponse>,
    ) -> Result<Option<FrontendLoginCallbackResponse>, InternalError> {
        let ctrlc_rx = ctrlc_channel().int_err()?;
        let cli_web_server = cli_web_server.server_future;

        tokio::select! {
            maybe_login_response = response_rx.recv() => {
                tracing::info!(?maybe_login_response, "Shutting down web server, as obtained callback response");
                Ok(maybe_login_response)
            }
            _ = ctrlc_rx.notified() => {
                tracing::info!("Shutting down web server, as Ctrl+C pressed");
                Ok(None)
            }
            _ = cli_web_server => {
                tracing::info!("Shutting down web server, as it died first");
                Ok(None)
            }
        }
    }

    pub async fn login_interactive(
        &self,
        odf_server_frontend_url: &Url,
        web_server_started_callback: impl Fn(&String) + Send,
    ) -> Result<FrontendLoginCallbackResponse, LoginError> {
        let (response_tx, response_rx) =
            tokio::sync::mpsc::channel::<FrontendLoginCallbackResponse>(1);

        let cli_web_server = self
            .initialize_cli_web_server(odf_server_frontend_url, response_tx)
            .await?;

        let cli_web_server_url = format!("http://{}", cli_web_server.local_addr);
        web_server_started_callback(&cli_web_server_url);
        let _ = webbrowser::open(&cli_web_server_url);

        let maybe_callback_response =
            Self::obtain_callback_response(cli_web_server, response_rx).await?;
        maybe_callback_response.ok_or_else(|| {
            LoginError::AccessFailed(LoginErrorAccessFailed {
                reason: String::from("No response"),
            })
        })
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
            _ => panic!(
                "Unexpected validation status code: {}, details: {}",
                response.status().as_str(),
                response.text().await.unwrap()
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FrontendLoginCallbackResponse {
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
    AccessFailed(LoginErrorAccessFailed),

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

// TODO: move to CLI general location
fn ctrlc_channel() -> Result<Arc<Notify>, ctrlc::Error> {
    let notify_rx = Arc::new(Notify::new());
    let notify_tx = notify_rx.clone();
    ctrlc::set_handler(move || {
        notify_tx.notify_waiters();
    })?;

    Ok(notify_rx)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
