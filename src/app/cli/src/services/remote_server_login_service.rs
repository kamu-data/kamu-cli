// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use console::style as s;
use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::Notify;
use url::Url;

use crate::{OutputConfig, RemoteServerAccountCredentials};

////////////////////////////////////////////////////////////////////////////////////////

pub const DEFAULT_LOGIN_URL: &str = "http://localhost:4200";

type WebServer =
    axum::Server<hyper::server::conn::AddrIncoming, axum::routing::IntoMakeService<axum::Router>>;

////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteServerLoginService {
    output_config: Arc<OutputConfig>,
}

#[component(pub)]
impl RemoteServerLoginService {
    pub fn new(output_config: Arc<OutputConfig>) -> Self {
        Self { output_config }
    }

    #[tracing::instrument(
        level = "info",
        skip_all,
        fields(
            body = %body,
        )
    )]
    async fn post_handler(
        axum::extract::State(response_tx): axum::extract::State<
            tokio::sync::mpsc::Sender<LoginCallbackResponse>,
        >,
        body: String,
    ) -> impl axum::response::IntoResponse {
        let response_result = serde_json::from_str::<LoginCallbackResponse>(body.as_str());
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

    fn initialize_cli_web_server(
        &self,
        server_frontend_url: &Url,
        response_tx: tokio::sync::mpsc::Sender<LoginCallbackResponse>,
    ) -> WebServer {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));

        let bound_addr = hyper::server::conn::AddrIncoming::bind(&addr).unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        });

        let redirect_url = format!(
            "{}?callbackUrl=http://{}/",
            server_frontend_url.join("/v/login").unwrap(),
            bound_addr.local_addr().to_string()
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

        axum::Server::builder(bound_addr).serve(app.into_make_service())
    }

    fn open_web_browser(&self, cli_web_server_url: &String) {
        tracing::info!("HTTP server is listening on: {}", cli_web_server_url);

        if self.output_config.is_tty
            && self.output_config.verbosity_level == 0
            && !self.output_config.quiet
        {
            eprintln!(
                "{}\n  {}",
                s("HTTP server is listening on:").green().bold(),
                s(&cli_web_server_url).bold(),
            );
            eprintln!("{}", s("Use Ctrl+C to stop the server").yellow());
        }

        let _ = webbrowser::open(&cli_web_server_url);
    }

    async fn obtain_callback_response(
        mut cli_web_server: WebServer,
        mut response_rx: tokio::sync::mpsc::Receiver<LoginCallbackResponse>,
    ) -> Result<Option<LoginCallbackResponse>, InternalError> {
        let ctrlc_rx = ctrlc_channel().int_err()?;

        tokio::select! {
            maybe_login_response = response_rx.recv() => {
                tracing::info!(?maybe_login_response, "Shutting down web server, as obtained callback response");
                Ok(maybe_login_response)
            }
            _ = ctrlc_rx.notified() => {
                tracing::info!("Shutting down web server, as Ctrl+C pressed");
                Ok(None)
            }
            _ = &mut cli_web_server => {
                tracing::info!("Shutting down web server, as it died first");
                Ok(None)
            }
        }
    }

    #[allow(dead_code)]
    pub async fn login(
        &self,
        remote_server_frontend_url: &Url,
    ) -> Result<LoginCallbackResponse, RemoteServerLoginError> {
        let (response_tx, response_rx) = tokio::sync::mpsc::channel::<LoginCallbackResponse>(1);

        let cli_web_server =
            self.initialize_cli_web_server(remote_server_frontend_url, response_tx);

        let cli_web_server_url = format!("http://{}", cli_web_server.local_addr());
        self.open_web_browser(&cli_web_server_url);

        let maybe_callback_response =
            Self::obtain_callback_response(cli_web_server, response_rx).await?;
        maybe_callback_response.ok_or_else(|| RemoteServerLoginError::CredentialsNotObtained)
    }

    #[allow(dead_code)]
    pub async fn validate_login_credentials(
        &self,
        _remote_server_url: &Url,
        _account_credentials: RemoteServerAccountCredentials,
    ) -> Result<(), RemoteServerValidateLoginError> {
        // TODO
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginCallbackResponse {
    pub access_token: String,
    pub backend_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RemoteServerLoginError {
    #[error("Did not obtain user credentials")]
    CredentialsNotObtained,

    #[error(transparent)]
    Internal(InternalError),
}

impl From<InternalError> for RemoteServerLoginError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RemoteServerValidateLoginError {
    #[error(transparent)]
    ExpiredCredentials(RemoteServerExpiredCredentialsError),

    #[error(transparent)]
    InvalidCredentials(RemoteServerInvalidCredentialsError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Credentials for '{server_url}' remote server have expired. Please re-run `kamu login`")]
pub struct RemoteServerExpiredCredentialsError {
    server_url: Url,
}

#[derive(Debug, Error)]
#[error("Credentials for '{server_url}' are invalid. Please re-run `kamu login`")]
pub struct RemoteServerInvalidCredentialsError {
    server_url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////

fn ctrlc_channel() -> Result<Arc<Notify>, ctrlc::Error> {
    let notify_rx = Arc::new(Notify::new());
    let notify_tx = notify_rx.clone();
    ctrlc::set_handler(move || {
        let _ = notify_tx.notify_waiters();
    })?;

    Ok(notify_rx)
}

////////////////////////////////////////////////////////////////////////////////////////
