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

use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use database_common::DatabaseTransactionRunner;
use database_common_macros::transactional_handler;
use dill::{Catalog, CatalogBuilder};
use http_common::ApiError;
use internal_error::*;
use kamu::domain::{Protocols, ServerUrlConfig};
use kamu_accounts::{
    AccountConfig,
    AuthenticationService,
    PredefinedAccountsConfig,
    PROVIDER_PASSWORD,
};
use kamu_accounts_services::PasswordLoginCredentials;
use kamu_adapter_http::FileUploadLimitConfig;
use opendatafabric::AccountName;
use rust_embed::RustEmbed;
use serde::Serialize;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(RustEmbed)]
#[folder = "$KAMU_WEB_UI_DIR"]
struct HttpRoot;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebUIConfig {
    api_server_gql_url: String,
    api_server_http_url: String,
    ingest_upload_file_limit_mb: usize,
    login_instructions: Option<WebUILoginInstructions>,
    feature_flags: WebUIFeatureFlags,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebUILoginInstructions {
    login_method: String,
    login_credentials_json: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebUIFeatureFlags {
    enable_logout: bool,
    enable_scheduling: bool,
    enable_dataset_env_vars_managment: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebUIServer {
    server: axum::serve::Serve<axum::routing::IntoMakeService<axum::Router>, axum::Router>,
    local_addr: SocketAddr,
    access_token: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebUIServer {
    pub async fn new(
        base_catalog: Catalog,
        multi_tenant_workspace: bool,
        current_account_name: AccountName,
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        file_upload_limit_config: Arc<FileUploadLimitConfig>,
        enable_dataset_env_vars_managment: bool,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Result<Self, InternalError> {
        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));
        let listener = tokio::net::TcpListener::bind(addr).await.int_err()?;
        let local_addr = listener.local_addr().unwrap();

        let account_config = predefined_accounts_config
            .find_account_config_by_name(&current_account_name)
            .or_else(|| Some(AccountConfig::from_name(current_account_name.clone())))
            .unwrap();

        let login_credentials = PasswordLoginCredentials {
            login: current_account_name.to_string(),
            password: account_config.get_password(),
        };

        let gql_schema = kamu_adapter_graphql::schema();

        let login_instructions = WebUILoginInstructions {
            login_method: String::from(PROVIDER_PASSWORD),
            login_credentials_json: serde_json::to_string(&login_credentials).unwrap(),
        };

        let web_ui_url = format!("http://{}", local_addr);

        let web_ui_config = WebUIConfig {
            api_server_gql_url: format!("http://{}/graphql", local_addr),
            api_server_http_url: web_ui_url.clone(),
            login_instructions: Some(login_instructions.clone()),
            ingest_upload_file_limit_mb: file_upload_limit_config.max_file_size_in_mb(),
            feature_flags: WebUIFeatureFlags {
                // No way to log out, always logging in a predefined user
                enable_logout: false,
                // No way to configure scheduling of datasets
                enable_scheduling: false,
                enable_dataset_env_vars_managment,
            },
        };

        let access_token = Self::acquire_access_token(base_catalog.clone(), &login_instructions)
            .await
            .expect("Token not retreieved");

        let web_ui_url = Url::parse(&web_ui_url).expect("URL failed to parse");

        let default_protocols = Protocols::default();

        let web_ui_catalog = CatalogBuilder::new_chained(&base_catalog)
            .add_value(ServerUrlConfig::new(Protocols {
                base_url_platform: web_ui_url.clone(),
                base_url_rest: web_ui_url,
                // Note: this is not a valid endpoint in Web UI mode
                base_url_flightsql: default_protocols.base_url_flightsql,
            }))
            .build();

        let app = axum::Router::new()
            .route(
                "/assets/runtime-config.json",
                axum::routing::get(runtime_config_handler),
            )
            .route(
                "/graphql",
                axum::routing::get(graphql_playground_handler).post(graphql_handler),
            )
            .nest("/", kamu_adapter_http::data::root_router())
            .route(
                "/platform/file/upload/prepare",
                axum::routing::post(kamu_adapter_http::platform_file_upload_prepare_post_handler),
            )
            .route(
                "/platform/file/upload/:upload_token",
                axum::routing::post(kamu_adapter_http::platform_file_upload_post_handler),
            )
            .nest(
                "/odata",
                if multi_tenant_workspace {
                    kamu_adapter_odata::router_multi_tenant()
                } else {
                    kamu_adapter_odata::router_single_tenant()
                },
            )
            .nest(
                if multi_tenant_workspace {
                    "/:account_name/:dataset_name"
                } else {
                    "/:dataset_name"
                },
                kamu_adapter_http::add_dataset_resolver_layer(
                    axum::Router::new()
                        .nest("/", kamu_adapter_http::smart_transfer_protocol_router())
                        .nest("/", kamu_adapter_http::data::dataset_router()),
                    multi_tenant_workspace,
                ),
            )
            .fallback(app_handler)
            .layer(kamu_adapter_http::AuthenticationLayer::new())
            .layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![http::Method::GET, http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            )
            .layer(observability::axum::http_layer())
            // Note: Healthcheck and metrics routes are placed before the tracing layer (layers
            // execute bottom-up) to avoid spam in logs
            .route(
                "/system/health",
                axum::routing::get(observability::health::health_handler),
            )
            .route(
                "/system/metrics",
                axum::routing::get(observability::metrics::metrics_handler),
            )
            .layer(axum::extract::Extension(web_ui_catalog))
            .layer(axum::extract::Extension(gql_schema))
            .layer(axum::extract::Extension(web_ui_config));

        let server = axum::serve(listener, app.into_make_service());

        Ok(Self {
            server,
            local_addr,
            access_token,
        })
    }

    async fn acquire_access_token(
        catalog: Catalog,
        login_instructions: &WebUILoginInstructions,
    ) -> Result<String, InternalError> {
        let transaction_runner = DatabaseTransactionRunner::new(catalog);

        transaction_runner
            .transactional_with(|auth_svc: Arc<dyn AuthenticationService>| async move {
                Ok(auth_svc
                    .login(
                        &login_instructions.login_method,
                        login_instructions.login_credentials_json.clone(),
                    )
                    .await
                    .unwrap()
                    .access_token)
            })
            .await
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub fn get_access_token(&self) -> String {
        self.access_token.clone()
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        self.server.await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn app_handler(uri: Uri) -> impl IntoResponse {
    let file_path = uri.path().trim_start_matches('/');

    let (file, mime) = match HttpRoot::get(file_path) {
        None => (HttpRoot::get("index.html").unwrap(), mime::TEXT_HTML),
        Some(file) => (
            file,
            mime_guess::from_path(file_path).first_or_octet_stream(),
        ),
    };

    Response::builder()
        .header(http::header::CONTENT_TYPE, mime.as_ref())
        .body(axum::body::Body::from(file.data))
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn runtime_config_handler(
    web_ui_config: axum::extract::Extension<WebUIConfig>,
) -> axum::Json<WebUIConfig> {
    axum::Json(web_ui_config.0)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
async fn graphql_handler(
    Extension(schema): Extension<kamu_adapter_graphql::Schema>,
    Extension(catalog): Extension<Catalog>,
    req: async_graphql_axum::GraphQLRequest,
) -> Result<async_graphql_axum::GraphQLResponse, ApiError> {
    let graphql_request = req.into_inner().data(catalog);
    let graphql_response = schema.execute(graphql_request).await.into();

    Ok(graphql_response)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn graphql_playground_handler() -> impl IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
