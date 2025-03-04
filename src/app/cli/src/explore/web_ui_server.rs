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

use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use database_common::DatabaseTransactionRunner;
use database_common_macros::transactional_handler;
use dill::{Catalog, CatalogBuilder};
use http_common::ApiError;
use internal_error::*;
use kamu::domain::{Protocols, ServerUrlConfig, TenancyConfig};
use kamu_accounts::{
    AccountConfig,
    AuthenticationService,
    PredefinedAccountsConfig,
    PROVIDER_PASSWORD,
};
use kamu_accounts_services::PasswordLoginCredentials;
use kamu_adapter_http::{DatasetAuthorizationLayer, FileUploadLimitConfig};
use observability::axum::unknown_fallback_handler;
use rust_embed::RustEmbed;
use serde::Serialize;
use url::Url;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

use super::{UIConfiguration, UIFeatureFlags};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(RustEmbed)]
#[folder = "$KAMU_WEB_UI_DIR"]
struct HttpRoot;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebUIRuntimeConfiguration {
    api_server_gql_url: String,
    api_server_http_url: String,
    login_instructions: Option<WebUILoginInstructions>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebUILoginInstructions {
    login_method: String,
    login_credentials_json: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WebUIServer {
    server_future: Pin<Box<dyn std::future::Future<Output = Result<(), std::io::Error>>>>,
    local_addr: SocketAddr,
    access_token: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WebUIServer {
    pub async fn new(
        server_catalog: Catalog,
        tenancy_config: TenancyConfig,
        current_account_name: odf::AccountName,
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        file_upload_limit_config: Arc<FileUploadLimitConfig>,
        enable_dataset_env_vars_management: bool,
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
            .or_else(|| {
                Some(AccountConfig::test_config_from_name(
                    current_account_name.clone(),
                ))
            })
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

        let web_ui_url = format!("http://{local_addr}");

        let web_ui_runtime_configuration = WebUIRuntimeConfiguration {
            api_server_gql_url: format!("http://{local_addr}/graphql"),
            api_server_http_url: web_ui_url.clone(),
            login_instructions: Some(login_instructions.clone()),
        };

        let ui_configuration = UIConfiguration {
            ingest_upload_file_limit_mb: file_upload_limit_config.max_file_size_in_mb(),
            feature_flags: UIFeatureFlags {
                // No way to log out, always logging in a predefined user
                enable_logout: false,
                // No way to configure scheduling of datasets
                enable_scheduling: false,
                enable_dataset_env_vars_management,
                enable_terms_of_service: true,
            },
        };

        let access_token = Self::acquire_access_token(server_catalog.clone(), &login_instructions)
            .await
            .expect("Token not retrieved");

        let web_ui_url = Url::parse(&web_ui_url).expect("URL failed to parse");

        let default_protocols = Protocols::default();

        let web_ui_catalog = CatalogBuilder::new_chained(&server_catalog)
            .add_value(ServerUrlConfig::new(Protocols {
                base_url_platform: web_ui_url.clone(),
                base_url_rest: web_ui_url,
                // Note: this is not a valid endpoint in Web UI mode
                base_url_flightsql: default_protocols.base_url_flightsql,
            }))
            .build();

        let (router, api) = OpenApiRouter::with_openapi(
            kamu_adapter_http::openapi::spec_builder(crate::app::VERSION, "").build(),
        )
        .route(
            "/assets/runtime-config.json",
            axum::routing::get(runtime_configuration_handler),
        )
        .route("/ui-config", axum::routing::get(ui_configuration_handler))
        .route(
            "/graphql",
            axum::routing::get(graphql_playground_handler).post(graphql_handler),
        )
        .merge(kamu_adapter_http::data::root_router())
        .routes(routes!(
            kamu_adapter_http::platform_file_upload_prepare_post_handler
        ))
        .routes(routes!(
            kamu_adapter_http::platform_file_upload_post_handler,
            kamu_adapter_http::platform_file_upload_get_handler
        ))
        .nest(
            "/odata",
            match tenancy_config {
                TenancyConfig::MultiTenant => kamu_adapter_odata::router_multi_tenant(),
                TenancyConfig::SingleTenant => kamu_adapter_odata::router_single_tenant(),
            },
        )
        .nest(
            match tenancy_config {
                TenancyConfig::MultiTenant => "/{account_name}/{dataset_name}",
                TenancyConfig::SingleTenant => "/{dataset_name}",
            },
            kamu_adapter_http::add_dataset_resolver_layer(
                OpenApiRouter::new()
                    .merge(kamu_adapter_http::smart_transfer_protocol_router())
                    .merge(kamu_adapter_http::data::dataset_router())
                    .layer(DatasetAuthorizationLayer::default()),
                tenancy_config,
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
        .merge(kamu_adapter_http::openapi::router().into())
        .layer(axum::extract::Extension(web_ui_catalog))
        .layer(axum::extract::Extension(gql_schema))
        .layer(axum::extract::Extension(web_ui_runtime_configuration))
        .layer(axum::extract::Extension(ui_configuration))
        .fallback(unknown_fallback_handler)
        .split_for_parts();

        let server_future = Box::pin(
            axum::serve(
                listener,
                router
                    .layer(axum::extract::Extension(Arc::new(api)))
                    .into_make_service(),
            )
            .into_future(),
        );

        Ok(Self {
            server_future,
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
        self.server_future.await
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

async fn runtime_configuration_handler(
    web_ui_runtime_configuration: axum::extract::Extension<WebUIRuntimeConfiguration>,
) -> axum::Json<WebUIRuntimeConfiguration> {
    axum::Json(web_ui_runtime_configuration.0)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn ui_configuration_handler(
    ui_configuration: axum::extract::Extension<UIConfiguration>,
) -> axum::Json<UIConfiguration> {
    axum::Json(ui_configuration.0)
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
