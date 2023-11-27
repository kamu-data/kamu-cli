// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::http::Uri;
use axum::response::{IntoResponse, Response};
use dill::Catalog;
use kamu::domain::auth::AuthenticationService;
use opendatafabric::AccountName;
use rust_embed::RustEmbed;
use serde::Serialize;

use crate::accounts;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(RustEmbed)]
#[folder = "$KAMU_WEB_UI_DIR"]
struct HttpRoot;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WebUIConfig {
    api_server_gql_url: String,
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
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct WebUIServer {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
    access_token: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl WebUIServer {
    pub async fn new(
        base_catalog: Catalog,
        multi_tenant_workspace: bool,
        current_account_name: AccountName,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Self {
        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));

        let bound_addr = hyper::server::conn::AddrIncoming::bind(&addr).unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        });

        let login_credentials = accounts::PasswordLoginCredentials {
            login: current_account_name.to_string(),
            // Note: note a mistake, use identical login and password, equal to account name
            password: current_account_name.to_string(),
        };

        let gql_schema = kamu_adapter_graphql::schema();

        let login_instructions = WebUILoginInstructions {
            login_method: accounts::LOGIN_METHOD_PASSWORD.to_string(),
            login_credentials_json: serde_json::to_string::<accounts::PasswordLoginCredentials>(
                &login_credentials,
            )
            .unwrap(),
        };

        let auth_svc = base_catalog.get_one::<dyn AuthenticationService>().unwrap();
        let access_token = auth_svc
            .login(
                &login_instructions.login_method,
                login_instructions.login_credentials_json.clone(),
            )
            .await
            .unwrap()
            .access_token;

        let web_ui_config = WebUIConfig {
            api_server_gql_url: format!("http://{}/graphql", bound_addr.local_addr()),
            login_instructions: Some(login_instructions),
            feature_flags: WebUIFeatureFlags {
                // No way to log out, always logging in a predefined user
                enable_logout: false,
                // No way to configure scheduling of datasets
                enable_scheduling: false,
            },
        };

        let app = axum::Router::new()
            .route(
                "/assets/runtime-config.json",
                axum::routing::get(runtime_config_handler),
            )
            .route(
                "/graphql",
                axum::routing::get(graphql_playground_handler).post(graphql_handler),
            )
            .route(
                "/platform/token/validate",
                axum::routing::get(kamu_adapter_http::platform_token_validate_handler),
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
                        .nest("/", kamu_adapter_http::data::router()),
                    multi_tenant_workspace,
                ),
            )
            .fallback(app_handler)
            .layer(
                tower::ServiceBuilder::new()
                    .layer(tower_http::trace::TraceLayer::new_for_http())
                    .layer(
                        tower_http::cors::CorsLayer::new()
                            .allow_origin(tower_http::cors::Any)
                            .allow_methods(vec![http::Method::GET, http::Method::POST])
                            .allow_headers(tower_http::cors::Any),
                    )
                    .layer(axum::extract::Extension(base_catalog))
                    .layer(axum::extract::Extension(gql_schema))
                    .layer(axum::extract::Extension(web_ui_config))
                    .layer(kamu_adapter_http::AuthenticationLayer::new()),
            );

        let server = axum::Server::builder(bound_addr).serve(app.into_make_service());

        Self {
            server,
            access_token,
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub fn get_access_token(&self) -> String {
        self.access_token.clone()
    }

    pub async fn run(self) -> Result<(), hyper::Error> {
        self.server.await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

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
        .body(axum::body::Full::from(file.data))
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn runtime_config_handler(
    web_ui_config: axum::extract::Extension<WebUIConfig>,
) -> axum::Json<WebUIConfig> {
    axum::Json(web_ui_config.0)
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn graphql_handler(
    schema: axum::extract::Extension<kamu_adapter_graphql::Schema>,
    catalog: axum::extract::Extension<dill::Catalog>,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    let graphql_request = req.into_inner().data(catalog.0);
    schema.execute(graphql_request).await.into()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn graphql_playground_handler() -> impl IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

/////////////////////////////////////////////////////////////////////////////////////////
