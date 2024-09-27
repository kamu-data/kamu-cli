// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::future::{Future, IntoFuture};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use axum::Extension;
use database_common_macros::transactional_handler;
use dill::{Catalog, CatalogBuilder};
use http_common::ApiError;
use indoc::indoc;
use internal_error::*;
use kamu::domain::{Protocols, ServerUrlConfig};
use kamu_adapter_http::e2e::e2e_router;
use kamu_flow_system_inmem::domain::FlowExecutor;
use kamu_task_system_inmem::domain::TaskExecutor;
use messaging_outbox::OutboxExecutor;
use tokio::sync::Notify;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct APIServer {
    server: axum::serve::Serve<axum::routing::IntoMakeService<axum::Router>, axum::Router>,
    local_addr: SocketAddr,
    task_executor: Arc<dyn TaskExecutor>,
    flow_executor: Arc<dyn FlowExecutor>,
    outbox_executor: Arc<OutboxExecutor>,
    maybe_shutdown_notify: Option<Arc<Notify>>,
}

impl APIServer {
    pub async fn new(
        server_catalog: &Catalog,
        multi_tenant_workspace: bool,
        address: Option<IpAddr>,
        port: Option<u16>,
        external_address: Option<IpAddr>,
        e2e_output_data_path: Option<&PathBuf>,
    ) -> Result<Self, InternalError> {
        // Background task executor must run with server privileges to execute tasks on
        // behalf of the system, as they are automatically scheduled
        let task_executor = server_catalog.get_one().unwrap();

        let flow_executor = server_catalog.get_one().unwrap();

        let outbox_executor = server_catalog.get_one().unwrap();

        let gql_schema = kamu_adapter_graphql::schema();

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));
        let listener = tokio::net::TcpListener::bind(addr).await.int_err()?;
        let local_addr = listener.local_addr().unwrap();

        let base_url_rest = {
            let mut base_addr_rest = local_addr;

            if let Some(external_address) = external_address {
                base_addr_rest.set_ip(external_address);
            }

            Url::parse(&format!("http://{base_addr_rest}")).expect("URL failed to parse")
        };

        if let Some(path) = e2e_output_data_path {
            fs::write(path, base_url_rest.to_string()).unwrap();
        };

        let default_protocols = Protocols::default();

        let api_server_catalog = CatalogBuilder::new_chained(server_catalog)
            .add_value(ServerUrlConfig::new(Protocols {
                base_url_rest,
                base_url_platform: default_protocols.base_url_platform,
                // Note: this is not a valid endpoint in Web UI mode
                base_url_flightsql: default_protocols.base_url_flightsql,
            }))
            .build();

        let mut app = axum::Router::new()
            .route("/", axum::routing::get(root))
            .route(
                "/graphql",
                axum::routing::get(graphql_playground_handler).post(graphql_handler),
            )
            .route(
                "/platform/login",
                axum::routing::post(kamu_adapter_http::platform_login_handler),
            )
            .route(
                "/platform/token/validate",
                axum::routing::get(kamu_adapter_http::platform_token_validate_handler),
            )
            .route(
                "/platform/file/upload/prepare",
                axum::routing::post(kamu_adapter_http::platform_file_upload_prepare_post_handler),
            )
            .route(
                "/platform/file/upload/:upload_token",
                axum::routing::post(kamu_adapter_http::platform_file_upload_post_handler),
            )
            .nest("/", kamu_adapter_http::data::root_router())
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
            .layer(Extension(gql_schema))
            .layer(Extension(api_server_catalog));

        let is_e2e_testing = e2e_output_data_path.is_some();
        let maybe_shutdown_notify = if is_e2e_testing {
            let shutdown_notify = Arc::new(Notify::new());

            app = app.nest("/e2e", e2e_router(shutdown_notify.clone()));

            Some(shutdown_notify)
        } else {
            None
        };

        let server = axum::serve(listener, app.into_make_service());

        Ok(Self {
            server,
            local_addr,
            task_executor,
            flow_executor,
            outbox_executor,
            maybe_shutdown_notify,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub async fn run(self) -> Result<(), InternalError> {
        let server_run_fut: Pin<Box<dyn Future<Output = _>>> =
            if let Some(shutdown_notify) = self.maybe_shutdown_notify {
                Box::pin(async move {
                    let server_with_graceful_shutdown =
                        self.server.with_graceful_shutdown(async move {
                            shutdown_notify.notified().await;
                        });

                    server_with_graceful_shutdown.await
                })
            } else {
                Box::pin(self.server.into_future())
            };

        tokio::select! {
            res = server_run_fut => { res.int_err() },
            res = self.outbox_executor.run() => { res.int_err() },
            res = self.task_executor.run() => { res.int_err() },
            res = self.flow_executor.run() => { res.int_err() }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn root() -> impl axum::response::IntoResponse {
    axum::response::Html(indoc!(
        r#"
        <h1>Kamu HTTP Server</h1>
        <ul>
            <li><a href="/graphql">GraphQL Playground</li>
        </ul>
        "#
    ))
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

async fn graphql_playground_handler() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
