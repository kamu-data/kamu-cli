// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;

use axum::Extension;
use database_common_macros::transactional_handler;
use dill::{Catalog, CatalogBuilder};
use http_common::ApiError;
use indoc::indoc;
use internal_error::*;
use kamu::domain::{Protocols, ServerUrlConfig, SystemTimeSource};
use kamu_adapter_http::e2e::e2e_router;
use kamu_flow_system_inmem::domain::FlowService;
use kamu_task_system_inmem::domain::TaskExecutor;
use tokio::sync::Notify;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct APIServer {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
    task_executor: Arc<dyn TaskExecutor>,
    flow_service: Arc<dyn FlowService>,
    time_source: Arc<dyn SystemTimeSource>,
    maybe_shutdown_notify: Option<Arc<Notify>>,
}

impl APIServer {
    pub fn new(
        base_catalog: &Catalog,
        cli_catalog: &Catalog,
        multi_tenant_workspace: bool,
        address: Option<IpAddr>,
        port: Option<u16>,
        external_address: Option<IpAddr>,
        is_e2e_testing: bool,
    ) -> Self {
        // Background task executor must run with server privileges to execute tasks on
        // behalf of the system, as they are automatically scheduled
        let task_executor = cli_catalog.get_one().unwrap();

        let flow_service = cli_catalog.get_one().unwrap();

        let time_source = base_catalog.get_one().unwrap();

        let gql_schema = kamu_adapter_graphql::schema();

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));
        let bound_addr = hyper::server::conn::AddrIncoming::bind(&addr).unwrap_or_else(|e| {
            panic!("error binding to {addr}: {e}");
        });

        let base_url_rest = {
            let mut base_addr_rest = addr;

            if let Some(external_address) = external_address {
                base_addr_rest.set_ip(external_address);
            }

            Url::parse(&format!("http://{base_addr_rest}")).expect("URL failed to parse")
        };

        let default_protocols = Protocols::default();

        let api_server_catalog = CatalogBuilder::new_chained(base_catalog)
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
            .layer(
                tower::ServiceBuilder::new()
                    .layer(tower_http::trace::TraceLayer::new_for_http())
                    .layer(
                        tower_http::cors::CorsLayer::new()
                            .allow_origin(tower_http::cors::Any)
                            .allow_methods(vec![http::Method::GET, http::Method::POST])
                            .allow_headers(tower_http::cors::Any),
                    )
                    .layer(Extension(api_server_catalog))
                    .layer(Extension(gql_schema))
                    .layer(kamu_adapter_http::AuthenticationLayer::new()),
            );

        let maybe_shutdown_notify = if is_e2e_testing {
            let shutdown_notify = Arc::new(Notify::new());

            app = app.nest("/e2e", e2e_router(shutdown_notify.clone()));

            Some(shutdown_notify)
        } else {
            None
        };

        let server = axum::Server::builder(bound_addr).serve(app.into_make_service());

        Self {
            server,
            task_executor,
            flow_service,
            time_source,
            maybe_shutdown_notify,
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub async fn run(self) -> Result<(), InternalError> {
        let server_run_fut: Pin<Box<dyn Future<Output = _>>> =
            if let Some(shutdown_notify) = self.maybe_shutdown_notify {
                Box::pin(async move {
                    let server_with_graceful_shutdown = self.server.with_graceful_shutdown(async {
                        shutdown_notify.notified().await;
                    });

                    server_with_graceful_shutdown.await
                })
            } else {
                Box::pin(self.server)
            };

        tokio::select! {
            res = server_run_fut => { res.int_err() },
            res = self.task_executor.run() => { res.int_err() },
            res = self.flow_service.run(self.time_source.now()) => { res.int_err() }
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
