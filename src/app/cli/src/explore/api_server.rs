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

use dill::Catalog;
use internal_error::*;
use kamu::domain::SystemTimeSource;
use kamu_flow_system_inmem::domain::FlowService;
use kamu_task_system_inmem::domain::TaskExecutor;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct APIServer {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
    task_executor: Arc<dyn TaskExecutor>,
    flow_service: Arc<dyn FlowService>,
    time_source: Arc<dyn SystemTimeSource>,
}

impl APIServer {
    pub fn new(
        base_catalog: Catalog,
        cli_catalog: &Catalog,
        multi_tenant_workspace: bool,
        address: Option<IpAddr>,
        port: Option<u16>,
    ) -> Self {
        use axum::extract::Extension;

        // Background task executor must run with server privileges to execute tasks on
        // behalf of the system, as they are automatically scheduled
        let task_executor = cli_catalog.get_one().unwrap();

        let flow_service = cli_catalog.get_one().unwrap();

        let time_source = base_catalog.get_one().unwrap();

        let gql_schema = kamu_adapter_graphql::schema();

        let app = axum::Router::new()
            .route("/", axum::routing::get(root))
            .route(
                "/graphql",
                axum::routing::get(graphql_playground).post(graphql_handler),
            )
            .route(
                "/platform/token/validate",
                axum::routing::get(kamu_adapter_http::platform_token_validate_handler),
            )
            .nest("/", kamu_adapter_http::data::root_router())
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
                    .layer(Extension(base_catalog))
                    .layer(Extension(gql_schema))
                    .layer(kamu_adapter_http::AuthenticationLayer::new()),
            );

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));

        let server = axum::Server::bind(&addr).serve(app.into_make_service());

        Self {
            server,
            task_executor,
            flow_service,
            time_source,
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub async fn run(self) -> Result<(), InternalError> {
        tokio::select! {
            res = self.server => { res.int_err() },
            res = self.task_executor.run() => { res.int_err() },
            res = self.flow_service.run(self.time_source.now()) => { res.int_err() }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn root() -> impl axum::response::IntoResponse {
    axum::response::Html(
        r#"
<h1>Kamu HTTP Server</h1>
<ul>
    <li><a href="/graphql">GraphQL Playground</li>
</ul>
"#,
    )
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

async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

/////////////////////////////////////////////////////////////////////////////////////////
