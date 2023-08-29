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
use kamu_task_system_inmem::domain::TaskExecutor;

// Extractor of dataset identity for smart transfer protocol
#[derive(serde::Deserialize)]
struct DatasetByName {
    dataset_name: opendatafabric::DatasetName,
}

pub struct APIServer {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
    task_executor: Arc<dyn TaskExecutor>,
}

impl APIServer {
    pub fn new(catalog: Catalog, address: Option<IpAddr>, port: Option<u16>) -> Self {
        use axum::extract::{Extension, Path};

        let task_executor = catalog.get_one().unwrap();

        let gql_schema = kamu_adapter_graphql::schema(catalog.clone());

        let app = axum::Router::new()
            .route("/", axum::routing::get(root))
            .route(
                "/graphql",
                axum::routing::get(graphql_playground).post(graphql_handler),
            )
            .nest(
                "/:dataset_name",
                kamu_adapter_http::smart_transfer_protocol_routes()
                    .layer(kamu_adapter_http::DatasetResolverLayer::new(
                        |Path(p): Path<DatasetByName>| p.dataset_name.as_local_ref(),
                    ))
                    .layer(Extension(catalog)),
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
                    .layer(Extension(gql_schema)),
            );

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));

        let server = axum::Server::bind(&addr).serve(app.into_make_service());

        Self {
            server,
            task_executor,
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub async fn run(self) -> Result<(), InternalError> {
        tokio::select! {
            res = self.server => { res.int_err() },
            res = self.task_executor.run() => { res.int_err() },
        }
    }
}

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

async fn graphql_handler(
    schema: axum::extract::Extension<kamu_adapter_graphql::Schema>,
    maybe_access_token_header: Option<
        axum::TypedHeader<axum::headers::Authorization<axum::headers::authorization::Bearer>>,
    >,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    let req_inner = if let Some(th) = maybe_access_token_header {
        req.into_inner()
            .data(kamu_adapter_graphql::AccessToken::new(th.token()))
    } else {
        req.into_inner()
    };

    schema.execute(req_inner).await.into()
}

async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}
