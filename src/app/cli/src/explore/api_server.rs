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
use kamu::domain::{AnonymousAccountReason, CurrentAccountSubject, DatasetRepository};
use kamu_task_system_inmem::domain::TaskExecutor;

// Extractor of dataset identity for single-tenant smart transfer protocol
#[derive(serde::Deserialize)]
struct DatasetByName {
    dataset_name: opendatafabric::DatasetName,
}

// Extractor of account + dataset identity for multi-tenant smart transfer
// protocol
#[derive(serde::Deserialize)]
struct DatasetByAccountAndName {
    account_name: opendatafabric::AccountName,
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
    pub fn new(base_catalog: Catalog, address: Option<IpAddr>, port: Option<u16>) -> Self {
        use axum::extract::Extension;

        let task_executor = base_catalog.get_one().unwrap();

        let multi_tenant = base_catalog
            .get_one::<dyn DatasetRepository>()
            .unwrap()
            .is_multi_tenant();

        let gql_schema = kamu_adapter_graphql::schema();

        let app = axum::Router::new()
            .route("/", axum::routing::get(root))
            .route(
                "/graphql",
                axum::routing::get(graphql_playground).post(graphql_handler),
            )
            .route(
                "/platform/token/validate",
                axum::routing::get(platform_token_validate_handler),
            )
            .nest(
                if multi_tenant {
                    "/:account_name/:dataset_name"
                } else {
                    "/:dataset_name"
                },
                Self::add_dataset_resolver_layer(
                    kamu_adapter_http::smart_transfer_protocol_routes()
                        .layer(kamu_adapter_http::DatasetAuthorizationLayer::new()),
                    multi_tenant,
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
        }
    }

    fn add_dataset_resolver_layer(
        dataset_router: axum::Router,
        multi_tenant: bool,
    ) -> axum::Router {
        use axum::extract::Path;

        if multi_tenant {
            dataset_router.layer(kamu_adapter_http::DatasetResolverLayer::new(
                |Path(p): Path<DatasetByAccountAndName>| {
                    opendatafabric::DatasetAlias::new(Some(p.account_name), p.dataset_name)
                        .into_local_ref()
                },
            ))
        } else {
            dataset_router.layer(kamu_adapter_http::DatasetResolverLayer::new(
                |Path(p): Path<DatasetByName>| p.dataset_name.as_local_ref(),
            ))
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
    catalog: axum::extract::Extension<dill::Catalog>,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    let graphql_request = req.into_inner().data(catalog.0);
    schema.execute(graphql_request).await.into()
}

async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql"),
    ))
}

async fn platform_token_validate_handler(
    catalog: axum::extract::Extension<dill::Catalog>,
) -> axum::response::Response {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();

    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(_) => {
            return axum::response::Response::builder()
                .status(http::StatusCode::OK)
                .body(Default::default())
                .unwrap()
        }
        CurrentAccountSubject::Anonymous(reason) => {
            return axum::response::Response::builder()
                .status(match reason {
                    AnonymousAccountReason::AuthenticationExpired => http::StatusCode::UNAUTHORIZED,
                    AnonymousAccountReason::AuthenticationInvalid => http::StatusCode::BAD_REQUEST,
                    AnonymousAccountReason::NoAuthenticationProvided => {
                        http::StatusCode::BAD_REQUEST
                    }
                })
                .body(Default::default())
                .unwrap();
        }
    }
}
