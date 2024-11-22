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

use axum::{middleware, Extension};
use database_common_macros::transactional_handler;
use dill::{Catalog, CatalogBuilder};
use http_common::ApiError;
use indoc::indoc;
use internal_error::*;
use kamu::domain::{Protocols, ServerUrlConfig, TenancyConfig};
use kamu_adapter_http::e2e::e2e_router;
use kamu_flow_system_inmem::domain::FlowExecutor;
use kamu_task_system_inmem::domain::TaskExecutor;
use messaging_outbox::OutboxExecutor;
use tokio::sync::Notify;
use url::Url;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

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
        base_catalog: &Catalog,
        cli_catalog: &Catalog,
        tenancy_config: TenancyConfig,
        address: Option<IpAddr>,
        port: Option<u16>,
        external_address: Option<IpAddr>,
        e2e_output_data_path: Option<&PathBuf>,
    ) -> Result<Self, InternalError> {
        // Background task executor must run with server privileges to execute tasks on
        // behalf of the system, as they are automatically scheduled
        let task_executor = cli_catalog.get_one().unwrap();

        let flow_executor = cli_catalog.get_one().unwrap();

        let outbox_executor = cli_catalog.get_one().unwrap();

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

        let api_server_catalog = CatalogBuilder::new_chained(base_catalog)
            .add_value(ServerUrlConfig::new(Protocols {
                base_url_rest,
                base_url_platform: default_protocols.base_url_platform,
                // Note: this is not a valid endpoint in Web UI mode
                base_url_flightsql: default_protocols.base_url_flightsql,
            }))
            .build();

        let mut router = OpenApiRouter::with_openapi(build_openapi().build())
            .route("/", axum::routing::get(root))
            .route(
                // IMPORTANT: The same name is used inside e2e_middleware_fn().
                //            If there is a need to change, please update there too.
                "/graphql",
                axum::routing::get(graphql_playground_handler).post(graphql_handler),
            )
            .routes(routes!(kamu_adapter_http::platform_login_handler))
            .routes(routes!(kamu_adapter_http::platform_token_validate_handler))
            .routes(routes!(
                kamu_adapter_http::platform_file_upload_prepare_post_handler
            ))
            .routes(routes!(
                kamu_adapter_http::platform_file_upload_post_handler,
                kamu_adapter_http::platform_file_upload_get_handler
            ))
            .merge(kamu_adapter_http::data::root_router())
            .merge(kamu_adapter_http::general::root_router())
            .nest(
                "/odata",
                match tenancy_config {
                    TenancyConfig::MultiTenant => kamu_adapter_odata::router_multi_tenant(),
                    TenancyConfig::SingleTenant => kamu_adapter_odata::router_single_tenant(),
                },
            )
            .nest(
                match tenancy_config {
                    TenancyConfig::MultiTenant => "/:account_name/:dataset_name",
                    TenancyConfig::SingleTenant => "/:dataset_name",
                },
                kamu_adapter_http::add_dataset_resolver_layer(
                    OpenApiRouter::new()
                        .merge(kamu_adapter_http::smart_transfer_protocol_router())
                        .merge(kamu_adapter_http::data::dataset_router()),
                    tenancy_config,
                ),
            );

        let is_e2e_testing = e2e_output_data_path.is_some();

        if is_e2e_testing {
            router = router.layer(middleware::from_fn(
                kamu_adapter_http::e2e::e2e_middleware_fn,
            ));
        }

        router = router
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
            );

        let maybe_shutdown_notify = if is_e2e_testing {
            let shutdown_notify = Arc::new(Notify::new());

            router = router.nest("/e2e", e2e_router(shutdown_notify.clone()).into());

            Some(shutdown_notify)
        } else {
            None
        };

        router = router
            .layer(Extension(gql_schema))
            .layer(Extension(api_server_catalog));

        let (router, api) = router.split_for_parts();
        let router = router
            .route("/openapi.json", axum::routing::get(openapi_spec))
            .route("/openapi", axum::routing::get(openapi_playground))
            .layer(Extension(Arc::new(api)));

        let server = axum::serve(listener, router.into_make_service());

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
            <li><a href="/openapi">OpenAPI Playground</li>
        </ul>
        "#
    ))
}

async fn openapi_playground() -> impl axum::response::IntoResponse {
    // NOTE: Keep in sync with kamu-docs repo
    axum::response::Html(indoc!(
        r#"
        <!DOCTYPE html>
        <html>
        <head>
            <title>Kamu REST API Reference</title>
            <meta charset="utf-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1" />
            <style>
                :root {
                    --scalar-custom-header-height: 50px;
                }

                .custom-header {
                    height: var(--scalar-custom-header-height);
                    background-color: var(--scalar-background-1);
                    box-shadow: inset 0 -1px 0 var(--scalar-border-color);
                    color: var(--scalar-color-1);
                    font-size: var(--scalar-font-size-2);
                    padding: 0 18px;
                    position: sticky;
                    justify-content: space-between;
                    top: 0;
                    z-index: 100;
                }

                .custom-header,
                .custom-header nav {
                    display: flex;
                    align-items: center;
                    gap: 18px;
                }

                .custom-header a:hover {
                    color: var(--scalar-color-2);
                }
            </style>
        </head>
        <body>
            <header class="custom-header scalar-app">
            <b>Kamu REST API</b>
            <nav>
                <a href="https://docs.kamu.dev/">Docs</a>
                <a href="https://discord.gg/nU6TXRQNXC">Discord</a>
            </nav>
            </header>

            <script id="api-reference"></script>

            <script>
            var configuration = {
                theme: "purple",
                spec: {
                    url: "/openapi.json",
                },
                servers: [
                    {
                        url: "/",
                        description: "Current Node",
                    },
                ],
            };

            document.getElementById("api-reference").dataset.configuration =
                JSON.stringify(configuration);
            </script>

            <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
        </body>
        </html>
        "#
    ))
}

async fn openapi_spec(
    Extension(api): Extension<Arc<utoipa::openapi::OpenApi>>,
) -> impl axum::response::IntoResponse {
    axum::Json(api)
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
// OpenAPI root
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn build_openapi() -> utoipa::openapi::OpenApiBuilder {
    use utoipa::openapi::security::*;
    use utoipa::openapi::tag::*;
    use utoipa::openapi::*;

    OpenApiBuilder::new()
        .info(
            InfoBuilder::new()
                .title("Kamu REST API")
                .terms_of_service(Some("https://docs.kamu.dev/terms-of-service/"))
                .license(Some(
                    LicenseBuilder::new()
                        .name("BSL")
                        .url(Some("https://docs.kamu.dev/license/"))
                        .build(),
                ))
                .description(Some(indoc::indoc!(
                    r#"
                    You are currently running Kamu CLI in the API server mode. For a fully-featured
                    server consider using [Kamu Node](https://docs.kamu.dev/node/).

                    ## Auth
                    Some operation require an **API token**. Pass `--get-token` command line argument
                    for CLI to generate a token for you.

                    ## Resources
                    - [Documentation](https://docs.kamu.dev)
                    - [Discord](https://discord.gg/nU6TXRQNXC)
                    - [Other protocols](https://docs.kamu.dev/node/protocols/)
                    - [Open Data Fabric specification](https://docs.kamu.dev/odf/)
                    "#
                ))),
        )
        .tags(Some([
            TagBuilder::new()
                .name("odf-core")
                .description(Some(indoc::indoc!(
                    r#"
                    Core ODF APIs.

                    [Open Data Fabric](https://docs.kamu.dev/odf/) (ODF) is a specification for
                    open data formats and protocols for interoperable data exchange and processing.
                    Kamu is just one possible implementation of this spec. APIs in this group are
                    part of the spec and thus should be supported by most ODF implementations.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("odf-transfer")
                .description(Some(indoc::indoc!(
                    r#"
                    ODF Data Transfer APIs.

                    This group includes two main protocols:

                    - [Simple Transfer Protocol](https://docs.kamu.dev/odf/spec/#simple-transfer-protocol)
                    (SiTP) is a bare-minimum read-only protocol used for synchronizing datasets between
                    repositories. It is simple to implement and support, but accesses metadata blocks and
                    other dataset components on individual object basis which may be inefficient for
                    large datasets.

                    - [Smart Transfer Protocol](https://docs.kamu.dev/odf/spec/#smart-transfer-protocol)
                    (SmTP) is an extension of SiTP that adds efficient batch transfer of small objects,
                    compression, and can proxy reads and writes of large objects directly to underlying
                    storage.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("odf-query")
                .description(Some(indoc::indoc!(
                    r#"
                    ODF Data Query APIs.

                    APIs in this group allow to query data and metadata of datasets and generate
                    cryptographic proofs for the results.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("kamu")
                .description(Some(indoc::indoc!(
                    r#"
                    General Node APIs.

                    APIs in this group are either Kamu-specific or are experimental before they
                    are included into ODF spec.
                    "#
                )))
                .build(),
            TagBuilder::new()
                .name("kamu-odata")
                .description(Some(indoc::indoc!(
                    r#"
                    OData Adapter.

                    This group f APIs represente an [OData](https://learn.microsoft.com/en-us/odata/overview)
                    protocol adapter on top of the ODF query API. See
                    [datafusion-odata](https://github.com/kamu-data/datafusion-odata/) library for details
                    on what parts of the OData protocol are currently supported.
                    "#
                )))
                .build(),
        ]))
        .components(Some(
            ComponentsBuilder::new()
                .security_scheme(
                    "api_key",
                    SecurityScheme::Http(
                        HttpBuilder::new()
                            .scheme(HttpAuthScheme::Bearer)
                            .bearer_format("AccessToken")
                            .build(),
                    ),
                )
                .build(),
        ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
