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

use async_utils::BackgroundAgent;
use axum::{Extension, middleware};
use database_common_macros::transactional_handler;
use http_common::ApiError;
use internal_error::*;
use kamu::domain::{FileUploadLimitConfig, Protocols, ServerUrlConfig, TenancyConfig};
use kamu_accounts_services::PasswordPolicyConfig;
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};
use kamu_adapter_http::DatasetAuthorizationLayer;
use kamu_adapter_http::e2e::e2e_router;
use observability::axum::{panic_handler, unknown_fallback_handler};
use tokio::sync::Notify;
use tower_http::catch_panic::CatchPanicLayer;
use url::Url;
use utoipa_axum::router::OpenApiRouter;

use super::{UIConfiguration, UIFeatureFlags};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct APIServer {
    server_future: Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>,
    local_addr: SocketAddr,
    background_agents: Vec<Arc<dyn BackgroundAgent>>,
    api_server_catalog: dill::Catalog,
}

impl APIServer {
    pub async fn new(
        base_catalog: &dill::Catalog,
        cli_catalog: &dill::Catalog,
        tenancy_config: TenancyConfig,
        address: Option<IpAddr>,
        port: Option<u16>,
        file_upload_limit_config: &FileUploadLimitConfig,
        enable_dataset_env_vars_management: bool,
        allow_anonymous: bool,
        external_address: Option<IpAddr>,
        e2e_output_data_path: Option<&PathBuf>,
        password_policy_config: &PasswordPolicyConfig,
    ) -> Result<Self, InternalError> {
        // Background task executor must run with server privileges to execute tasks on
        // behalf of the system, as they are automatically scheduled
        let background_agents = cli_catalog
            .get::<dill::AllOf<dyn BackgroundAgent>>()
            .unwrap();

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST)),
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
        }

        let default_protocols = Protocols::default();

        let api_server_catalog = dill::CatalogBuilder::new_chained(base_catalog)
            .add_value(ServerUrlConfig::new(Protocols {
                base_url_rest,
                base_url_platform: default_protocols.base_url_platform,
                // Note: this is not a valid endpoint in Web UI mode
                base_url_flightsql: default_protocols.base_url_flightsql,
            }))
            .build();

        let ui_configuration = UIConfiguration {
            ingest_upload_file_limit_mb: file_upload_limit_config.max_file_size_in_mb(),
            min_new_password_length: password_policy_config.min_new_password_length,
            feature_flags: UIFeatureFlags {
                enable_logout: true,
                enable_scheduling: true,
                enable_dataset_env_vars_management,
                allow_anonymous,
                enable_terms_of_service: true,
            },
        };

        let graphql_router = OpenApiRouter::new()
            .route("/graphql", axum::routing::post(graphql_handler))
            .layer(graphql_http::middleware::GraphqlTracingLayer::new(
                kamu_adapter_graphql::schema(),
                kamu_adapter_graphql::schema_quiet(),
            ));

        let mut router = OpenApiRouter::with_openapi(
            kamu_adapter_http::openapi::spec_builder(
                crate::app::VERSION,
                indoc::indoc!(
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
                ),
            )
            .build(),
        )
        .merge(server_console::router(
            "Kamu API Server".to_string(),
            format!("v{} embedded", crate::VERSION),
        ).into())
        .merge(kamu_adapter_http::data::root_router())
        .merge(kamu_adapter_http::general::root_router())
        .nest(
            "/odata",
            match tenancy_config {
                TenancyConfig::MultiTenant => kamu_adapter_odata::router_multi_tenant(),
                TenancyConfig::SingleTenant => kamu_adapter_odata::router_single_tenant(),
            },
        )
        .merge(graphql_router)
        .nest(
                match tenancy_config {
                    TenancyConfig::MultiTenant => "/{account_name}/{dataset_name}",
                    TenancyConfig::SingleTenant => "/{dataset_name}",
                },
                kamu_adapter_http::add_dataset_resolver_layer(
                    OpenApiRouter::new()
                        .merge(kamu_adapter_http::data::dataset_router())
                        .merge(kamu_adapter_http::smart_transfer_protocol_router())
                        .layer(DatasetAuthorizationLayer::default()),
                    tenancy_config,
                ),
            );

        if !allow_anonymous {
            router = router.layer(kamu_adapter_http::AuthPolicyLayer::new());
        }

        // All endpoints bellow AuthPolicyLayer are opened for anonymous access
        router = router
            .nest(
                "/platform",
                kamu_adapter_http::platform::root_router(allow_anonymous),
            )
            .route("/ui-config", axum::routing::get(ui_configuration_handler));

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
            .layer(CatchPanicLayer::custom(panic_handler))
            // Note: Healthcheck, metrics, and OpenAPI routes are placed before the tracing layer
            // (layers execute bottom-up) to avoid spam in logs
            .route(
                "/system/health",
                axum::routing::get(observability::health::health_handler),
            )
            .route(
                "/system/metrics",
                axum::routing::get(observability::metrics::metrics_handler),
            )
            .route(
                "/system/info",
                axum::routing::get(observability::build_info::build_info_handler),
            )
            .merge(kamu_adapter_http::openapi::router().into())
            .fallback(unknown_fallback_handler);

        let maybe_shutdown_notify = if is_e2e_testing {
            let shutdown_notify = Arc::new(Notify::new());

            router = router.nest("/e2e", e2e_router(shutdown_notify.clone()).into());

            Some(shutdown_notify)
        } else {
            None
        };

        let (router, api) = router.split_for_parts();
        let router = router
            .layer(Extension(api_server_catalog.clone()))
            .layer(Extension(ui_configuration))
            .layer(Extension(Arc::new(api)));

        let server = axum::serve(listener, router.into_make_service());

        let server_future: Pin<Box<dyn Future<Output = _> + Send>> =
            if let Some(shutdown_notify) = maybe_shutdown_notify {
                Box::pin(async move {
                    server
                        .with_graceful_shutdown(async move {
                            shutdown_notify.notified().await;
                        })
                        .await
                })
            } else {
                Box::pin(server.into_future())
            };

        Ok(Self {
            server_future,
            local_addr,
            background_agents,
            api_server_catalog,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub fn api_server_catalog(&self) -> &dill::Catalog {
        &self.api_server_catalog
    }

    pub async fn run(self) -> Result<(), InternalError> {
        // Start all background agents as tasks
        // Note: Background agents are designed to run forever in event loops.
        // If any agent completes normally, it's considered an unexpected event
        // that should trigger a server shutdown.
        let agent_tasks: Vec<_> = self
            .background_agents
            .into_iter()
            .map(|agent| {
                let agent_name = agent.agent_name().to_string();
                tokio::spawn(async move {
                    tracing::info!("Starting background agent: {}", agent_name);
                    let result = agent.run().await;
                    match &result {
                        Err(error) => {
                            tracing::error!("Background agent {} failed: {}", agent_name, error);
                        }
                        Ok(()) => {
                            tracing::warn!(
                                "Background agent {} completed unexpectedly! This will cause \
                                 server shutdown.",
                                agent_name
                            );
                        }
                    }
                    result
                })
            })
            .collect();

        // Ensure we have background agents registered
        assert!(
            !agent_tasks.is_empty(),
            "No background agents found! This indicates a DI container configuration issue. Make \
             sure all agent implementations are registered as both their specific trait (e.g., \
             TaskAgent) AND as BackgroundAgent in the DI container."
        );

        tokio::select! {
            res = self.server_future => {
                tracing::warn!("HTTP server completed unexpectedly! This will cause server shutdown.");
                res.int_err()
            },
            res = async {
                let (result, _index, _remaining) = futures::future::select_all(agent_tasks).await;
                match result {
                    Ok(agent_result) => agent_result,
                    Err(join_error) => {
                        Err(InternalError::new(format!("Background agent task panicked: {join_error}")))
                    },
                }
            } => {
                res
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Handlers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn ui_configuration_handler(
    ui_configuration: Extension<UIConfiguration>,
) -> axum::Json<UIConfiguration> {
    axum::Json(ui_configuration.0)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
async fn graphql_handler(
    Extension(schema): Extension<kamu_adapter_graphql::Schema>,
    Extension(catalog): Extension<dill::Catalog>,
    req: async_graphql_axum::GraphQLRequest,
) -> Result<async_graphql_axum::GraphQLResponse, ApiError> {
    let graphql_request = req
        .into_inner()
        .data(account_entity_data_loader(&catalog))
        .data(dataset_handle_data_loader(&catalog))
        .data(catalog);
    let graphql_response = schema.execute(graphql_request).await.into();

    Ok(graphql_response)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
