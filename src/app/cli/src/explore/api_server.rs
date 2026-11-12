// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::future::{Future, IntoFuture, pending};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use axum::{Extension, middleware};
use background_agent_host::{BackgroundAgentHost, BackgroundAgentHostConfig, CancellationToken};
use container_runtime::signal::graceful_stop;
use internal_error::*;
use kamu::domain::{FileUploadLimitConfig, Protocols, ServerUrlConfig, TenancyConfig};
use kamu_accounts_services::PasswordPolicyConfig;
use kamu_adapter_http::DatasetAuthorizationLayer;
use kamu_adapter_http::e2e::e2e_router;
use kamu_datasets::VersionedFilesConfig;
use kamu_webhooks::WebhooksConfig;
use observability::axum::{panic_handler, unknown_fallback_handler};
use tokio::sync::Notify;
use tokio::time::timeout;
use tower_http::catch_panic::CatchPanicLayer;
use url::Url;
use utoipa_axum::router::OpenApiRouter;

use super::{
    UIConfiguration,
    UIFeatureFlags,
    UIVersionedFilesConfiguration,
    UIWebhooksConfiguration,
    graphql_handler,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct APIServer {
    server_future: Pin<Box<dyn Future<Output = Result<(), std::io::Error>> + Send>>,
    local_addr: SocketAddr,
    background_agent_host: Arc<BackgroundAgentHost>,
    background_agent_host_config: Arc<BackgroundAgentHostConfig>,
    shutdown: CancellationToken,
    e2e_shutdown_notify: Option<Arc<Notify>>,
    api_server_catalog: dill::Catalog,
}

impl APIServer {
    pub async fn new(
        base_catalog: &dill::Catalog,
        background_catalog: &dill::Catalog,
        tenancy_config: TenancyConfig,
        address: Option<IpAddr>,
        port: Option<u16>,
        file_upload_limit_config: &FileUploadLimitConfig,
        versioned_files_config: &VersionedFilesConfig,
        enable_dataset_env_vars_management: bool,
        allow_anonymous: bool,
        external_address: Option<IpAddr>,
        e2e_output_data_path: Option<&PathBuf>,
        password_policy_config: &PasswordPolicyConfig,
        webhooks_config: &WebhooksConfig,
    ) -> Result<Self, InternalError> {
        // Agents run as the node: they act for the system, not for whoever started the server
        let background_agent_host = background_catalog.get_one::<BackgroundAgentHost>().unwrap();
        let background_agent_host_config = background_catalog
            .get_one::<BackgroundAgentHostConfig>()
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
            webhooks: UIWebhooksConfiguration {
                loopback_targets_only: webhooks_config.loopback_targets_only,
            },
            versioned_files: UIVersionedFilesConfiguration {
                max_in_band_content_size_in_bytes: versioned_files_config
                    .max_in_band_content_size_in_bytes,
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
        )
        .route(
            "/system/probe",
            axum::routing::get(kamu_adapter_http::system::probe_handler),
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
            // Must stay inside the CORS layer: otherwise the 500 response lacks CORS headers and
            // browsers report it as a network failure
            .layer(CatchPanicLayer::custom(panic_handler))
            .layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![http::Method::GET, http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            )
            .layer(observability::axum::http_layer())
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

        let e2e_shutdown_notify = if is_e2e_testing {
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

        // One token stops both the HTTP server and the agents
        let shutdown = CancellationToken::new();
        let server_future: Pin<Box<dyn Future<Output = _> + Send>> = Box::pin(
            axum::serve(listener, router.into_make_service())
                .with_graceful_shutdown(shutdown.clone().cancelled_owned())
                .into_future(),
        );

        Ok(Self {
            server_future,
            local_addr,
            background_agent_host,
            background_agent_host_config,
            shutdown,
            e2e_shutdown_notify,
            api_server_catalog,
        })
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub fn api_server_catalog(&self) -> &dill::Catalog {
        &self.api_server_catalog
    }

    /// Serves until Ctrl+C, SIGTERM or the e2e shutdown route, or until a
    /// background agent fails fatally. On a shutdown, in-flight requests and
    /// the agents' current steps get the agents' shutdown timeout to finish
    pub async fn run(self) -> Result<(), InternalError> {
        let Self {
            mut server_future,
            background_agent_host,
            background_agent_host_config,
            shutdown,
            e2e_shutdown_notify,
            ..
        } = self;
        let shutdown_timeout = background_agent_host_config.shutdown_timeout;

        let shutdown_requests =
            Self::cancel_on_shutdown_request(shutdown.clone(), e2e_shutdown_notify);

        let server = async {
            let result = tokio::select! {
                result = &mut server_future => {
                    tracing::warn!("HTTP server completed unexpectedly, shutting down");
                    shutdown.cancel();
                    result
                }
                () = shutdown.cancelled() => {
                    match timeout(shutdown_timeout, &mut server_future).await {
                        Ok(result) => result,
                        Err(_) => {
                            tracing::warn!(
                                "HTTP grace period expired, remaining requests end with the process"
                            );
                            Ok(())
                        }
                    }
                }
            };
            result.int_err()
        };

        let (server_result, agents_result, ()) = tokio::join!(
            server,
            background_agent_host.run(shutdown.clone()),
            shutdown_requests,
        );

        agents_result?;
        server_result
    }

    /// Cancels `shutdown` on Ctrl+C, SIGTERM or the e2e shutdown route;
    /// returns as soon as it is cancelled for any reason
    async fn cancel_on_shutdown_request(
        shutdown: CancellationToken,
        e2e_shutdown_notify: Option<Arc<Notify>>,
    ) {
        // Without signals the other sources still work
        let signal = async {
            if let Err(e) = graceful_stop().await {
                tracing::warn!(error = %e, "Failed to listen for shutdown signals");
                pending::<()>().await;
            }
        };
        let e2e_shutdown = async {
            match e2e_shutdown_notify {
                Some(notify) => notify.notified().await,
                None => pending().await,
            }
        };

        tokio::select! {
            () = shutdown.cancelled() => return,
            () = signal => tracing::info!("Shutdown signal received"),
            () = e2e_shutdown => tracing::info!("Shutdown requested through the e2e route"),
        }
        shutdown.cancel();
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
