// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use dill::Catalog;
use kamu_core::TenancyConfig;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestAPIServer {
    server: axum::serve::Serve<axum::routing::IntoMakeService<axum::Router>, axum::Router>,
    local_addr: SocketAddr,
}

impl TestAPIServer {
    pub fn new(
        catalog: Catalog,
        listener: tokio::net::TcpListener,
        tenancy_config: TenancyConfig,
    ) -> Self {
        let (router, _api) = OpenApiRouter::new()
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
            )
            .layer(
                tower::ServiceBuilder::new()
                    .layer(
                        tower_http::cors::CorsLayer::new()
                            .allow_origin(tower_http::cors::Any)
                            .allow_methods(vec![http::Method::GET, http::Method::POST])
                            .allow_headers(tower_http::cors::Any),
                    )
                    .layer(axum::extract::Extension(catalog))
                    .layer(kamu_adapter_http::AuthenticationLayer::new()),
            )
            .split_for_parts();

        let local_addr = listener.local_addr().unwrap();

        let server = axum::serve(listener, router.into_make_service());

        Self { server, local_addr }
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        self.server.await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
