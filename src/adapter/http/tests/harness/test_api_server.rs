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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestAPIServer {
    server: axum::serve::Serve<axum::routing::IntoMakeService<axum::Router>, axum::Router>,
    local_addr: SocketAddr,
}

impl TestAPIServer {
    pub fn new(catalog: Catalog, listener: tokio::net::TcpListener, multi_tenant: bool) -> Self {
        let app = axum::Router::new()
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
            .nest("/", kamu_adapter_http::general::root_router())
            .nest(
                if multi_tenant {
                    "/:account_name/:dataset_name"
                } else {
                    "/:dataset_name"
                },
                kamu_adapter_http::add_dataset_resolver_layer(
                    axum::Router::new()
                        .nest("/", kamu_adapter_http::smart_transfer_protocol_router())
                        .nest("/", kamu_adapter_http::data::dataset_router()),
                    multi_tenant,
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
            );

        let local_addr = listener.local_addr().unwrap();

        let server = axum::serve(listener, app.into_make_service());

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
