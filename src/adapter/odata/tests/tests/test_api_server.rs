// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use dill::Catalog;
use kamu_core::TenancyConfig;
use utoipa_axum::router::OpenApiRouter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TestAPIServer {
    server_future: Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Unpin>,
    local_addr: SocketAddr,
}

impl TestAPIServer {
    pub async fn new(
        catalog: Catalog,
        address: Option<IpAddr>,
        port: Option<u16>,
        tenancy_config: TenancyConfig,
    ) -> Self {
        let (router, _api) = OpenApiRouter::new()
            .nest(
                "/odata",
                match tenancy_config {
                    TenancyConfig::MultiTenant => kamu_adapter_odata::router_multi_tenant(),
                    TenancyConfig::SingleTenant => kamu_adapter_odata::router_single_tenant(),
                },
            )
            .layer(
                tower_http::cors::CorsLayer::new()
                    .allow_origin(tower_http::cors::Any)
                    .allow_methods(vec![http::Method::GET, http::Method::POST])
                    .allow_headers(tower_http::cors::Any),
            )
            .layer(axum::extract::Extension(catalog))
            .split_for_parts();

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));

        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let local_addr = listener.local_addr().unwrap();
        let server_future =
            Box::new(axum::serve(listener, router.into_make_service()).into_future());

        Self {
            server_future,
            local_addr,
        }
    }

    pub fn local_addr(&self) -> &SocketAddr {
        &self.local_addr
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        self.server_future.await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
