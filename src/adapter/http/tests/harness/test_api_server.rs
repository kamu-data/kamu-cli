// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use dill::Catalog;
use opendatafabric::{DatasetAlias, DatasetName};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TestAPIServer {
    server: axum::Server<
        hyper::server::conn::AddrIncoming,
        axum::routing::IntoMakeService<axum::Router>,
    >,
}

impl TestAPIServer {
    pub fn new(catalog: Catalog, address: Option<IpAddr>, port: Option<u16>) -> Self {
        use axum::extract::Path;

        #[derive(serde::Deserialize)]
        struct DatasetByName {
            dataset_name: DatasetName,
        }

        let app = axum::Router::new()
            .nest(
                "/:dataset_name",
                kamu_adapter_http::smart_transfer_protocol_routes()
                    .layer(kamu_adapter_http::DatasetAuthorizationLayer::new())
                    .layer(kamu_adapter_http::DatasetResolverLayer::new(
                        |Path(p): Path<DatasetByName>| {
                            DatasetAlias::new(None, p.dataset_name).as_local_ref()
                        },
                    ))
                    .layer(axum::extract::Extension(catalog)),
            )
            .layer(
                tower::ServiceBuilder::new().layer(
                    tower_http::cors::CorsLayer::new()
                        .allow_origin(tower_http::cors::Any)
                        .allow_methods(vec![http::Method::GET, http::Method::POST])
                        .allow_headers(tower_http::cors::Any),
                ),
            );

        let addr = SocketAddr::from((
            address.unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
            port.unwrap_or(0),
        ));

        let server = axum::Server::bind(&addr).serve(app.into_make_service());

        Self { server }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub async fn run(self) -> Result<(), hyper::Error> {
        self.server.await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
