// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;

use hyper::server::conn::AddrIncoming;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct HttpFileServer {
    server: axum::Server<AddrIncoming, axum::routing::IntoMakeService<axum::Router>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl HttpFileServer {
    pub fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));

        let bound_addr = AddrIncoming::bind(&addr).unwrap_or_else(|e| {
            panic!("error binding to {}: {}", addr, e);
        });

        let app = axum::Router::new()
            .route(
                "/*path",
                axum::routing::get_service(tower_http::services::ServeDir::new(path)),
            )
            .layer(
                tower::ServiceBuilder::new().layer(tower_http::trace::TraceLayer::new_for_http()),
            );

        let server = axum::Server::builder(bound_addr).serve(app.into_make_service());

        Self { server }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.server.local_addr()
    }

    pub async fn run(self) -> Result<(), hyper::Error> {
        self.server.await
    }
}
