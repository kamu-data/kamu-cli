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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct HttpFileServer {
    server: axum::serve::Serve<axum::routing::IntoMakeService<axum::Router>, axum::Router>,
    local_addr: SocketAddr,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl HttpFileServer {
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0));
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("Error binding TCP listener");
        let local_addr = listener.local_addr().unwrap();

        let app = axum::Router::new()
            .route(
                "/*path",
                axum::routing::get_service(tower_http::services::ServeDir::new(path)),
            )
            .layer(
                tower::ServiceBuilder::new().layer(tower_http::trace::TraceLayer::new_for_http()),
            );

        let server = axum::serve(listener, app.into_make_service());

        Self { server, local_addr }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        self.server.await
    }
}
