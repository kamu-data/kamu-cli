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
use std::path::Path;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct HttpFileServer {
    server_future: Box<dyn std::future::Future<Output = Result<(), std::io::Error>> + Unpin + Send>,
    local_addr: SocketAddr,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl HttpFileServer {
    pub async fn new<P>(path: P) -> Self
    where
        P: AsRef<Path>,
    {
        let addr = SocketAddr::from((IpAddr::V4(Ipv4Addr::LOCALHOST), 0));
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .expect("Error binding TCP listener");
        let local_addr = listener.local_addr().unwrap();

        let app = axum::Router::new()
            .route(
                "/{*path}",
                axum::routing::get_service(tower_http::services::ServeDir::new(path)),
            )
            .layer(
                tower::ServiceBuilder::new().layer(tower_http::trace::TraceLayer::new_for_http()),
            );

        let server_future = Box::new(axum::serve(listener, app.into_make_service()).into_future());

        Self {
            server_future,
            local_addr,
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        self.server_future.await
    }
}
