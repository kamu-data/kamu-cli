// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::IntoFuture;
use std::net::SocketAddr;

use axum::extract::ws::WebSocket;
use kamu_adapter_http::smart_protocol::ErrorReplyClose;
use kamu_adapter_http::smart_protocol::axum_server_protocol_common::{
    axum_read_payload,
    axum_write_error_close,
    axum_write_payload,
};
use kamu_adapter_http::smart_protocol::messages::DatasetPushCompleteError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The local port a scripted smart-protocol server answers on
pub(crate) struct ScriptedServerListener {
    listener: tokio::net::TcpListener,
    local_addr: SocketAddr,
}

impl ScriptedServerListener {
    pub async fn bind() -> Self {
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let local_addr = listener.local_addr().unwrap();
        Self {
            listener,
            local_addr,
        }
    }

    pub fn dataset_url(&self, dataset_name: &str) -> Url {
        Url::parse(&format!("odf+http://{}/{dataset_name}", self.local_addr)).unwrap()
    }

    /// `path` is relative to the server's root
    pub fn http_url(&self, path: &str) -> Url {
        Url::parse(&format!("http://{}/{path}", self.local_addr)).unwrap()
    }

    pub async fn serve(self, router: axum::Router) -> Result<(), std::io::Error> {
        axum::serve(self.listener, router.into_make_service())
            .into_future()
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The server end of a scripted session; a message that cannot be read or
/// written fails the test
pub(crate) struct ScriptedSocket {
    socket: WebSocket,
}

impl ScriptedSocket {
    pub fn new(socket: WebSocket) -> Self {
        Self { socket }
    }

    pub async fn receive<T: DeserializeOwned>(&mut self) -> T {
        axum_read_payload::<T>(&mut self.socket).await.unwrap()
    }

    pub async fn send<T: Serialize>(&mut self, payload: T) {
        axum_write_payload(&mut self.socket, payload).await.unwrap();
    }

    /// Fails the push completion as a real server does
    pub async fn close_push_not_completed(&mut self, error: DatasetPushCompleteError) {
        axum_write_error_close(&mut self.socket, ErrorReplyClose::push_not_completed(error)).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
