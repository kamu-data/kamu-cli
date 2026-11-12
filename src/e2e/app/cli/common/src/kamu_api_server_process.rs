// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::time::Duration;

use container_runtime::{Terminate, TerminateStatus};
use headers::Header as _;
use kamu_adapter_http::OdfSmtpVersion;
use kamu_adapter_http::smart_protocol::messages::SMART_TRANSFER_PROTOCOL_VERSION;
use kamu_cli_puppet::KamuCliPuppet;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::{KamuApiServerClient, KamuApiServerClientExt, get_server_api_base_url};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `kamu system api-server` running as a child process of the test, for
/// scenarios that control the process itself, such as sending it signals
pub struct KamuApiServerProcess {
    child: tokio::process::Child,
    client: KamuApiServerClient,
}

impl KamuApiServerProcess {
    /// Starts the server in the puppet's workspace and waits until it serves
    pub async fn start(kamu: &KamuCliPuppet) -> Self {
        let child = kamu.spawn_api_server();
        let base_url = get_server_api_base_url(kamu.get_e2e_output_data_path())
            .await
            .unwrap();
        let client = KamuApiServerClient::new(base_url, kamu.workspace_path().to_path_buf());
        client.e2e().ready().await.unwrap();

        Self { child, client }
    }

    /// Opens a smart transfer protocol push session to the dataset as
    /// `e2e-user` and sends nothing on it, so the server's side waits for the
    /// client
    pub async fn open_idle_push_session(
        &mut self,
        dataset_alias: &odf::DatasetAlias,
    ) -> WebSocketStream<MaybeTlsStream<TcpStream>> {
        let token = self.client.auth().login_as_e2e_user().await;

        let mut url = self
            .client
            .get_base_url()
            .join(&format!("{dataset_alias}/push"))
            .unwrap();
        url.set_scheme("ws").unwrap();

        let mut request = url.as_str().into_client_request().unwrap();
        request.headers_mut().insert(
            OdfSmtpVersion::name(),
            http::HeaderValue::from(SMART_TRANSFER_PROTOCOL_VERSION),
        );
        request.headers_mut().insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );

        let (socket, _) = connect_async(request).await.unwrap();
        socket
    }

    /// Sends SIGTERM and waits up to `timeout` for the process to exit, then
    /// kills it. Returns how it ended
    pub async fn terminate(&mut self, timeout: Duration) -> TerminateStatus {
        self.child.terminate(timeout).await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
