// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::{SinkExt, StreamExt};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A smart protocol socket that sends and receives messages as they are,
/// without the client's own checks
pub(crate) struct RawProtocolSocket {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl RawProtocolSocket {
    pub fn new(socket: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { socket }
    }

    pub async fn send<T: Serialize>(&mut self, payload: &T) {
        self.socket
            .send(Message::Text(
                serde_json::to_string(payload).unwrap().into(),
            ))
            .await
            .unwrap();
    }

    /// Receives the next message, which must be a text message of type `T`
    pub async fn receive<T: DeserializeOwned>(&mut self) -> T {
        let message = self.socket.next().await;
        let Some(Ok(Message::Text(message))) = message else {
            panic!("The server must reply with a message, got {message:?}");
        };
        serde_json::from_str(&message).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
