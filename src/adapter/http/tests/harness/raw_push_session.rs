// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use futures::{SinkExt, StreamExt};
use kamu_adapter_http::smart_protocol::messages::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A push driven one message at a time, so a test decides when the client
/// side answers
pub(crate) struct RawPushSession {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
}

impl RawPushSession {
    pub fn new(socket: WebSocketStream<MaybeTlsStream<TcpStream>>) -> Self {
        Self { socket }
    }

    /// Sends the push request and the metadata, then an objects request with
    /// no objects, and checks the server accepts each
    pub async fn begin_metadata_only_push(
        &mut self,
        request: DatasetPushRequest,
        metadata: DatasetPushMetadataRequest,
    ) {
        self.send(&request).await;
        assert_matches!(
            self.receive::<DatasetPushResponse>().await,
            Ok(DatasetPushRequestAccepted {})
        );

        self.send(&metadata).await;
        assert_matches!(
            self.receive::<DatasetPushMetadataResponse>().await,
            Ok(DatasetPushMetadataAccepted {})
        );

        self.send(&DatasetPushObjectsTransferRequest {
            object_files: Vec::new(),
            is_truncated: false,
        })
        .await;
        assert_matches!(
            self.receive::<DatasetPushObjectsTransferResponse>().await,
            Ok(DatasetPushObjectsTransferAccepted {
                object_transfer_strategies
            }) if object_transfer_strategies.is_empty()
        );
    }

    pub async fn next_progress_request(&mut self) -> DatasetPushObjectsUploadProgressRequest {
        self.receive().await
    }

    pub async fn report_progress(&mut self, details: ObjectsUploadProgressDetails) {
        self.send(&DatasetPushObjectsUploadProgressResponse { details })
            .await;
    }

    /// Completes the push and returns the server's reply, skipping progress
    /// requests the server sent before it read that the upload ended
    pub async fn complete(&mut self) -> DatasetPushCompleteResponse {
        self.send(&DatasetPushComplete {}).await;
        loop {
            match self.receive::<DatasetPushCompleteReply>().await {
                DatasetPushCompleteReply::StaleProgressRequest(_) => {}
                DatasetPushCompleteReply::Response(response) => return response,
                DatasetPushCompleteReply::Legacy(reply) => {
                    panic!("The server must send a typed reply, got {reply:?}")
                }
            }
        }
    }

    async fn send<T: Serialize>(&mut self, payload: &T) {
        self.socket
            .send(Message::Text(
                serde_json::to_string(payload).unwrap().into(),
            ))
            .await
            .unwrap();
    }

    async fn receive<T: DeserializeOwned>(&mut self) -> T {
        let message = self.socket.next().await;
        let Some(Ok(Message::Text(message))) = message else {
            panic!("The server must reply with a message, got {message:?}");
        };
        serde_json::from_str(&message).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
