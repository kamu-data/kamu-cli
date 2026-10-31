// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use kamu_adapter_http::smart_protocol::messages::*;

use super::RawProtocolSocket;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A push driven one message at a time, so a test decides when the client
/// side answers
pub(crate) struct RawPushSession {
    socket: RawProtocolSocket,
}

impl RawPushSession {
    pub fn new(socket: RawProtocolSocket) -> Self {
        Self { socket }
    }

    /// Sends the push request and the metadata, then an objects request with
    /// no objects, and checks the server accepts each
    pub async fn begin_metadata_only_push(
        &mut self,
        request: DatasetPushRequest,
        metadata: DatasetPushMetadataRequest,
    ) {
        self.socket.send(&request).await;
        assert_matches!(
            self.socket.receive::<DatasetPushResponse>().await,
            Ok(DatasetPushRequestAccepted {})
        );

        self.socket.send(&metadata).await;
        assert_matches!(
            self.socket.receive::<DatasetPushMetadataResponse>().await,
            Ok(DatasetPushMetadataAccepted {})
        );

        self.socket
            .send(&DatasetPushObjectsTransferRequest {
                object_files: Vec::new(),
                is_truncated: false,
            })
            .await;
        assert_matches!(
            self.socket.receive::<DatasetPushObjectsTransferResponse>().await,
            Ok(DatasetPushObjectsTransferAccepted {
                object_transfer_strategies
            }) if object_transfer_strategies.is_empty()
        );
    }

    pub async fn next_progress_request(&mut self) -> DatasetPushObjectsUploadProgressRequest {
        self.socket.receive().await
    }

    pub async fn report_progress(&mut self, details: ObjectsUploadProgressDetails) {
        self.socket
            .send(&DatasetPushObjectsUploadProgressResponse { details })
            .await;
    }

    /// Completes the push and returns the server's reply, skipping progress
    /// requests the server sent before it read that the upload ended
    pub async fn complete(&mut self) -> DatasetPushCompleteResponse {
        self.socket.send(&DatasetPushComplete {}).await;
        loop {
            match self.socket.receive::<DatasetPushCompleteReply>().await {
                DatasetPushCompleteReply::StaleProgressRequest(_) => {}
                DatasetPushCompleteReply::Response(response) => return response,
                DatasetPushCompleteReply::Legacy(reply) => {
                    panic!("The server must send a typed reply, got {reply:?}")
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
