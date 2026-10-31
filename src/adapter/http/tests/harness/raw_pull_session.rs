// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_http::smart_protocol::messages::*;
use kamu_adapter_http::smart_protocol::protocol_dataset_helper::decode_metadata_batch;

use super::RawProtocolSocket;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A pull driven one phase at a time, so a test decides what happens on the
/// server between them
pub(crate) struct RawPullSession {
    socket: RawProtocolSocket,
}

impl RawPullSession {
    pub fn new(socket: RawProtocolSocket) -> Self {
        Self { socket }
    }

    /// Sends the pull request and returns the plan the server accepted it with
    pub async fn plan_pull(&mut self, request: DatasetPullRequest) -> TransferPlan {
        self.socket.send(&request).await;
        match self.socket.receive::<DatasetPullResponse>().await {
            Ok(DatasetPullSuccessResponse { transfer_plan }) => transfer_plan,
            Err(e) => panic!("The server must accept the pull request, got {e:?}"),
        }
    }

    pub async fn request_metadata(&mut self) -> ReceivedMetadata {
        self.socket.send(&DatasetPullMetadataRequest {}).await;
        let DatasetMetadataPullResponse { blocks } = self.socket.receive().await;
        let block_hashes = decode_metadata_batch(&blocks)
            .unwrap()
            .into_iter()
            .map(|(hash, _, _)| hash)
            .collect();
        ReceivedMetadata {
            num_blocks: blocks.num_blocks,
            block_hashes,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A metadata batch as the server sent it, with its blocks oldest first
pub(crate) struct ReceivedMetadata {
    pub num_blocks: u32,
    pub block_hashes: Vec<odf::Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
