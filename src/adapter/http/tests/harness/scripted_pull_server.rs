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

use axum::extract::Path;
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::routing::get;
use kamu_adapter_http::smart_protocol::axum_server_protocol_common::{
    axum_read_payload,
    axum_write_payload,
};
use kamu_adapter_http::smart_protocol::messages::*;
use kamu_adapter_http::smart_protocol::protocol_dataset_helper::prepare_dataset_metadata_batch;
use kamu_datasets::{DatasetRegistry, ResolvedDataset};
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Answers one smart pull of a dataset by script: it serves the dataset's
/// `HEAD` and blocks and sends its whole chain, but no usable transfer
/// strategy for the objects the client asks for
pub(crate) struct ScriptedPullServer {
    listener: tokio::net::TcpListener,
    local_addr: SocketAddr,
    dataset: ResolvedDataset,
    session: ScriptedPullSession,
}

/// How the server answers the request for objects
#[derive(Clone, Copy)]
enum PullScript {
    /// Offers none of them
    OmitObjects,

    /// Offers each of them as the other object type, with a download URL
    /// the client must never reach
    MislabelObjects,
}

impl ScriptedPullServer {
    pub async fn omitting_objects(
        dataset_registry: &dyn DatasetRegistry,
        dataset_handle: &odf::DatasetHandle,
    ) -> Self {
        Self::new(dataset_registry, dataset_handle, PullScript::OmitObjects).await
    }

    pub async fn mislabeling_objects(
        dataset_registry: &dyn DatasetRegistry,
        dataset_handle: &odf::DatasetHandle,
    ) -> Self {
        Self::new(
            dataset_registry,
            dataset_handle,
            PullScript::MislabelObjects,
        )
        .await
    }

    async fn new(
        dataset_registry: &dyn DatasetRegistry,
        dataset_handle: &odf::DatasetHandle,
        script: PullScript,
    ) -> Self {
        let dataset = dataset_registry.get_dataset_by_handle(dataset_handle).await;
        let head = Self::head(&dataset).await;
        let blocks =
            prepare_dataset_metadata_batch(dataset.as_metadata_chain(), &head, None, false)
                .await
                .unwrap();

        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let local_addr = listener.local_addr().unwrap();
        Self {
            listener,
            local_addr,
            dataset,
            session: ScriptedPullSession {
                script,
                blocks,
                unreachable_url: Url::parse(&format!("http://{local_addr}/unreachable")).unwrap(),
            },
        }
    }

    pub fn dataset_url(&self, dataset_name: &str) -> Url {
        Url::parse(&format!("odf+http://{}/{dataset_name}", self.local_addr)).unwrap()
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        let session = self.session;
        let dataset = self.dataset;
        let blocks_dataset = dataset.clone();
        let router = axum::Router::new()
            .route(
                "/{dataset_name}/pull",
                get(move |ws: WebSocketUpgrade| {
                    let session = session.clone();
                    async move { ws.on_upgrade(|socket| session.run(socket)) }
                }),
            )
            .route(
                "/{dataset_name}/refs/head",
                get(move || async move { Self::head(&dataset).await.to_string() }),
            )
            .route(
                "/{dataset_name}/blocks/{block_hash}",
                get(
                    move |Path((_, block_hash)): Path<(String, odf::Multihash)>| async move {
                        blocks_dataset
                            .as_metadata_chain()
                            .get_block_bytes(&block_hash)
                            .await
                            .unwrap()
                            .to_vec()
                    },
                ),
            );
        axum::serve(self.listener, router.into_make_service())
            .into_future()
            .await
    }

    async fn head(dataset: &ResolvedDataset) -> odf::Multihash {
        dataset
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct ScriptedPullSession {
    script: PullScript,
    blocks: MetadataBlocksBatch,
    unreachable_url: Url,
}

impl ScriptedPullSession {
    async fn run(self, mut socket: WebSocket) {
        Self::receive::<DatasetPullRequest>(&mut socket).await;
        Self::send::<DatasetPullResponse>(
            &mut socket,
            Ok(DatasetPullSuccessResponse {
                transfer_plan: TransferPlan {
                    num_blocks: self.blocks.num_blocks,
                    num_objects: 0,
                    num_records: 0,
                    bytes_in_raw_blocks: 0,
                    bytes_in_raw_objects: 0,
                },
            }),
        )
        .await;

        Self::receive::<DatasetPullMetadataRequest>(&mut socket).await;
        Self::send(
            &mut socket,
            DatasetMetadataPullResponse {
                blocks: self.blocks.clone(),
            },
        )
        .await;

        let objects_request = Self::receive::<DatasetPullObjectsTransferRequest>(&mut socket).await;
        let object_transfer_strategies = match self.script {
            PullScript::OmitObjects => Vec::new(),
            PullScript::MislabelObjects => objects_request
                .object_files
                .into_iter()
                .map(|object_file| self.mislabeled_strategy(object_file))
                .collect(),
        };
        Self::send(
            &mut socket,
            DatasetPullObjectsTransferResponse {
                object_transfer_strategies,
            },
        )
        .await;
    }

    fn mislabeled_strategy(&self, object_file: ObjectFileReference) -> PullObjectTransferStrategy {
        let object_type = match object_file.object_type {
            ObjectType::DataSlice => ObjectType::Checkpoint,
            ObjectType::Checkpoint => ObjectType::DataSlice,
        };
        PullObjectTransferStrategy {
            object_file: ObjectFileReference {
                object_type,
                ..object_file
            },
            pull_strategy: ObjectPullStrategy::HttpDownload,
            download_from: TransferUrl {
                url: self.unreachable_url.clone(),
                headers: Vec::new(),
                expires_at: None,
            },
        }
    }

    async fn receive<T: DeserializeOwned>(socket: &mut WebSocket) -> T {
        axum_read_payload::<T>(socket).await.unwrap()
    }

    async fn send<T: Serialize>(socket: &mut WebSocket, payload: T) {
        axum_write_payload(socket, payload).await.unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
