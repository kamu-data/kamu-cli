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
use std::sync::{Arc, Mutex};

use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::routing::{get, put};
use kamu_adapter_http::smart_protocol::axum_server_protocol_common::axum_read_payload;
use kamu_adapter_http::smart_protocol::messages::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PUSH_NOT_COMPLETED_CLOSE_CODE: u16 = 1013;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Answers one smart push by script and stores nothing. It has no route for
/// `refs`, so the client finds no `HEAD` and pushes its whole chain
pub(crate) struct ScriptedPushServer {
    listener: tokio::net::TcpListener,
    local_addr: SocketAddr,
    script: PushScript,
    state: Arc<ScriptState>,
}

/// What the server does once the client asked to transfer objects
#[derive(Clone, Copy)]
enum PushScript {
    /// Skips every upload. Right after the client reports the upload complete
    /// it sends one more progress request, as a real server does when its
    /// timer fires just before that report arrives, then fails the completion
    /// with `UpdatedConcurrently`. A client that read the stale request as
    /// the reply would report success
    StaleProgressRequest,

    /// Takes every object by HTTP upload, but holds each upload until the
    /// client answered one progress request, then confirms the completion
    ProgressDuringUpload,
}

#[derive(Default)]
struct ScriptState {
    progress_reports: Mutex<Vec<ObjectsUploadProgressDetails>>,
    uploads_released: tokio::sync::watch::Sender<bool>,
}

impl ScriptedPushServer {
    pub async fn stale_progress_request() -> Self {
        Self::new(PushScript::StaleProgressRequest).await
    }

    pub async fn progress_during_upload() -> Self {
        Self::new(PushScript::ProgressDuringUpload).await
    }

    async fn new(script: PushScript) -> Self {
        let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let local_addr = listener.local_addr().unwrap();
        Self {
            listener,
            local_addr,
            script,
            state: Arc::new(ScriptState::default()),
        }
    }

    pub fn dataset_url(&self, dataset_name: &str) -> Url {
        Url::parse(&format!("odf+http://{}/{dataset_name}", self.local_addr)).unwrap()
    }

    /// Taken before `run`, which consumes the server
    pub fn progress_reports(&self) -> ProgressReports {
        ProgressReports {
            state: self.state.clone(),
        }
    }

    pub async fn run(self) -> Result<(), std::io::Error> {
        let session = ScriptedPushSession {
            script: self.script,
            state: self.state.clone(),
            uploads_base_url: Url::parse(&format!("http://{}/uploads/", self.local_addr)).unwrap(),
        };
        let uploads_released = self.state.uploads_released.subscribe();

        let router = axum::Router::new()
            .route(
                "/{dataset_name}/push",
                get(move |ws: WebSocketUpgrade| {
                    let session = session.clone();
                    async move { ws.on_upgrade(|socket| session.run(socket)) }
                }),
            )
            .route(
                "/uploads/{object}",
                put(move |_body: axum::body::Bytes| {
                    let mut uploads_released = uploads_released.clone();
                    async move {
                        uploads_released
                            .wait_for(|released| *released)
                            .await
                            .unwrap();
                    }
                }),
            );
        axum::serve(self.listener, router.into_make_service())
            .into_future()
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The upload progress the client reported, in order
pub(crate) struct ProgressReports {
    state: Arc<ScriptState>,
}

impl ProgressReports {
    pub fn get(&self) -> Vec<ObjectsUploadProgressDetails> {
        self.state.progress_reports.lock().unwrap().clone()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct ScriptedPushSession {
    script: PushScript,
    state: Arc<ScriptState>,
    uploads_base_url: Url,
}

impl ScriptedPushSession {
    async fn run(self, mut socket: WebSocket) {
        Self::receive::<DatasetPushRequest>(&mut socket).await;
        Self::send::<DatasetPushResponse>(&mut socket, Ok(DatasetPushRequestAccepted {})).await;

        Self::receive::<DatasetPushMetadataRequest>(&mut socket).await;
        Self::send(
            &mut socket,
            DatasetPushMetadataResponse::Ok(DatasetPushMetadataAccepted {}),
        )
        .await;

        let objects_request = Self::receive::<DatasetPushObjectsTransferRequest>(&mut socket).await;

        match self.script {
            PushScript::StaleProgressRequest => {
                Self::send::<DatasetPushObjectsTransferResponse>(
                    &mut socket,
                    Ok(DatasetPushObjectsTransferAccepted {
                        object_transfer_strategies: Vec::new(),
                    }),
                )
                .await;

                self.receive_progress(&mut socket).await;
                Self::send_progress_request(&mut socket).await;

                Self::receive::<DatasetPushComplete>(&mut socket).await;
                Self::close_with_completion_error(
                    &mut socket,
                    DatasetPushCompleteError::UpdatedConcurrently(
                        DatasetPushUpdatedConcurrentlyError {},
                    ),
                )
                .await;
            }
            PushScript::ProgressDuringUpload => {
                assert!(
                    !objects_request.object_files.is_empty(),
                    "The push must upload objects for the server to hold"
                );
                let object_transfer_strategies = objects_request
                    .object_files
                    .into_iter()
                    .map(|object_file| self.upload_strategy(object_file))
                    .collect();
                Self::send::<DatasetPushObjectsTransferResponse>(
                    &mut socket,
                    Ok(DatasetPushObjectsTransferAccepted {
                        object_transfer_strategies,
                    }),
                )
                .await;

                Self::send_progress_request(&mut socket).await;
                self.receive_progress(&mut socket).await;
                self.state.uploads_released.send_replace(true);
                self.receive_progress(&mut socket).await;

                Self::receive::<DatasetPushComplete>(&mut socket).await;
                Self::send::<DatasetPushCompleteResponse>(
                    &mut socket,
                    Ok(DatasetPushCompleteConfirmed {}),
                )
                .await;
            }
        }
    }

    fn upload_strategy(&self, object_file: ObjectFileReference) -> PushObjectTransferStrategy {
        let url = self
            .uploads_base_url
            .join(&object_file.physical_hash.to_string())
            .unwrap();
        PushObjectTransferStrategy {
            object_file,
            push_strategy: ObjectPushStrategy::HttpUpload,
            upload_to: Some(TransferUrl {
                url,
                headers: Vec::new(),
                expires_at: None,
            }),
        }
    }

    async fn send_progress_request(socket: &mut WebSocket) {
        Self::send(
            socket,
            DatasetPushObjectsUploadProgressRequest {
                progress_requested: true,
            },
        )
        .await;
    }

    async fn receive_progress(&self, socket: &mut WebSocket) {
        let progress = Self::receive::<DatasetPushObjectsUploadProgressResponse>(socket).await;
        self.state
            .progress_reports
            .lock()
            .unwrap()
            .push(progress.details);
    }

    async fn receive<T: DeserializeOwned>(socket: &mut WebSocket) -> T {
        axum_read_payload::<T>(socket).await.unwrap()
    }

    async fn send<T: Serialize>(socket: &mut WebSocket, payload: T) {
        socket
            .send(Message::Text(
                serde_json::to_string(&payload).unwrap().into(),
            ))
            .await
            .unwrap();
    }

    /// Fails the completion as a real server does: in the reason of a close
    /// with the code a client that predates typed completion replies reads
    /// as a disconnect
    async fn close_with_completion_error(socket: &mut WebSocket, error: DatasetPushCompleteError) {
        let reason = serde_json::to_string(&DatasetPushCompleteResponse::Err(error)).unwrap();
        socket
            .send(Message::Close(Some(CloseFrame {
                code: PUSH_NOT_COMPLETED_CLOSE_CODE,
                reason: reason.into(),
            })))
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
