// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::routing::{get, put};
use kamu_adapter_http::smart_protocol::messages::*;
use url::Url;

use super::{ScriptedServerListener, ScriptedSocket};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Answers one smart push by script and stores nothing. It has no route for
/// `refs`, so the client finds no `HEAD` and pushes its whole chain
pub(crate) struct ScriptedPushServer {
    listener: ScriptedServerListener,
    script: PushScript,
    state: Arc<ScriptState>,
}

impl ScriptedPushServer {
    pub async fn stale_progress_request() -> Self {
        Self::new(PushScript::StaleProgressRequest).await
    }

    pub async fn progress_during_upload() -> Self {
        Self::new(PushScript::ProgressDuringUpload).await
    }

    async fn new(script: PushScript) -> Self {
        Self {
            listener: ScriptedServerListener::bind().await,
            script,
            state: Arc::new(ScriptState::default()),
        }
    }

    pub fn dataset_url(&self, dataset_name: &str) -> Url {
        self.listener.dataset_url(dataset_name)
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
            uploads_base_url: self.listener.http_url("uploads/"),
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
        self.listener.serve(router).await
    }
}

/// What the server does once the client asked to transfer objects
#[derive(Clone, Copy)]
enum PushScript {
    /// Skips every upload, sends one more progress request after the client
    /// reports completion (as a real server's timer can), then fails the
    /// completion with `UpdatedConcurrently`
    StaleProgressRequest,

    /// Takes every object by HTTP upload, but holds each upload until the
    /// client answered one progress request, then confirms the completion.
    /// A push with no objects to hold fails, as the server closes the socket
    ProgressDuringUpload,
}

#[derive(Default)]
struct ScriptState {
    progress_reports: Mutex<Vec<ObjectsUploadProgressDetails>>,
    uploads_released: tokio::sync::watch::Sender<bool>,
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
    async fn run(self, socket: WebSocket) {
        let mut socket = ScriptedSocket::new(socket);
        socket.receive::<DatasetPushRequest>().await;
        socket
            .send::<DatasetPushResponse>(Ok(DatasetPushRequestAccepted {}))
            .await;

        socket.receive::<DatasetPushMetadataRequest>().await;
        socket
            .send(DatasetPushMetadataResponse::Ok(
                DatasetPushMetadataAccepted {},
            ))
            .await;

        let objects_request = socket.receive::<DatasetPushObjectsTransferRequest>().await;

        match self.script {
            PushScript::StaleProgressRequest => {
                socket
                    .send::<DatasetPushObjectsTransferResponse>(Ok(
                        DatasetPushObjectsTransferAccepted {
                            object_transfer_strategies: Vec::new(),
                        },
                    ))
                    .await;

                self.receive_progress(&mut socket).await;
                Self::send_progress_request(&mut socket).await;

                socket.receive::<DatasetPushComplete>().await;
                socket
                    .close_push_not_completed(DatasetPushCompleteError::UpdatedConcurrently(
                        DatasetPushUpdatedConcurrentlyError {},
                    ))
                    .await;
            }
            PushScript::ProgressDuringUpload => {
                if objects_request.object_files.is_empty() {
                    return;
                }
                let object_transfer_strategies = objects_request
                    .object_files
                    .into_iter()
                    .map(|object_file| self.upload_strategy(object_file))
                    .collect();
                socket
                    .send::<DatasetPushObjectsTransferResponse>(Ok(
                        DatasetPushObjectsTransferAccepted {
                            object_transfer_strategies,
                        },
                    ))
                    .await;

                Self::send_progress_request(&mut socket).await;
                self.receive_progress(&mut socket).await;
                self.state.uploads_released.send_replace(true);
                self.receive_progress(&mut socket).await;

                socket.receive::<DatasetPushComplete>().await;
                socket
                    .send::<DatasetPushCompleteResponse>(Ok(DatasetPushCompleteConfirmed {}))
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

    async fn send_progress_request(socket: &mut ScriptedSocket) {
        socket
            .send(DatasetPushObjectsUploadProgressRequest {
                progress_requested: true,
            })
            .await;
    }

    async fn receive_progress(&self, socket: &mut ScriptedSocket) {
        let progress = socket
            .receive::<DatasetPushObjectsUploadProgressResponse>()
            .await;
        self.state
            .progress_reports
            .lock()
            .unwrap()
            .push(progress.details);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
