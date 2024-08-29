// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use kamu_core::{BlockRef, Dataset};
use url::Url;

use super::errors::*;
use super::messages::*;
use super::phases::*;
use super::protocol_dataset_helper::*;
use crate::smart_protocol::*;
use crate::ws_common::ReadMessageError;
use crate::BearerHeader;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AxumServerPullProtocolInstance {
    socket: axum::extract::ws::WebSocket,
    dataset: Arc<dyn Dataset>,
    dataset_url: Url,
    maybe_bearer_header: Option<BearerHeader>,
}

impl AxumServerPullProtocolInstance {
    pub fn new(
        socket: axum::extract::ws::WebSocket,
        dataset: Arc<dyn Dataset>,
        dataset_url: Url,
        maybe_bearer_header: Option<BearerHeader>,
    ) -> Self {
        Self {
            socket,
            dataset,
            dataset_url,
            maybe_bearer_header,
        }
    }

    pub async fn serve(mut self) {
        match self.pull_main_flow().await {
            Ok(_) => {
                tracing::debug!("Pull process success");
            }
            Err(ref e @ PullServerError::Internal(ref int_err)) => {
                if let Err(write_err) = axum_write_close_payload::<DatasetPullResponse>(
                    &mut self.socket,
                    Err(DatasetPullRequestError::Internal(TransferInternalError {
                        phase: int_err.phase.clone(),
                        error_message: "Internal error".to_string(),
                    })),
                )
                .await
                {
                    tracing::error!(
                      error = ?write_err,
                      error_msg = %write_err,
                      "Failed to send error to client with error",
                    );
                };
                tracing::error!(
                  error = ?e,
                  error_msg = %e,
                  "Push process aborted with internal error",
                );
            }
            Err(ref _e @ PullServerError::ReadFailed(ref err)) => {
                if let ReadMessageError::IncompatibleVersion = err.read_error {
                    if let Err(write_err) = axum_write_close_payload::<DatasetPullResponse>(
                        &mut self.socket,
                        Err(DatasetPullRequestError::Internal(TransferInternalError {
                            phase: TransferPhase::Pull(PullPhase::InitialRequest),
                            error_message: "Incompatible version.".to_string(),
                        })),
                    )
                    .await
                    {
                        tracing::error!(
                          error = ?write_err,
                          error_msg = %write_err,
                          "Failed to send error to client with error",
                        );
                    };
                }
            }
            Err(e) => tracing::error!(
              error = ?e,
              error_msg = %e,
              "Push process aborted with error",
            ),
        }

        // After we finish processing to ensure graceful closing
        // we wait for client acknowledgment. And to handle
        // custom clients without logic of close acknowledgment
        // we will give 5 secs timeout
        let timeout_duration = Duration::from_secs(5);
        if tokio::time::timeout(timeout_duration, wait_for_close(&mut self.socket))
            .await
            .is_err()
        {
            tracing::debug!("Timeout reached, closing connection");
        };
    }

    async fn pull_main_flow(&mut self) -> Result<(), PullServerError> {
        let pull_request = self.handle_pull_request_initiation().await?;

        let received_pull_metadata_request =
            self.try_handle_pull_metadata_request(pull_request).await?;

        if received_pull_metadata_request {
            loop {
                let should_continue = self.try_handle_pull_objects_request().await?;
                if !should_continue {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn handle_pull_request_initiation(
        &mut self,
    ) -> Result<DatasetPullRequest, PullServerError> {
        let pull_request = axum_read_payload::<DatasetPullRequest>(&mut self.socket)
            .await
            .map_err(|e| {
                PullServerError::ReadFailed(PullReadError::new(e, PullPhase::InitialRequest))
            })?;

        tracing::debug!(
            "Pull client sent a pull request: beginAfter={:?} stopAt={:?} \
             force_update_if_diverged={}",
            pull_request
                .begin_after
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None"),
            pull_request
                .stop_at
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None"),
            pull_request.force_update_if_diverged,
        );

        let metadata_chain = self.dataset.as_metadata_chain();
        let head = metadata_chain
            .resolve_ref(&BlockRef::Head)
            .await
            .protocol_int_err(PullPhase::InitialRequest)?;

        let transfer_plan_result = prepare_dataset_transfer_plan(
            metadata_chain,
            pull_request.stop_at.as_ref().unwrap_or(&head),
            pull_request.begin_after.as_ref(),
            pull_request.force_update_if_diverged,
        )
        .await;

        axum_write_payload::<DatasetPullResponse>(
            &mut self.socket,
            match transfer_plan_result {
                Ok(transfer_plan) => {
                    tracing::debug!("Sending size estimate: {:?}", transfer_plan);
                    DatasetPullResponse::Ok(DatasetPullSuccessResponse { transfer_plan })
                }
                Err(PrepareDatasetTransferEstimateError::InvalidInterval(e)) => {
                    tracing::debug!("Sending invalid interval error: {:?}", e);
                    DatasetPullResponse::Err(DatasetPullRequestError::InvalidInterval(
                        DatasetPullInvalidIntervalError {
                            head: e.head.clone(),
                            tail: e.tail.clone(),
                        },
                    ))
                }
                Err(PrepareDatasetTransferEstimateError::Internal(e)) => {
                    tracing::debug!("Sending internal error: {:?}", e);
                    DatasetPullResponse::Err(DatasetPullRequestError::Internal(
                        TransferInternalError {
                            phase: TransferPhase::Pull(PullPhase::InitialRequest),
                            error_message: e.to_string(),
                        },
                    ))
                }
            },
        )
        .await
        .map_err(|e| {
            PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::InitialRequest))
        })?;

        Ok(pull_request)
    }

    async fn try_handle_pull_metadata_request(
        &mut self,
        pull_request: DatasetPullRequest,
    ) -> Result<bool, PullServerError> {
        let maybe_pull_metadata_request =
            axum_read_payload::<DatasetPullMetadataRequest>(&mut self.socket).await;

        match maybe_pull_metadata_request {
            Ok(_) => {
                tracing::debug!("Pull client sent a pull metadata request");

                let metadata_chain = self.dataset.as_metadata_chain();
                let head = metadata_chain
                    .resolve_ref(&BlockRef::Head)
                    .await
                    .protocol_int_err(PullPhase::MetadataRequest)?;

                let metadata_batch = prepare_dataset_metadata_batch(
                    metadata_chain,
                    pull_request.stop_at.as_ref().unwrap_or(&head),
                    pull_request.begin_after.as_ref(),
                    pull_request.force_update_if_diverged,
                )
                .await
                .protocol_int_err(PullPhase::MetadataRequest)?;

                tracing::debug!(
                    num_blocks = % metadata_batch.num_blocks,
                    payload_size = % metadata_batch.payload.len(),
                    "Metadata batch of blocks formed",
                );

                axum_write_payload::<DatasetMetadataPullResponse>(
                    &mut self.socket,
                    DatasetMetadataPullResponse {
                        blocks: metadata_batch,
                    },
                )
                .await
                .map_err(|e| {
                    PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::MetadataRequest))
                })?;

                Ok(true)
            }
            Err(ReadMessageError::Closed) => Ok(false),
            Err(e) => Err(PullServerError::ReadFailed(PullReadError::new(
                e,
                PullPhase::MetadataRequest,
            ))),
        }
    }

    async fn try_handle_pull_objects_request(&mut self) -> Result<bool, PullServerError> {
        let maybe_pull_objects_transfer_request =
            axum_read_payload::<DatasetPullObjectsTransferRequest>(&mut self.socket).await;

        match maybe_pull_objects_transfer_request {
            Ok(request) => {
                tracing::debug!(
                    objects_count = %request.object_files.len(),
                    "Pull client sent a pull objects request"
                );

                let mut object_transfer_strategies: Vec<PullObjectTransferStrategy> = Vec::new();
                for r in request.object_files {
                    let transfer_strategy = prepare_pull_object_transfer_strategy(
                        self.dataset.as_ref(),
                        &r,
                        &self.dataset_url,
                        &self.maybe_bearer_header,
                    )
                    .await
                    .protocol_int_err(PullPhase::ObjectsRequest)?;

                    object_transfer_strategies.push(transfer_strategy);
                }

                tracing::debug!("Object transfer strategies defined");

                axum_write_payload::<DatasetPullObjectsTransferResponse>(
                    &mut self.socket,
                    DatasetPullObjectsTransferResponse {
                        object_transfer_strategies,
                    },
                )
                .await
                .map_err(|e| {
                    PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::ObjectsRequest))
                })?;

                Ok(true)
            }
            Err(ReadMessageError::Closed) => Ok(false),
            Err(e) => Err(PullServerError::ReadFailed(PullReadError::new(
                e,
                PullPhase::ObjectsRequest,
            ))),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
