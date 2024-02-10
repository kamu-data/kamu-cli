// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::{BlockRef, Dataset, ErrorIntoInternal, ResultIntoInternal};
use url::Url;

use super::errors::*;
use super::messages::*;
use super::phases::*;
use super::protocol_dataset_helper::*;
use crate::smart_protocol::*;
use crate::ws_common::ReadMessageError;

/////////////////////////////////////////////////////////////////////////////////

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
        let pull_request = match self.handle_pull_request_initiation().await {
            Ok(pull_request) => pull_request,
            Err(e) => {
                tracing::debug!("Pull process aborted with error: {}", e);
                return;
            }
        };

        let received_pull_metadata_request =
            match self.try_handle_pull_metadata_request(pull_request).await {
                Ok(received) => received,
                Err(e) => {
                    tracing::debug!("Pull process aborted with error: {}", e);
                    return;
                }
            };

        if received_pull_metadata_request {
            loop {
                let should_continue = match self.try_handle_pull_objects_request().await {
                    Ok(should_continue) => should_continue,
                    Err(e) => {
                        tracing::debug!("Pull process aborted with error: {}", e);
                        return;
                    }
                };
                if !should_continue {
                    break;
                }
            }
        }

        // Success
        tracing::debug!("Pull process success");
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
            "Pull client sent a pull request: beginAfter={:?} stopAt={:?}",
            pull_request
                .begin_after
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None"),
            pull_request
                .stop_at
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None")
        );

        let metadata_chain = self.dataset.as_metadata_chain();
        let head = metadata_chain
            .get_ref(&BlockRef::Head)
            .await
            .map_err(|e| PullServerError::Internal(e.int_err()))?;

        let size_estimate_result = prepare_dataset_transfer_estimate(
            metadata_chain,
            pull_request.stop_at.as_ref().unwrap_or(&head),
            pull_request.begin_after.as_ref(),
        )
        .await;

        axum_write_payload::<DatasetPullResponse>(
            &mut self.socket,
            match size_estimate_result {
                Ok(size_estimate) => {
                    tracing::debug!("Sending size estimate: {:?}", size_estimate);
                    DatasetPullResponse::Ok(DatasetPullSuccessResponse { size_estimate })
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
                        DatasetInternalError {
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
                    .get_ref(&BlockRef::Head)
                    .await
                    .map_err(|e| PullServerError::Internal(e.int_err()))?;

                let metadata_batch = prepare_dataset_metadata_batch(
                    metadata_chain,
                    pull_request.stop_at.as_ref().unwrap_or(&head),
                    pull_request.begin_after.as_ref(),
                )
                .await
                .int_err()?;

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
                    .int_err()?;

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

/////////////////////////////////////////////////////////////////////////////////
