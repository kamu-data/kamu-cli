// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use database_common::DatabaseTransactionRunner;
use dill::Catalog;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::{
    AppendDatasetMetadataBatchUseCase,
    BlockRef,
    CorruptedSourceError,
    CreateDatasetError,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    Dataset,
    DatasetVisibility,
    GetRefError,
    HashedMetadataBlock,
};
use opendatafabric::{AsTypedBlock, DatasetRef};
use url::Url;

use super::errors::*;
use super::messages::*;
use super::phases::*;
use super::protocol_dataset_helper::*;
use crate::smart_protocol::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Should be less than idle timeout rules in the load balancer
const MIN_UPLOAD_PROGRESS_PING_DELAY_SEC: u64 = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AxumServerPushProtocolInstance {
    socket: axum::extract::ws::WebSocket,
    catalog: Catalog,
    dataset_ref: DatasetRef,
    dataset: Option<Arc<dyn Dataset>>,
    dataset_url: Url,
    maybe_bearer_header: Option<BearerHeader>,
}

impl AxumServerPushProtocolInstance {
    pub fn new(
        socket: axum::extract::ws::WebSocket,
        catalog: Catalog,
        dataset_ref: DatasetRef,
        dataset: Option<Arc<dyn Dataset>>,
        dataset_url: Url,
        maybe_bearer_header: Option<BearerHeader>,
    ) -> Self {
        Self {
            socket,
            catalog,
            dataset_ref,
            dataset,
            dataset_url,
            maybe_bearer_header,
        }
    }

    pub async fn serve(mut self) {
        match self.push_main_flow().await {
            Ok(_) => {
                tracing::debug!("Push process success");
            }
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
            }
        }

        // After we finish processing to ensure graceful closing
        // we wait for client acknowledgment. And to handle
        // custom clients without logic of close acknowledgment
        // we will give 5 secs timeout
        let timeout_duration = Duration::from_secs(5);
        tokio::select! {
            _ = wait_for_close(&mut self.socket) => {}
            _ = tokio::time::sleep(timeout_duration) => {
                tracing::debug!("Timeout reached, closing connection");
            }
        };
    }

    async fn push_main_flow(&mut self) -> Result<(), PushServerError> {
        let push_request = self.handle_push_request_initiation().await?;
        let force_update_if_diverged = push_request.force_update_if_diverged;

        let mut new_blocks = self.try_handle_push_metadata_request(push_request).await?;
        if !new_blocks.is_empty() {
            if self.dataset.is_none() {
                tracing::info!("Dataset does not exist, trying to create from Seed block");

                let dataset_alias = self
                    .dataset_ref
                    .alias()
                    .expect("Dataset ref is not an alias");

                let (_, first_block) = new_blocks.pop_front().unwrap();
                let seed_block = first_block
                    .into_typed()
                    .ok_or_else(|| {
                        tracing::debug!("First metadata block was not a Seed");
                        CorruptedSourceError {
                            message: "First metadata block is not Seed".to_owned(),
                            source: None,
                        }
                    })
                    .int_err()?;

                // TODO: Read the visibility parameter from CLI
                //
                //       Private Datasets: Update use case: Pushing a dataset
                //       https://github.com/kamu-data/kamu-cli/issues/728
                let create_options = CreateDatasetUseCaseOptions {
                    dataset_visibility: DatasetVisibility::Private,
                };

                let create_result = DatabaseTransactionRunner::new(self.catalog.clone())
                    .transactional_with(
                        |create_dataset_use_case: Arc<dyn CreateDatasetUseCase>| async move {
                            create_dataset_use_case
                                .execute(dataset_alias, seed_block, create_options)
                                .await
                        },
                    )
                    .await;
                match create_result {
                    Ok(create_result) => self.dataset = Some(create_result.dataset),
                    Err(err) => {
                        if let CreateDatasetError::RefCollision(err) = &err {
                            axum_write_close_payload::<DatasetPushObjectsTransferResponse>(
                                &mut self.socket,
                                DatasetPushObjectsTransferResponse::Err(
                                    DatasetPushObjectsTransferError::RefCollision(
                                        DatasetPushObjectsTransferRefCollisionError {
                                            dataset_id: err.id.clone(),
                                        },
                                    ),
                                ),
                            )
                            .await
                            .map_err(|e| {
                                PushServerError::WriteFailed(PushWriteError::new(
                                    e,
                                    PushPhase::InitialRequest,
                                ))
                            })?;
                        };
                        return Err(err.int_err().into());
                    }
                }
            }

            loop {
                let should_continue = self
                    .try_handle_push_objects_request(self.dataset.as_ref().unwrap().clone())
                    .await?;

                if !should_continue {
                    break;
                }
            }
        }

        self.try_handle_push_complete(new_blocks, force_update_if_diverged)
            .await?;

        Ok(())
    }

    async fn handle_push_request_initiation(
        &mut self,
    ) -> Result<DatasetPushRequest, PushServerError> {
        let push_request = axum_read_payload::<DatasetPushRequest>(&mut self.socket)
            .await
            .map_err(|e| {
                PushServerError::ReadFailed(PushReadError::new(e, PushPhase::InitialRequest))
            })?;

        tracing::debug!(
            "Push client sent a push request: currentHead={:?} sizeEstimate={:?}",
            push_request
                .current_head
                .as_ref()
                .map(ToString::to_string)
                .ok_or("None"),
            push_request.transfer_plan
        );

        // TODO: consider size estimate and maybe cancel too large pushes

        let actual_head = if let Some(dataset) = self.dataset.as_ref() {
            match dataset
                .as_metadata_chain()
                .resolve_ref(&BlockRef::Head)
                .await
            {
                Ok(head) => Some(head),
                Err(GetRefError::NotFound(_)) => None,
                Err(e) => {
                    let int_err = e.int_err();
                    axum_write_close_payload::<DatasetPushResponse>(
                        &mut self.socket,
                        Err(DatasetPushRequestError::Internal(DatasetInternalError {
                            error_message: int_err.reason(),
                        })),
                    )
                    .await
                    .map_err(|e| {
                        PushServerError::WriteFailed(PushWriteError::new(
                            e,
                            PushPhase::MetadataRequest,
                        ))
                    })?;
                    return Err(PushServerError::Internal(int_err));
                }
            }
        } else {
            None
        };

        let response = if push_request.current_head == actual_head {
            Ok(DatasetPushRequestAccepted {})
        } else {
            Err(DatasetPushRequestError::InvalidHead(
                DatasetPushInvalidHeadError {
                    actual_head: push_request.current_head.clone(),
                    expected_head: actual_head,
                },
            ))
        };

        axum_write_payload::<DatasetPushResponse>(&mut self.socket, response)
            .await
            .map_err(|e| {
                PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::InitialRequest))
            })?;

        Ok(push_request)
    }

    async fn try_handle_push_metadata_request(
        &mut self,
        push_request: DatasetPushRequest,
    ) -> Result<VecDeque<HashedMetadataBlock>, PushServerError> {
        let push_metadata_request =
            match axum_read_payload::<DatasetPushMetadataRequest>(&mut self.socket).await {
                Ok(push_metadata_request) => Ok(push_metadata_request),
                Err(e) => Err(PushServerError::ReadFailed(PushReadError::new(
                    e,
                    PushPhase::MetadataRequest,
                ))),
            }?;

        tracing::debug!(
            num_blocks = %push_metadata_request.new_blocks.num_blocks,
            media_type = %push_metadata_request.new_blocks.media_type,
            encoding = %push_metadata_request.new_blocks.encoding,
            payload_length = %push_metadata_request.new_blocks.payload.len(),
            "Obtained compressed object batch",
        );

        assert_eq!(
            push_request.transfer_plan.num_blocks,
            push_metadata_request.new_blocks.num_blocks
        );

        let new_blocks = decode_metadata_batch(&push_metadata_request.new_blocks).int_err()?;

        axum_write_payload::<DatasetPushMetadataAccepted>(
            &mut self.socket,
            DatasetPushMetadataAccepted {},
        )
        .await
        .map_err(|e| {
            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::MetadataRequest))
        })?;

        Ok(new_blocks)
    }

    async fn try_handle_push_objects_request(
        &mut self,
        dataset: Arc<dyn Dataset>,
    ) -> Result<bool, PushServerError> {
        let request = axum_read_payload::<DatasetPushObjectsTransferRequest>(&mut self.socket)
            .await
            .map_err(|e| {
                PushServerError::ReadFailed(PushReadError::new(e, PushPhase::ObjectsRequest))
            })?;

        let objects_count = request.object_files.len();

        tracing::debug!(
            % objects_count,
            "Push client sent a push objects request",
        );

        let mut object_transfer_strategies: Vec<PushObjectTransferStrategy> = Vec::new();
        for r in request.object_files {
            let transfer_strategy = match prepare_push_object_transfer_strategy(
                dataset.as_ref(),
                &r,
                &self.dataset_url,
                &self.maybe_bearer_header,
            )
            .await
            {
                Ok(res) => res,
                Err(e) => {
                    let int_err = e.int_err();
                    axum_write_close_payload::<DatasetPushResponse>(
                        &mut self.socket,
                        Err(DatasetPushRequestError::Internal(DatasetInternalError {
                            error_message: int_err.reason(),
                        })),
                    )
                    .await
                    .map_err(|e| {
                        PushServerError::WriteFailed(PushWriteError::new(
                            e,
                            PushPhase::MetadataRequest,
                        ))
                    })?;
                    return Err(PushServerError::Internal(int_err));
                }
            };

            object_transfer_strategies.push(transfer_strategy);
        }

        tracing::debug!("Object transfer strategies defined");

        axum_write_payload::<DatasetPushObjectsTransferResponse>(
            &mut self.socket,
            Ok(DatasetPushObjectsTransferAccepted {
                object_transfer_strategies,
            }),
        )
        .await
        .map_err(|e| {
            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::ObjectsRequest))
        })?;

        let mut notify_interval = tokio::time::interval(std::time::Duration::from_secs(
            MIN_UPLOAD_PROGRESS_PING_DELAY_SEC,
        ));
        notify_interval.tick().await; // clear first immediate interval tick

        loop {
            tokio::select! {
                _ = notify_interval.tick() => {
                    tracing::debug!("Time to ask client about the upload progress");
                    axum_write_payload::<DatasetPushObjectsUploadProgressRequest>(&mut self.socket, DatasetPushObjectsUploadProgressRequest {})
                        .await
                        .map_err(|e| {
                            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::ObjectsUploadProgress))
                        })?;
                },
                progress_response_result = axum_read_payload::<DatasetPushObjectsUploadProgressResponse>(&mut self.socket) => {
                    let progress = progress_response_result
                        .map_err(|e| {
                            PushServerError::ReadFailed(PushReadError::new(e, PushPhase::ObjectsUploadProgress))
                        })?;
                    match progress.details {
                        ObjectsUploadProgressDetails::Running(p) => {
                            tracing::debug!(
                                uploaded_objects_count = % p.uploaded_objects_count,
                                total_objects_count = % objects_count,
                                "Objects upload progress notification"
                            );
                        }
                        ObjectsUploadProgressDetails::Complete => {
                            tracing::debug!("Objects upload complete");
                            break;
                        }
                    }
                }
            }
        }

        Ok(request.is_truncated)
    }

    async fn try_handle_push_complete(
        &mut self,
        new_blocks: VecDeque<HashedMetadataBlock>,
        force_update_if_diverged: bool,
    ) -> Result<(), PushServerError> {
        axum_read_payload::<DatasetPushComplete>(&mut self.socket)
            .await
            .map_err(|e| {
                PushServerError::ReadFailed(PushReadError::new(e, PushPhase::CompleteRequest))
            })?;

        tracing::debug!("Push client sent a complete request. Committing the dataset");

        let dataset = self.dataset.clone().unwrap();
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |append_dataset_metadata_batch: Arc<dyn AppendDatasetMetadataBatchUseCase>| async move {
                    append_dataset_metadata_batch
                        .execute(dataset.as_ref(), new_blocks, force_update_if_diverged)
                        .await
                },
            )
            .await
            .int_err()?;

        tracing::debug!("Sending completion confirmation");

        axum_write_payload::<DatasetPushCompleteConfirmed>(
            &mut self.socket,
            DatasetPushCompleteConfirmed {},
        )
        .await
        .map_err(|e| {
            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::CompleteRequest))
        })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
