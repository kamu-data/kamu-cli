// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu::utils::smart_transfer_protocol::TransferOptions;
use kamu::utils::sync_increment::sync_increment;
use kamu_core::*;
use kamu_datasets::NameCollisionError;
use url::Url;

use super::WsSmartTransferProtocolClient;
use super::transport::{TungsteniteStream, read_payload, write_payload};
use crate::smart_protocol::errors::*;
use crate::smart_protocol::messages::*;
use crate::smart_protocol::phases::*;
use crate::smart_protocol::protocol_dataset_helper::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    async fn push_send_request(
        &self,
        socket: &mut TungsteniteStream,
        transfer_plan: TransferPlan,
        dst_head: Option<&odf::Multihash>,
        force_update_if_diverged: bool,
        visibility_for_created_dataset: odf::DatasetVisibility,
    ) -> Result<DatasetPushRequestAccepted, PushClientError> {
        let push_request_message = DatasetPushRequest {
            current_head: dst_head.cloned(),
            transfer_plan,
            force_update_if_diverged,
            visibility_for_created_dataset,
        };

        tracing::debug!("Sending push request: {:?}", push_request_message);

        write_payload(socket, push_request_message)
            .await
            .map_err(|e| {
                PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::InitialRequest))
            })?;

        tracing::debug!("Reading push request response");

        let dataset_push_response =
            read_payload::<DatasetPushResponse>(socket)
                .await
                .map_err(|e| {
                    PushClientError::ReadFailed(PushReadError::new(e, PushPhase::InitialRequest))
                })?;

        match dataset_push_response {
            Ok(success) => {
                tracing::debug!("Push response accepted");
                Ok(success)
            }
            Err(DatasetPushRequestError::Internal(e)) => {
                Err(PushClientError::Internal(InternalError::new(Box::new(
                    ClientInternalError::new(e.error_message.as_str(), e.phase),
                ))))
            }
            Err(DatasetPushRequestError::InvalidHead(e)) => {
                Err(PushClientError::InvalidHead(e.into()))
            }
        }
    }

    async fn push_send_metadata_request(
        &self,
        socket: &mut TungsteniteStream,
        src_dataset: &dyn odf::Dataset,
        src_head: &odf::Multihash,
        dst_head: Option<&odf::Multihash>,
        force_update_if_diverged: bool,
    ) -> Result<DatasetPushMetadataAccepted, PushClientError> {
        tracing::debug!("Sending push metadata request");

        let metadata_batch = prepare_dataset_metadata_batch(
            src_dataset.as_metadata_chain(),
            src_head,
            dst_head,
            force_update_if_diverged,
        )
        .await
        .int_err()?;

        tracing::debug!(
            num_blocks = % metadata_batch.num_blocks,
            payload_len = metadata_batch.payload.len(),
            "Metadata batch of blocks formed",
        );

        write_payload(
            socket,
            DatasetPushMetadataRequest {
                new_blocks: metadata_batch,
            },
        )
        .await
        .map_err(|e| {
            PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::MetadataRequest))
        })?;

        tracing::debug!("Reading push metadata response");

        let dataset_metadata_push_response = read_payload::<DatasetPushMetadataResponse>(socket)
            .await
            .map_err(|e| {
                PushClientError::ReadFailed(PushReadError::new(e, PushPhase::MetadataRequest))
            })?
            .map_err(|e| match e {
                DatasetPushMetadataError::SeedBlockOverwriteRestricted(_) => {
                    PushClientError::OverwriteSeedBlock(OverwriteSeedBlockError {})
                }
            })?;

        Ok(dataset_metadata_push_response)
    }

    async fn push_send_objects_request(
        &self,
        socket: &mut TungsteniteStream,
        object_files: Vec<ObjectFileReference>,
    ) -> Result<DatasetPushObjectsTransferAccepted, PushClientError> {
        let push_objects_request = DatasetPushObjectsTransferRequest {
            object_files,
            is_truncated: false, // TODO: split on pages to avoid links expiry
        };
        tracing::debug!(
            objects_count = % push_objects_request.object_files.len(),
            "Sending push objects request"
        );

        write_payload(socket, push_objects_request)
            .await
            .map_err(|e| {
                PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::ObjectsRequest))
            })?;

        tracing::debug!("Reading push objects request response");

        let dataset_objects_push_response =
            read_payload::<DatasetPushObjectsTransferResponse>(socket)
                .await
                .map_err(|e| {
                    PushClientError::ReadFailed(PushReadError::new(e, PushPhase::ObjectsRequest))
                })?
                .map_err(|e| match e {
                    DatasetPushObjectsTransferError::RefCollision(err) => {
                        PushClientError::RefCollision(odf::dataset::RefCollisionError {
                            id: err.dataset_id,
                        })
                    }
                    DatasetPushObjectsTransferError::NameCollision(err) => {
                        PushClientError::NameCollision(NameCollisionError {
                            alias: err.dataset_alias,
                        })
                    }
                    DatasetPushObjectsTransferError::Internal(err) => {
                        PushClientError::Internal(InternalError::new(Box::new(
                            ClientInternalError::new(err.error_message.as_str(), err.phase),
                        )))
                    }
                })?;

        tracing::debug!(
            objects_count = % dataset_objects_push_response
                .object_transfer_strategies
                .len(),
            "Obtained transfer strategies",
        );

        dataset_objects_push_response
            .object_transfer_strategies
            .iter()
            .for_each(|s| match s.upload_to.as_ref() {
                Some(transfer_url) => {
                    tracing::debug!(
                        physical_hash = % s.object_file.physical_hash,
                        transfer_url = % transfer_url.url,
                        push_strategy = ? s.push_strategy,
                        expires_at = ? transfer_url.expires_at,
                        "Object file upload strategy details"
                    );
                }
                None => {
                    tracing::debug!(
                        physical_hash = % s.object_file.physical_hash,
                        "Skipping object file upload"
                    );
                }
            });

        Ok(dataset_objects_push_response)
    }

    async fn export_group_of_object_files(
        &self,
        socket: &mut TungsteniteStream,
        push_objects_response: DatasetPushObjectsTransferAccepted,
        src: Arc<dyn odf::Dataset>,
        transfer_options: TransferOptions,
    ) -> Result<(), SyncError> {
        let uploaded_files_counter = Arc::new(AtomicI32::new(0));

        let task_data: Vec<_> = push_objects_response
            .object_transfer_strategies
            .into_iter()
            .map(|s| (s, uploaded_files_counter.clone()))
            .collect();

        let mut export_task = tokio::spawn(async move {
            let src_ref = src.as_ref();
            use futures::stream::{StreamExt, TryStreamExt};
            futures::stream::iter(task_data)
                .map(Ok)
                .try_for_each_concurrent(
                    /* limit */ transfer_options.max_parallel_transfers,
                    |(s, counter)| async move {
                        let export_result = dataset_export_object_file(src_ref, s).await;
                        counter.fetch_add(1, Ordering::Relaxed);
                        export_result
                    },
                )
                .await
        });

        loop {
            tokio::select! {
                _ = read_payload::<DatasetPushObjectsUploadProgressRequest>(socket) => {
                        let uploaded_files_count: i32 = uploaded_files_counter.load(Ordering::Relaxed);
                        tracing::debug!(%uploaded_files_count, "Notifying server about the upload progress");
                        self
                            .push_send_objects_upload_progress(socket, uploaded_files_count)
                            .await
                            .int_err()?;
                    }
                res = &mut export_task => {
                    let join_res = res.int_err()?;
                    if join_res.is_err() {
                        tracing::error!(
                            "File transfer failed, error is {:?}",
                            join_res
                        );
                        join_res?;
                    }
                    break;
                }
            }
        }

        tracing::debug!("Uploading group of files finished");

        match self.push_send_objects_upload_complete(socket).await {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        }

        Ok(())
    }

    async fn push_send_objects_upload_progress(
        &self,
        socket: &mut TungsteniteStream,
        uploaded_objects_count: i32,
    ) -> Result<(), PushClientError> {
        tracing::debug!("Sending push objects upload progress");
        write_payload(
            socket,
            DatasetPushObjectsUploadProgressResponse {
                details: ObjectsUploadProgressDetails::Running(
                    ObjectsUploadProgressDetailsRunning {
                        uploaded_objects_count,
                    },
                ),
            },
        )
        .await
        .map_err(|e| {
            PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::ObjectsUploadProgress))
        })?;
        Ok(())
    }

    async fn push_send_objects_upload_complete(
        &self,
        socket: &mut TungsteniteStream,
    ) -> Result<(), PushClientError> {
        tracing::debug!("Sending push objects upload progress");
        write_payload(
            socket,
            DatasetPushObjectsUploadProgressResponse {
                details: ObjectsUploadProgressDetails::Complete,
            },
        )
        .await
        .map_err(|e| {
            PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::ObjectsUploadProgress))
        })?;
        Ok(())
    }

    async fn push_send_complete_request(
        &self,
        socket: &mut TungsteniteStream,
    ) -> Result<(), PushClientError> {
        tracing::debug!("Sending push complete request");

        write_payload(socket, DatasetPushComplete {})
            .await
            .map_err(|e| {
                PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::CompleteRequest))
            })?;

        tracing::debug!("Reading push complete response");

        let reply = read_payload::<DatasetPushCompleteReply>(socket)
            .await
            .map_err(|e| {
                PushClientError::ReadFailed(PushReadError::new(e, PushPhase::CompleteRequest))
            })?;

        match reply {
            DatasetPushCompleteReply::Response(Ok(DatasetPushCompleteConfirmed {}))
            | DatasetPushCompleteReply::Legacy(DatasetPushCompleteLegacyConfirmed {}) => Ok(()),
            DatasetPushCompleteReply::Response(Err(DatasetPushCompleteError::Internal(e))) => {
                Err(PushClientError::Internal(InternalError::new(Box::new(
                    ClientInternalError::new(e.error_message.as_str(), e.phase),
                ))))
            }
            DatasetPushCompleteReply::Response(Err(
                DatasetPushCompleteError::UpdatedConcurrently(
                    DatasetPushUpdatedConcurrentlyError {},
                ),
            )) => Err(PushClientError::UpdatedConcurrently(
                DestinationUpdatedConcurrentlyError,
            )),
        }
    }

    pub(super) async fn push_flow(
        &self,
        src: Arc<dyn odf::Dataset>,
        http_dst_url: &Url,
        dst_head: Option<&odf::Multihash>,
        transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError> {
        let src_head = src
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;

        // A forced push skips the head check that would otherwise reveal a
        // different dataset, so compare the IDs before transferring anything
        if transfer_options.force_update_if_diverged
            && dst_head.is_some()
            && let Some(mismatch) = self
                .find_push_dataset_id_mismatch(src.as_ref(), http_dst_url)
                .await
        {
            return Err(mismatch.into());
        }

        let transfer_plan = match prepare_dataset_transfer_plan(
            src.as_metadata_chain(),
            &src_head,
            dst_head,
            transfer_options.force_update_if_diverged,
        )
        .await
        {
            Ok(transfer_plan) => transfer_plan,
            Err(PrepareDatasetTransferEstimateError::InvalidInterval(e)) => {
                // The destination head is missing from the source chain. Unless the
                // datasets differ entirely, this means the chains diverged.
                return Err(
                    match self
                        .find_push_dataset_id_mismatch(src.as_ref(), http_dst_url)
                        .await
                    {
                        Some(mismatch) => mismatch.into(),
                        None => SyncError::DatasetsDiverged(DatasetsDivergedError {
                            src_head: e.head,
                            dst_head: e.tail,
                            detail: None,
                        }),
                    },
                );
            }
            Err(PrepareDatasetTransferEstimateError::Internal(e)) => {
                return Err(SyncError::Internal(e));
            }
        };

        let num_blocks = transfer_plan.num_blocks;
        if num_blocks == 0 {
            return Ok(SyncResult::UpToDate);
        }

        // Read before the remote head moves, so a failure here aborts the push
        let increment = sync_increment(src.as_metadata_chain(), dst_head, &src_head).await?;

        let maybe_access_token = self
            .dataset_credential_resolver
            .resolve_odf_dataset_access_token(http_dst_url);
        let mut ws_stream =
            Self::connect(http_dst_url, "push", maybe_access_token.as_deref()).await?;

        match self
            .push_send_request(
                &mut ws_stream,
                transfer_plan,
                dst_head,
                transfer_options.force_update_if_diverged,
                transfer_options.visibility_for_created_dataset,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(e.into());
            }
        }

        match self
            .push_send_metadata_request(
                &mut ws_stream,
                src.as_ref(),
                &src_head,
                dst_head,
                transfer_options.force_update_if_diverged,
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(e.into());
            }
        }

        let missing_objects = match collect_object_references_from_interval(
            src.as_ref(),
            &src_head,
            dst_head,
            transfer_options.force_update_if_diverged,
            false,
        )
        .await
        {
            Ok(object_references) => object_references,
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        let push_objects_response = match self
            .push_send_objects_request(&mut ws_stream, missing_objects)
            .await
        {
            Ok(push_objects_response) => push_objects_response,
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(e.into());
            }
        };

        self.export_group_of_object_files(
            &mut ws_stream,
            push_objects_response,
            src.clone(),
            transfer_options,
        )
        .await?;

        match self.push_send_complete_request(&mut ws_stream).await {
            Ok(()) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(e.into());
            }
        }

        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        ws_stream
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "Client push flow succeeded".into(),
            }))
            .await
            .int_err()?;

        Ok(SyncResult::Updated {
            old_head: dst_head.cloned(),
            new_head: src_head,
            num_blocks: u64::from(num_blocks),
            increment,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
