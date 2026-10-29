// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::stream::{StreamExt, TryStreamExt};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu::utils::local_sync_writes::{append_transferred_blocks, prepare_transfer_target};
use kamu::utils::smart_transfer_protocol::TransferOptions;
use kamu::utils::sync_increment::sync_increment;
use kamu_core::*;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use url::Url;

use super::WsSmartTransferProtocolClient;
use super::transport::{TungsteniteStream, read_payload, write_payload};
use crate::smart_protocol::errors::*;
use crate::smart_protocol::messages::*;
use crate::smart_protocol::phases::*;
use crate::smart_protocol::protocol_dataset_helper::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    async fn pull_send_request(
        &self,
        socket: &mut TungsteniteStream,
        dst_head: Option<odf::Multihash>,
        force_update_if_diverged: bool,
    ) -> Result<DatasetPullSuccessResponse, PullClientError> {
        let pull_request_message = DatasetPullRequest {
            begin_after: dst_head,
            stop_at: None,
            force_update_if_diverged,
        };

        tracing::debug!("Sending pull request: {:?}", pull_request_message);

        write_payload(socket, pull_request_message)
            .await
            .map_err(|e| {
                PullClientError::WriteFailed(PullWriteError::new(e, PullPhase::InitialRequest))
            })?;

        tracing::debug!("Reading pull request response");

        let dataset_pull_response =
            read_payload::<DatasetPullResponse>(socket)
                .await
                .map_err(|e| {
                    PullClientError::ReadFailed(PullReadError::new(e, PullPhase::InitialRequest))
                })?;

        match dataset_pull_response {
            Ok(success) => {
                tracing::debug!(
                    num_blocks = % success.transfer_plan.num_blocks,
                    bytes_in_raw_locks = % success.transfer_plan.bytes_in_raw_blocks,
                    num_objects = % success.transfer_plan.num_objects,
                    bytes_in_raw_objecs = % success.transfer_plan.bytes_in_raw_objects,
                    "Pull response estimate",
                );
                Ok(success)
            }
            Err(DatasetPullRequestError::Internal(e)) => {
                Err(PullClientError::Internal(InternalError::new(Box::new(
                    ClientInternalError::new(e.error_message.as_str(), e.phase),
                ))))
            }
            Err(DatasetPullRequestError::SeedBlockOverwriteRestricted(_)) => Err(
                PullClientError::OverwriteSeedBlock(OverwriteSeedBlockError {}),
            ),
            Err(DatasetPullRequestError::InvalidInterval(DatasetPullInvalidIntervalError {
                head,
                tail,
            })) => Err(PullClientError::InvalidInterval(
                odf::dataset::InvalidIntervalError { head, tail },
            )),
        }
    }

    async fn pull_send_metadata_request(
        &self,
        socket: &mut TungsteniteStream,
    ) -> Result<DatasetMetadataPullResponse, PullClientError> {
        let pull_metadata_request = DatasetPullMetadataRequest {};
        tracing::debug!("Sending pull metadata request");

        write_payload(socket, pull_metadata_request)
            .await
            .map_err(|e| {
                PullClientError::WriteFailed(PullWriteError::new(e, PullPhase::MetadataRequest))
            })?;

        tracing::debug!("Reading pull metadata request response");

        let dataset_metadata_pull_response = read_payload::<DatasetMetadataPullResponse>(socket)
            .await
            .map_err(|e| {
                PullClientError::ReadFailed(PullReadError::new(e, PullPhase::MetadataRequest))
            })?;

        tracing::debug!(
            num_blocks = % dataset_metadata_pull_response.blocks.num_blocks,
            media_type = % dataset_metadata_pull_response.blocks.media_type,
            encoding = % dataset_metadata_pull_response.blocks.encoding,
            payload_length = % dataset_metadata_pull_response.blocks.payload.len(),
            "Obtained compressed objects batch",
        );

        Ok(dataset_metadata_pull_response)
    }

    async fn pull_send_objects_request(
        &self,
        socket: &mut TungsteniteStream,
        object_files: Vec<ObjectFileReference>,
    ) -> Result<DatasetPullObjectsTransferResponse, PullClientError> {
        let pull_objects_request = DatasetPullObjectsTransferRequest { object_files };
        tracing::debug!(
            num_objects = % pull_objects_request.object_files.len(),
            "Sending pull objects request",
        );

        write_payload(socket, pull_objects_request)
            .await
            .map_err(|e| {
                PullClientError::WriteFailed(PullWriteError::new(e, PullPhase::ObjectsRequest))
            })?;

        tracing::debug!("Reading pull objects request response");

        let dataset_objects_pull_response =
            read_payload::<DatasetPullObjectsTransferResponse>(socket)
                .await
                .map_err(|e| {
                    PullClientError::ReadFailed(PullReadError::new(e, PullPhase::ObjectsRequest))
                })?;

        tracing::debug!(
            num_objects = % dataset_objects_pull_response
                .object_transfer_strategies
                .len(),
            "Obtained transfer strategies",
        );

        dataset_objects_pull_response
            .object_transfer_strategies
            .iter()
            .for_each(|s| {
                tracing::debug!(
                    physical_hash = % s.object_file.physical_hash,
                    download_from_url = % s.download_from.url,
                    pull_strategy = ? s.pull_strategy,
                    expires_at = ? s.download_from.expires_at,
                    "Detailed file download strategy",
                );
            });

        Ok(dataset_objects_pull_response)
    }

    pub(super) async fn pull_flow(
        &self,
        http_src_url: &Url,
        dst: &LocalSyncTarget,
        transfer_options: TransferOptions,
    ) -> Result<SyncTransfer, SyncError> {
        let maybe_access_token = self
            .dataset_credential_resolver
            .resolve_odf_dataset_access_token(http_src_url);

        let (dst_dataset, dst_head) = match dst {
            LocalSyncTarget::Existing {
                dataset,
                expected_head,
            } => (Some(dataset), expected_head.as_ref()),
            LocalSyncTarget::New(_) => (None, None),
        };

        // A forced pull skips the head check that would otherwise reveal a
        // different dataset, so compare the IDs before transferring anything
        if transfer_options.force_update_if_diverged
            && let Some(dst_dataset) = dst_dataset
            && let Some(mismatch) = Self::find_pull_dataset_id_mismatch(
                http_src_url,
                maybe_access_token.as_deref(),
                dst_dataset,
            )
            .await
        {
            return Err(mismatch.into());
        }

        let mut ws_stream =
            Self::connect(http_src_url, "pull", maybe_access_token.as_deref()).await?;

        let dataset_pull_result = match self
            .pull_send_request(
                &mut ws_stream,
                dst_head.cloned(),
                transfer_options.force_update_if_diverged,
            )
            .await
        {
            Ok(dataset_pull_result) => dataset_pull_result,
            Err(e) => {
                tracing::debug!("Pull process aborted with error: {}", e);
                let e = match e {
                    PullClientError::InvalidInterval(e) => {
                        // The server could not find our head in its chain. Unless the
                        // datasets differ entirely, this means the chains diverged.
                        if let Some(dst_dataset) = dst_dataset
                            && let Some(mismatch) = Self::find_pull_dataset_id_mismatch(
                                http_src_url,
                                maybe_access_token.as_deref(),
                                dst_dataset,
                            )
                            .await
                        {
                            mismatch.into()
                        } else {
                            SyncError::DatasetsDiverged(DatasetsDivergedError {
                                src_head: e.head,
                                dst_head: e.tail,
                                detail: None,
                            })
                        }
                    }
                    PullClientError::OverwriteSeedBlock(_)
                    | PullClientError::ReadFailed(_)
                    | PullClientError::WriteFailed(_)
                    | PullClientError::Internal(_) => SyncError::Internal(e.int_err()),
                };
                return Err(e);
            }
        };

        let sync_transfer = if dataset_pull_result.transfer_plan.num_blocks > 0 {
            let dataset_pull_metadata_response =
                match self.pull_send_metadata_request(&mut ws_stream).await {
                    Ok(dataset_pull_metadata_response) => Ok(dataset_pull_metadata_response),
                    Err(e) => {
                        tracing::debug!("Pull process aborted with error: {}", e);
                        Err(SyncError::Internal(e.int_err()))
                    }
                }?;

            // A retry would receive the same blocks, so a batch that cannot be read is
            // a corrupted source, not a transient failure
            let mut new_blocks = decode_metadata_batch(&dataset_pull_metadata_response.blocks)
                .map_err(|e| CorruptedSourceError {
                    message: "Server sent a metadata batch that cannot be read".to_owned(),
                    source: Some(e.into()),
                })?;

            let new_head = new_blocks
                .back()
                .map(|(hash, _, _)| hash.clone())
                .ok_or_else(|| CorruptedSourceError {
                    message: "Server sent no blocks for a non-empty transfer plan".to_owned(),
                    source: None,
                })?;

            if let LocalSyncTarget::Existing { dataset, .. } = dst
                && transfer_options.force_update_if_diverged
                && !ensure_seed_not_in_conflict(new_blocks.front(), dataset.get_handle())
            {
                return Err(SyncError::OverwriteSeedBlock(OverwriteSeedBlockError {}));
            }

            // Safety: there are blocks, oldest first
            let (dst_storage, target) = prepare_transfer_target(
                &self.catalog,
                dst,
                transfer_options.visibility_for_created_dataset,
                || new_blocks.pop_front().unwrap(),
            )
            .await?;

            let object_files =
                collect_object_references_from_metadata(dst_storage.as_ref(), &new_blocks, true)
                    .await?;

            // TODO: analyze sizes and split on stages
            let object_files_transfer_plan = if object_files.is_empty() {
                vec![]
            } else {
                vec![object_files]
            };

            tracing::debug!(
                "Object files transfer plan consist of {} stages",
                object_files_transfer_plan.len()
            );

            let mut stage_index = 0;
            for stage_object_files in object_files_transfer_plan {
                stage_index += 1;
                tracing::debug!(
                    "Stage #{}: querying {} data objects",
                    stage_index,
                    stage_object_files.len()
                );

                let dataset_objects_pull_response = match self
                    .pull_send_objects_request(&mut ws_stream, stage_object_files)
                    .await
                {
                    Ok(dataset_objects_pull_response) => Ok(dataset_objects_pull_response),
                    Err(e) => {
                        tracing::debug!("Pull process aborted with error: {}", e);
                        Err(SyncError::Internal(e.int_err()))
                    }
                }?;

                let dst_ref = dst_storage.as_ref();
                futures::stream::iter(dataset_objects_pull_response.object_transfer_strategies)
                    .map(Ok)
                    .try_for_each_concurrent(
                        /* limit */ transfer_options.max_parallel_transfers,
                        |s| async move { dataset_import_object_file(dst_ref, s).await },
                    )
                    .await?;
            }

            append_transferred_blocks(
                dst_storage.as_ref(),
                Box::new(new_blocks.into_iter()),
                odf::dataset::AppendBatchOpts::default(),
            )
            .await?;

            let old_head = match dst {
                LocalSyncTarget::Existing { expected_head, .. } => expected_head.as_ref(),
                LocalSyncTarget::New(_) => None,
            };
            let increment =
                sync_increment(dst_storage.as_metadata_chain(), old_head, &new_head).await?;

            SyncTransfer::Transferred(TransferredSync {
                target,
                new_head,
                num_blocks: u64::from(dataset_pull_result.transfer_plan.num_blocks),
                increment,
            })
        } else {
            SyncTransfer::UpToDate
        };

        ws_stream
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "Client pull flow succeeded".into(),
            }))
            .await
            .int_err()?;

        Ok(sync_transfer)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
