// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use dill::*;
use futures::SinkExt;
use headers::Header;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu::utils::smart_transfer_protocol::{SmartTransferProtocolClient, TransferOptions};
use kamu_core::{OverwriteSeedBlockError, *};
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseOptions,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    NameCollisionError,
    SetRefCheckRefMode,
};
use odf::metadata::AsTypedBlock as _;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message};
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use url::Url;

use crate::smart_protocol::errors::*;
use crate::smart_protocol::messages::*;
use crate::smart_protocol::phases::*;
use crate::smart_protocol::protocol_dataset_helper::*;
use crate::ws_common::{self, ReadMessageError, WriteMessageError};
use crate::OdfSmtpVersion;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WsSmartTransferProtocolClient {
    catalog: Catalog,
    dataset_credential_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SmartTransferProtocolClient)]
impl WsSmartTransferProtocolClient {
    pub fn new(
        catalog: Catalog,
        dataset_credential_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
    ) -> Self {
        Self {
            catalog,
            dataset_credential_resolver,
        }
    }

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
                odf::dataset::InvalidIntervalError {
                    head: head.clone(),
                    tail: tail.clone(),
                },
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
                Err(PushClientError::InvalidHead(odf::dataset::RefCASError {
                    actual: e.actual_head.clone(),
                    expected: e.expected_head.clone(),
                    reference: odf::BlockRef::Head,
                }))
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
        };

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
    ) -> Result<DatasetPushCompleteConfirmed, PushClientError> {
        tracing::debug!("Sending push complete request");

        write_payload(socket, DatasetPushComplete {})
            .await
            .map_err(|e| {
                PushClientError::WriteFailed(PushWriteError::new(e, PushPhase::CompleteRequest))
            })?;

        tracing::debug!("Reading push complete response");

        read_payload::<DatasetPushCompleteConfirmed>(socket)
            .await
            .map_err(|e| {
                PushClientError::ReadFailed(PushReadError::new(e, PushPhase::CompleteRequest))
            })
    }

    fn generate_ws_url(http_base_url: &Url, additional_path_segment: &str) -> Url {
        let mut url = http_base_url.join(additional_path_segment).unwrap();
        let new_scheme = if url.scheme() == "https" { "wss" } else { "ws" };

        assert!(url.set_scheme(new_scheme).is_ok());

        url
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SmartTransferProtocolClient for WsSmartTransferProtocolClient {
    async fn pull_protocol_client_flow(
        &self,
        http_src_url: &Url,
        dst: Option<&ResolvedDataset>,
        dst_alias: Option<&odf::DatasetAlias>,
        listener: Arc<dyn SyncListener>,
        transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let maybe_access_token = self
            .dataset_credential_resolver
            .resolve_odf_dataset_access_token(http_src_url);
        let pull_url = Self::generate_ws_url(http_src_url, "pull");

        tracing::debug!(
            %pull_url, access_token = ?maybe_access_token,
            "Connecting smart pull protocol web socket",
        );

        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = pull_url.into_client_request().int_err()?;
        request.headers_mut().append(
            OdfSmtpVersion::name(),
            http::HeaderValue::from(SMART_TRANSFER_PROTOCOL_VERSION),
        );
        if let Some(access_token) = maybe_access_token {
            request.headers_mut().append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
            );
        }

        let mut ws_stream = match connect_async(request).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(e) => {
                tracing::debug!("Failed to connect to pull URL: {}", e);
                return Err(map_tungstenite_error(e, http_src_url));
            }
        };

        let dst_head = if let Some(dst) = &dst {
            match dst
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
            {
                Ok(head) => Ok(Some(head)),
                Err(odf::GetRefError::NotFound(_)) => Ok(None),
                Err(odf::GetRefError::Access(e)) => Err(SyncError::Access(e)),
                Err(odf::GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            }?
        } else {
            None
        };

        let dataset_pull_result = match self
            .pull_send_request(
                &mut ws_stream,
                dst_head.clone(),
                transfer_options.force_update_if_diverged,
            )
            .await
        {
            Ok(dataset_pull_result) => dataset_pull_result,
            Err(e) => {
                tracing::debug!("Pull process aborted with error: {}", e);
                let e = match e {
                    PullClientError::InvalidInterval(e) => {
                        SyncError::DatasetsDiverged(DatasetsDivergedError {
                            src_head: e.head,
                            dst_head: e.tail,
                            detail: None,
                        })
                    }
                    _ => SyncError::Internal(e.int_err()),
                };
                return Err(e);
            }
        };

        let sync_result = if dataset_pull_result.transfer_plan.num_blocks > 0 {
            let dataset_pull_metadata_response =
                match self.pull_send_metadata_request(&mut ws_stream).await {
                    Ok(dataset_pull_metadata_response) => Ok(dataset_pull_metadata_response),
                    Err(e) => {
                        tracing::debug!("Pull process aborted with error: {}", e);
                        Err(SyncError::Internal(e.int_err()))
                    }
                }?;

            let mut new_blocks =
                decode_metadata_batch(&dataset_pull_metadata_response.blocks).int_err()?;

            // Create destination dataset if not exists
            let dst = if let Some(dst) = dst {
                // Check is incoming seed is different from existing
                if transfer_options.force_update_if_diverged
                    && !ensure_seed_block_equals(new_blocks.front(), dst.get_handle())
                {
                    return Err(SyncError::OverwriteSeedBlock(OverwriteSeedBlockError {}));
                }
                (**dst).clone()
            } else {
                let (first_hash, first_block) = new_blocks.pop_front().unwrap();
                let seed_block = first_block
                    .into_typed()
                    .ok_or_else(|| CorruptedSourceError {
                        message: "First metadata block is not Seed".to_owned(),
                        source: None,
                    })?;

                let create_dataset_use_case =
                    self.catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();
                let alias =
                    dst_alias.ok_or_else(|| "Destination dataset alias is unknown".int_err())?;
                let create_result = create_dataset_use_case
                    .execute(
                        alias,
                        seed_block,
                        CreateDatasetUseCaseOptions {
                            dataset_visibility: transfer_options.visibility_for_created_dataset,
                        },
                    )
                    .await
                    .int_err()?;
                assert_eq!(first_hash, create_result.head);
                create_result.dataset
            };

            let object_files =
                collect_object_references_from_metadata(dst.as_ref(), &new_blocks, true).await;

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

                let dst_ref = dst.as_ref();
                use futures::stream::{StreamExt, TryStreamExt};
                futures::stream::iter(dataset_objects_pull_response.object_transfer_strategies)
                    .map(Ok)
                    .try_for_each_concurrent(
                        /* limit */ transfer_options.max_parallel_transfers,
                        |s| async move { dataset_import_object_file(dst_ref, s).await },
                    )
                    .await?;
            }

            let append_dataset_metadata_batch_use_case = self
                .catalog
                .get_one::<dyn AppendDatasetMetadataBatchUseCase>()
                .unwrap();
            let dst_dataset = dst.clone();

            append_dataset_metadata_batch_use_case
                .execute(
                    dst_dataset.as_ref(),
                    Box::new(new_blocks.into_iter()),
                    AppendDatasetMetadataBatchUseCaseOptions {
                        set_ref_check_ref_mode: Some(SetRefCheckRefMode::ForceUpdateIfDiverged(
                            transfer_options.force_update_if_diverged,
                        )),
                        ..Default::default()
                    },
                )
                .await
                .int_err()?;

            let new_dst_head = dst
                .as_metadata_chain()
                .resolve_ref(&odf::BlockRef::Head)
                .await
                .int_err()?;

            SyncResult::Updated {
                old_head: dst_head,
                new_head: new_dst_head,
                num_blocks: u64::from(dataset_pull_result.transfer_plan.num_blocks),
            }
        } else {
            SyncResult::UpToDate
        };

        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
        ws_stream
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "Client pull flow succeeded".into(),
            }))
            .await
            .int_err()?;

        Ok(sync_result)
    }

    async fn push_protocol_client_flow(
        &self,
        src: Arc<dyn odf::Dataset>,
        http_dst_url: &Url,
        dst_head: Option<&odf::Multihash>,
        listener: Arc<dyn SyncListener>,
        transfer_options: TransferOptions,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let src_head = src
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;

        let transfer_plan = prepare_dataset_transfer_plan(
            src.as_metadata_chain(),
            &src_head,
            dst_head,
            transfer_options.force_update_if_diverged,
        )
        .await
        .int_err()?;

        let num_blocks = transfer_plan.num_blocks;
        if num_blocks == 0 {
            return Ok(SyncResult::UpToDate);
        }

        let maybe_access_token = self
            .dataset_credential_resolver
            .resolve_odf_dataset_access_token(http_dst_url);
        let push_url = Self::generate_ws_url(http_dst_url, "push");

        tracing::debug!(
            %push_url, access_token = ?maybe_access_token,
            "Connecting smart push protocol web socket",
        );

        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = push_url.into_client_request().int_err()?;
        request.headers_mut().append(
            OdfSmtpVersion::name(),
            http::HeaderValue::from(SMART_TRANSFER_PROTOCOL_VERSION),
        );
        if let Some(access_token) = maybe_access_token {
            request.headers_mut().append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
            );
        }

        let mut ws_stream = match connect_async(request).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(e) => {
                tracing::debug!("Failed to connect to push URL: {}", e);
                return Err(map_tungstenite_error(e, http_dst_url));
            }
        };

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
                return Err(SyncError::Internal(e.int_err()));
            }
        };

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
                return Err(match e {
                    PushClientError::OverwriteSeedBlock(err) => SyncError::OverwriteSeedBlock(err),
                    e => SyncError::Internal(e.int_err()),
                });
            }
        };

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
                return Err(match e {
                    PushClientError::RefCollision(err) => SyncError::RefCollision(err),
                    PushClientError::NameCollision(err) => SyncError::NameCollision(err),
                    _ => SyncError::Internal(e.int_err()),
                });
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
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
        use tokio_tungstenite::tungstenite::protocol::CloseFrame;
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
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type TungsteniteStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn read_payload<TMessagePayload: DeserializeOwned>(
    stream: &mut TungsteniteStream,
) -> Result<TMessagePayload, ReadMessageError> {
    use tokio_stream::StreamExt;
    match stream.next().await {
        Some(msg) => match msg {
            Ok(Message::Text(raw_message)) => {
                ws_common::parse_payload::<TMessagePayload>(raw_message.as_str())
            }
            Ok(Message::Close(close_frame_maybe)) => {
                if let Some(close_frame) = close_frame_maybe
                    && close_frame.code == CloseCode::Error
                {
                    return ws_common::parse_payload::<TMessagePayload>(&close_frame.reason);
                }
                Err(ReadMessageError::ClientDisconnected)
            }
            Ok(_) => Err(ReadMessageError::NonTextMessageReceived),
            Err(e) => Err(ReadMessageError::SocketError(Box::new(e))),
        },
        None => Err(ReadMessageError::ClientDisconnected),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn write_payload<TMessagePayload: Serialize>(
    socket: &mut TungsteniteStream,
    payload: TMessagePayload,
) -> Result<(), WriteMessageError> {
    let payload_as_json_string = ws_common::payload_to_json::<TMessagePayload>(payload)?;

    let message = Message::Text(payload_as_json_string.into());
    let send_result = socket.send(message).await;
    match send_result {
        Ok(_) => Ok(()),
        Err(e) => Err(WriteMessageError::SocketError(Box::new(e))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_tungstenite_error(error: TungsteniteError, dataset_endpoint: &Url) -> SyncError {
    use http::StatusCode;

    if let TungsteniteError::Http(response) = &error {
        match response.status() {
            StatusCode::FORBIDDEN => {
                return SyncError::Access(odf::AccessError::Unauthorized(Box::new(error)));
            }
            StatusCode::UNAUTHORIZED => {
                return SyncError::Access(odf::AccessError::Unauthenticated(Box::new(error)))
            }
            StatusCode::NOT_FOUND => {
                return DatasetAnyRefUnresolvedError::new(dataset_endpoint).into();
            }
            StatusCode::BAD_REQUEST => {
                if let Some(body) = response.body().as_ref()
                    && let Ok(body_message) = std::str::from_utf8(body)
                {
                    return SyncError::Internal(body_message.int_err());
                }
            }
            _ => {}
        }
    }

    SyncError::Internal(error.int_err())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
