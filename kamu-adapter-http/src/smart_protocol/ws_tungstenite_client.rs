// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::component;
use futures::SinkExt;
use opendatafabric::{AsTypedBlock, Multihash};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use tokio::net::TcpStream;
use url::Url;

use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use kamu::infra::utils::smart_transfer_protocol::{DatasetFactoryFn, SmartTransferProtocolClient};
use kamu::{domain::*, infra::utils::smart_transfer_protocol::ObjectTransferOptions};

use crate::{
    smart_protocol::{dataset_helper::*, errors::*, messages::*, phases::*},
    ws_common::{self, ReadMessageError, WriteMessageError},
};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct WsSmartTransferProtocolClient {}

/////////////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    async fn pull_send_request(
        &self,
        socket: &mut TungsteniteStream,
        dst_head: Option<Multihash>,
    ) -> Result<DatasetPullSuccessResponse, PullClientError> {
        let pull_request_message = DatasetPullRequest {
            begin_after: dst_head,
            stop_at: None,
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
                    num_blocks = % success.size_estimate.num_blocks,
                    bytes_in_raw_locks = % success.size_estimate.bytes_in_raw_blocks,
                    num_objects = % success.size_estimate.num_objects,
                    bytes_in_raw_objecs = % success.size_estimate.bytes_in_raw_objects,
                    "Pull response estimate",
                );
                Ok(success)
            }
            Err(DatasetPullRequestError::Internal(e)) => Err(PullClientError::Internal(
                InternalError::new(Box::new(ClientInternalError::new(e.error_message.as_str()))),
            )),
            Err(DatasetPullRequestError::InvalidInterval(DatasetPullInvalidIntervalError {
                head,
                tail,
            })) => Err(PullClientError::InvalidInterval(InvalidIntervalError {
                head: head.clone(),
                tail: tail.clone(),
            })),
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
            objects_count = % dataset_metadata_pull_response.blocks.objects_count,
            object_type = ? dataset_metadata_pull_response.blocks.object_type,
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
                )
            });

        Ok(dataset_objects_pull_response)
    }

    async fn push_send_request(
        &self,
        socket: &mut TungsteniteStream,
        size_estimate: TransferSizeEstimate,
        dst_head: Option<&Multihash>,
    ) -> Result<DatasetPushRequestAccepted, PushClientError> {
        let push_request_message = DatasetPushRequest {
            current_head: dst_head.cloned(),
            size_estimate,
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
            Err(DatasetPushRequestError::Internal(e)) => Err(PushClientError::Internal(
                InternalError::new(Box::new(ClientInternalError::new(e.error_message.as_str()))),
            )),
            Err(DatasetPushRequestError::InvalidHead(e)) => {
                Err(PushClientError::InvalidHead(RefCASError {
                    actual: e.actual_head.clone(),
                    expected: e.expected_head.clone(),
                    reference: BlockRef::Head,
                }))
            }
        }
    }

    async fn push_send_metadata_request(
        &self,
        socket: &mut TungsteniteStream,
        src_dataset: &dyn Dataset,
        src_head: &Multihash,
        dst_head: Option<&Multihash>,
    ) -> Result<DatasetPushMetadataAccepted, PushClientError> {
        tracing::debug!("Sending push metadata request");

        let metadata_batch =
            prepare_dataset_metadata_batch(src_dataset.as_metadata_chain(), src_head, dst_head)
                .await
                .int_err()?;

        tracing::debug!(
            objects_count = % metadata_batch.objects_count,
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

        let dataset_metadata_push_response = read_payload::<DatasetPushMetadataAccepted>(socket)
            .await
            .map_err(|e| {
                PushClientError::ReadFailed(PushReadError::new(e, PushPhase::MetadataRequest))
            })?;

        Ok(dataset_metadata_push_response)
    }

    async fn push_send_objects_request(
        &self,
        socket: &mut TungsteniteStream,
        object_files: Vec<ObjectFileReference>,
    ) -> Result<DatasetPushObjectsTransferResponse, PushClientError> {
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
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SmartTransferProtocolClient for WsSmartTransferProtocolClient {
    async fn pull_protocol_client_flow(
        &self,
        http_src_url: &Url,
        dst: Option<Arc<dyn Dataset>>,
        dst_factory: Option<DatasetFactoryFn>,
        listener: Arc<dyn SyncListener>,
        transfer_options: ObjectTransferOptions,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let mut pull_url = http_src_url.join("pull").unwrap();
        let pull_url_res = pull_url.set_scheme("ws");
        assert!(pull_url_res.is_ok());

        tracing::debug!("Connecting to pull URL: {}", pull_url);

        let mut ws_stream = match connect_async(pull_url).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(e) => {
                tracing::debug!("Failed to connect to pull URL: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        let dst_head = if let Some(dst) = &dst {
            match dst.as_metadata_chain().get_ref(&BlockRef::Head).await {
                Ok(head) => Ok(Some(head)),
                Err(GetRefError::NotFound(_)) => Ok(None),
                Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            }?
        } else {
            None
        };

        let dataset_pull_result = match self
            .pull_send_request(&mut ws_stream, dst_head.clone())
            .await
        {
            Ok(dataset_pull_result) => dataset_pull_result,
            Err(e) => {
                tracing::debug!("Pull process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        let sync_result = if dataset_pull_result.size_estimate.num_blocks > 0 {
            let dataset_pull_metadata_response =
                match self.pull_send_metadata_request(&mut ws_stream).await {
                    Ok(dataset_pull_metadata_response) => Ok(dataset_pull_metadata_response),
                    Err(e) => {
                        tracing::debug!("Pull process aborted with error: {}", e);
                        Err(SyncError::Internal(e.int_err()))
                    }
                }?;

            let mut new_blocks = decode_metadata_batch(dataset_pull_metadata_response.blocks)
                .await
                .int_err()?;

            // Create destination dataset if not exists
            let dst = if let Some(dst) = dst {
                dst
            } else {
                let (first_hash, first_block) = new_blocks.pop_front().unwrap();
                let seed_block = first_block
                    .into_typed()
                    .ok_or_else(|| CorruptedSourceError {
                        message: "First metadata block is not Seed".to_owned(),
                        source: None,
                    })?;
                let create_result = (dst_factory.unwrap())(seed_block).await.int_err()?;
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
                        |s| async move { dataset_import_object_file(dst_ref, &s).await },
                    )
                    .await?;
            }

            dataset_append_metadata(dst.as_ref(), new_blocks)
                .await
                .map_err(|e| {
                    tracing::debug!("Appending dataset metadata failed with error: {}", e);
                    SyncError::Internal(e.int_err())
                })?;

            let new_dst_head = dst
                .as_metadata_chain()
                .get_ref(&BlockRef::Head)
                .await
                .int_err()?;

            SyncResult::Updated {
                old_head: dst_head,
                new_head: new_dst_head,
                num_blocks: dataset_pull_result.size_estimate.num_blocks as usize,
            }
        } else {
            SyncResult::UpToDate
        };

        use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
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
        src: Arc<dyn Dataset>,
        http_dst_url: &Url,
        dst_head: Option<&Multihash>,
        listener: Arc<dyn SyncListener>,
        transfer_options: ObjectTransferOptions,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let src_head = src
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .int_err()?;

        let size_estimate =
            prepare_dataset_transfer_estimate(src.as_metadata_chain(), &src_head, dst_head)
                .await
                .map_err(|e| SyncError::Internal(e.int_err()))?;

        let num_blocks = size_estimate.num_blocks;
        if num_blocks == 0 {
            return Ok(SyncResult::UpToDate);
        }

        let mut push_url = http_dst_url.join("push").unwrap();
        let push_url_res = push_url.set_scheme("ws");
        assert!(push_url_res.is_ok());

        tracing::debug!("Connecting to push URL: {}", push_url);

        let mut ws_stream = match connect_async(push_url).await {
            Ok((ws_stream, _)) => ws_stream,
            Err(e) => {
                tracing::debug!("Failed to connect to push URL: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        match self
            .push_send_request(&mut ws_stream, size_estimate, dst_head)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        match self
            .push_send_metadata_request(&mut ws_stream, src.as_ref(), &src_head, dst_head)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        let missing_objects =
            match collect_object_references_from_interval(src.as_ref(), &src_head, dst_head, false)
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
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        let src_ref = src.as_ref();
        use futures::stream::{StreamExt, TryStreamExt};
        futures::stream::iter(push_objects_response.object_transfer_strategies)
            .map(Ok)
            .try_for_each_concurrent(
                /* limit */ transfer_options.max_parallel_transfers,
                |s| async move { dataset_export_object_file(src_ref, &s).await },
            )
            .await?;

        match self.push_send_complete_request(&mut ws_stream).await {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
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
            num_blocks: num_blocks as usize,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

type TungsteniteStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/////////////////////////////////////////////////////////////////////////////////

async fn read_payload<TMessagePayload: DeserializeOwned>(
    stream: &mut TungsteniteStream,
) -> Result<TMessagePayload, ReadMessageError> {
    use tokio_stream::StreamExt;
    match stream.next().await {
        Some(msg) => match msg {
            Ok(Message::Text(raw_message)) => {
                ws_common::parse_payload::<TMessagePayload>(raw_message.as_str())
            }
            Ok(_) => Err(ReadMessageError::NonTextMessageReceived),
            Err(e) => Err(ReadMessageError::SocketError(Box::new(e))),
        },
        None => Err(ReadMessageError::ClientDisconnected),
    }
}

/////////////////////////////////////////////////////////////////////////////////

async fn write_payload<TMessagePayload: Serialize>(
    socket: &mut TungsteniteStream,
    payload: TMessagePayload,
) -> Result<(), WriteMessageError> {
    let payload_as_json_string = ws_common::payload_to_json::<TMessagePayload>(payload)?;

    let message = Message::Text(payload_as_json_string);
    let send_result = socket.send(message).await;
    match send_result {
        Ok(_) => Ok(()),
        Err(e) => Err(WriteMessageError::SocketError(Box::new(e))),
    }
}

/////////////////////////////////////////////////////////////////////////////////
