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
use opendatafabric::Multihash;
use serde::{de::DeserializeOwned, Serialize};
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpStream;
use url::Url;

use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

use kamu::domain::*;
use kamu::infra::utils::smart_transfer_protocol::SmartTransferProtocolClient;

use crate::{
    dataset_protocol_helper::*,
    messages::*,
    ws_common::{self, ReadMessageError, WriteMessageError},
};

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct WsSmartTransferProtocolClient {}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
enum SmartProtocolPullClientError {
    #[error(transparent)]
    PullRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PullResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PullResponseInvalidInterval(InvalidIntervalError),

    #[error(transparent)]
    PullMetadataRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PullMetadataResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PullObjectRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PullObjectResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
enum SmartProtocolPushClientError {
    #[error(transparent)]
    PushRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PushResponseInvalidHead(InvalidHeadError),

    #[error(transparent)]
    PushMetadataRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushMetadataResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PushObjectRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushObjectResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    PushCompleteRequestWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushCompleteResponseReadFailed(ReadMessageError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    async fn pull_send_request(
        &self,
        socket: &mut TungsteniteStream,
        dst_head: Option<Multihash>,
    ) -> Result<DatasetPullSuccessResponse, SmartProtocolPullClientError> {
        let pull_request_message = DatasetPullRequest {
            begin_after: dst_head,
            stop_at: None,
        };

        tracing::debug!("Sending pull request: {:?}", pull_request_message);

        let write_result = write_payload(socket, pull_request_message).await;
        if let Err(e) = write_result {
            return Err(SmartProtocolPullClientError::PullRequestWriteFailed(e));
        }

        tracing::debug!("Reading pull request response");

        let dataset_pull_response = match read_payload::<DatasetPullResponse>(socket).await {
            Ok(dataset_pull_response) => dataset_pull_response,
            Err(e) => return Err(SmartProtocolPullClientError::PullResponseReadFailed(e)),
        };

        match dataset_pull_response {
            Ok(success) => {
                tracing::debug!(
                    "Pull response estimate: {} blocks to synchronize of {} total bytes, {} data objects of {} total bytes", 
                    success.size_estimation.num_blocks,
                    success.size_estimation.bytes_in_raw_blocks,
                    success.size_estimation.num_objects,
                    success.size_estimation.bytes_in_raw_objects,
                );
                Ok(success)
            }
            Err(DatasetPullRequestError::Internal(e)) => {
                Err(SmartProtocolPullClientError::Internal(InternalError::new(
                    Box::new(ClientInternalError::new(e.error_message.as_str())),
                )))
            }
            Err(DatasetPullRequestError::InvalidInterval { head, tail }) => Err(
                SmartProtocolPullClientError::PullResponseInvalidInterval(InvalidIntervalError {
                    head: head.clone(),
                    tail: tail.clone(),
                }),
            ),
        }
    }

    async fn pull_send_metadata_request(
        &self,
        socket: &mut TungsteniteStream,
    ) -> Result<DatasetMetadataPullResponse, SmartProtocolPullClientError> {
        let pull_metadata_request = DatasetPullMetadataRequest {};
        tracing::debug!("Sending pull metadata request");

        let write_result_metadata = write_payload(socket, pull_metadata_request).await;
        if let Err(e) = write_result_metadata {
            return Err(SmartProtocolPullClientError::PullMetadataRequestWriteFailed(e));
        }

        tracing::debug!("Reading pull metadata request response");

        let dataset_metadata_pull_response =
            match read_payload::<DatasetMetadataPullResponse>(socket).await {
                Ok(dataset_metadata_pull_response) => dataset_metadata_pull_response,
                Err(e) => {
                    return Err(SmartProtocolPullClientError::PullMetadataResponseReadFailed(e));
                }
            };

        tracing::debug!(
            "Obtained object batch with {} objects of type {:?}, media type {:?}, encoding {:?}, bytes in compressed blocks {}",
            dataset_metadata_pull_response.blocks.objects_count,
            dataset_metadata_pull_response.blocks.object_type,
            dataset_metadata_pull_response.blocks.media_type,
            dataset_metadata_pull_response.blocks.encoding,
            dataset_metadata_pull_response.blocks.payload.len()
        );

        Ok(dataset_metadata_pull_response)
    }

    async fn pull_send_objects_request(
        &self,
        socket: &mut TungsteniteStream,
        object_files: Vec<ObjectFileReference>,
    ) -> Result<DatasetPullObjectsTransferResponse, SmartProtocolPullClientError> {
        let pull_objects_request = DatasetPullObjectsTransferRequest { object_files };
        tracing::debug!(
            "Sending pull objects request for {} objects",
            pull_objects_request.object_files.len()
        );

        let write_result_objects = write_payload(socket, pull_objects_request).await;
        if let Err(e) = write_result_objects {
            return Err(SmartProtocolPullClientError::PullObjectRequestWriteFailed(
                e,
            ));
        }

        tracing::debug!("Reading pull objects request response");

        let dataset_objects_pull_response =
            match read_payload::<DatasetPullObjectsTransferResponse>(socket).await {
                Ok(dataset_objects_pull_response) => dataset_objects_pull_response,
                Err(e) => {
                    return Err(SmartProtocolPullClientError::PullObjectResponseReadFailed(
                        e,
                    ));
                }
            };

        tracing::debug!(
            "Obtained transfer strategies for {} objects",
            dataset_objects_pull_response
                .object_transfer_strategies
                .len()
        );

        dataset_objects_pull_response
            .object_transfer_strategies
            .iter()
            .for_each(|s| {
                tracing::debug!(
                    "Object file {} needs to download from {} via {:?} method, expires at {:?}",
                    s.object_file.physical_hash.to_string(),
                    s.download_from.url,
                    s.pull_strategy,
                    s.download_from.expires_at
                )
            });

        Ok(dataset_objects_pull_response)
    }

    async fn push_send_request(
        &self,
        socket: &mut TungsteniteStream,
        size_estimation: TransferSizeEstimation,
        dst_head: Option<&Multihash>,
    ) -> Result<DatasetPushRequestAccepted, SmartProtocolPushClientError> {
        let push_request_message = DatasetPushRequest {
            current_head: dst_head.cloned(),
            size_estimation,
        };

        tracing::debug!("Sending push request: {:?}", push_request_message);

        let write_result = write_payload(socket, push_request_message).await;
        if let Err(e) = write_result {
            return Err(SmartProtocolPushClientError::PushRequestWriteFailed(e));
        }

        tracing::debug!("Reading push request response");

        let dataset_push_response = match read_payload::<DatasetPushResponse>(socket).await {
            Ok(dataset_push_response) => dataset_push_response,
            Err(e) => return Err(SmartProtocolPushClientError::PushResponseReadFailed(e)),
        };

        match dataset_push_response {
            Ok(success) => {
                tracing::debug!("Push response accepted");
                Ok(success)
            }
            Err(DatasetPushRequestError::Internal(e)) => {
                Err(SmartProtocolPushClientError::Internal(InternalError::new(
                    Box::new(ClientInternalError::new(e.error_message.as_str())),
                )))
            }
            Err(DatasetPushRequestError::InvalidHead {
                suggested_head,
                actual_head,
            }) => Err(SmartProtocolPushClientError::PushResponseInvalidHead(
                InvalidHeadError {
                    suggested_head: suggested_head.clone(),
                    actual_head: actual_head.clone(),
                },
            )),
        }
    }

    async fn push_send_metadata_request(
        &self,
        socket: &mut TungsteniteStream,
        src_dataset: &dyn Dataset,
        src_head: &Multihash,
        dst_head: Option<&Multihash>,
    ) -> Result<DatasetPushMetadataAccepted, SmartProtocolPushClientError> {
        tracing::debug!("Sending push metadata request");

        let metadata_batch = match prepare_dataset_metadata_batch(
            src_dataset.as_metadata_chain(),
            src_head,
            dst_head,
        )
        .await
        {
            Ok(metadata_batch) => metadata_batch,
            Err(e) => return Err(SmartProtocolPushClientError::Internal(e)),
        };

        tracing::debug!(
            "Metadata batch of {} blocks formed, payload size {} bytes",
            metadata_batch.objects_count,
            metadata_batch.payload.len()
        );

        let write_result_metadata = write_payload(
            socket,
            DatasetPushMetadataRequest {
                new_blocks: metadata_batch,
            },
        )
        .await;
        if let Err(e) = write_result_metadata {
            return Err(SmartProtocolPushClientError::PushMetadataRequestWriteFailed(e));
        }

        tracing::debug!("Reading push metadata response");

        let dataset_metadata_push_response =
            match read_payload::<DatasetPushMetadataAccepted>(socket).await {
                Ok(dataset_metadata_push_response) => dataset_metadata_push_response,
                Err(e) => {
                    return Err(SmartProtocolPushClientError::PushMetadataResponseReadFailed(e));
                }
            };

        Ok(dataset_metadata_push_response)
    }

    async fn push_send_objects_request(
        &self,
        socket: &mut TungsteniteStream,
        object_files: Vec<ObjectFileReference>,
    ) -> Result<DatasetPushObjectsTransferResponse, SmartProtocolPushClientError> {
        let push_objects_request = DatasetPushObjectsTransferRequest {
            object_files,
            is_truncated: false, // TODO: split on pages to avoid links expiry
        };
        tracing::debug!(
            "Sending push objects request for {} objects",
            push_objects_request.object_files.len()
        );

        let write_result_objects = write_payload(socket, push_objects_request).await;
        if let Err(e) = write_result_objects {
            return Err(SmartProtocolPushClientError::PushObjectRequestWriteFailed(
                e,
            ));
        }

        tracing::debug!("Reading push objects request response");

        let dataset_objects_push_response =
            match read_payload::<DatasetPushObjectsTransferResponse>(socket).await {
                Ok(dataset_objects_push_response) => dataset_objects_push_response,
                Err(e) => {
                    return Err(SmartProtocolPushClientError::PushObjectResponseReadFailed(
                        e,
                    ));
                }
            };

        tracing::debug!(
            "Obtained transfer strategies for {} objects",
            dataset_objects_push_response
                .object_transfer_strategies
                .len()
        );

        dataset_objects_push_response
            .object_transfer_strategies
            .iter()
            .for_each(|s| match s.upload_to.as_ref() {
                Some(transfer_url) => {
                    tracing::debug!(
                        "Object file {} needs to upload to {} via {:?} method, expires at {:?}",
                        s.object_file.physical_hash.to_string(),
                        transfer_url.url,
                        s.push_strategy,
                        transfer_url.expires_at
                    );
                }
                None => {
                    tracing::debug!(
                        "Object file {} does not need an upload",
                        s.object_file.physical_hash.to_string(),
                    );
                }
            });

        Ok(dataset_objects_push_response)
    }

    async fn push_send_complete_request(
        &self,
        socket: &mut TungsteniteStream,
    ) -> Result<DatasetPushCompleteConfirmed, SmartProtocolPushClientError> {
        tracing::debug!("Sending push complete request");

        let write_result = write_payload(socket, DatasetPushComplete {}).await;
        if let Err(e) = write_result {
            return Err(SmartProtocolPushClientError::PushCompleteRequestWriteFailed(e));
        }

        tracing::debug!("Reading push complete response");

        match read_payload::<DatasetPushCompleteConfirmed>(socket).await {
            Ok(complete_response) => Ok(complete_response),
            Err(e) => Err(SmartProtocolPushClientError::PushCompleteResponseReadFailed(e)),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SmartTransferProtocolClient for WsSmartTransferProtocolClient {
    async fn pull_protocol_client_flow(
        &self,
        http_src_url: &Url,
        dst: &dyn Dataset,
        listener: Arc<dyn SyncListener>,
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

        let dst_head_result = dst.as_metadata_chain().get_ref(&BlockRef::Head).await;
        let dst_head = match dst_head_result {
            Ok(head) => Ok(Some(head)),
            Err(GetRefError::NotFound(_)) => Ok(None),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

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

        let sync_result = if dataset_pull_result.size_estimation.num_blocks > 0 {
            let dataset_pull_metadata_response =
                match self.pull_send_metadata_request(&mut ws_stream).await {
                    Ok(dataset_pull_metadata_response) => Ok(dataset_pull_metadata_response),
                    Err(e) => {
                        tracing::debug!("Pull process aborted with error: {}", e);
                        Err(SyncError::Internal(e.int_err()))
                    }
                }?;

            let object_files_transfer_plan =
                dataset_import_pulled_metadata(dst, dataset_pull_metadata_response.blocks).await;

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

                use futures::stream::{StreamExt, TryStreamExt};
                futures::stream::iter(dataset_objects_pull_response.object_transfer_strategies)
                    .map(Ok)
                    .try_for_each_concurrent(
                        /* limit */ 4, // TODO: external configuration?
                        |s| async move { dataset_import_object_file(dst, &s).await },
                    )
                    .await?;
            }

            let new_dst_head = dst
                .as_metadata_chain()
                .get_ref(&BlockRef::Head)
                .await
                .int_err()?;

            SyncResult::Updated {
                old_head: dst_head,
                new_head: new_dst_head,
                num_blocks: dataset_pull_result.size_estimation.num_blocks as usize,
            }
        } else {
            SyncResult::UpToDate
        };

        use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
        ws_stream
            .close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: Cow::Borrowed("Client pull flow succeeded"),
            }))
            .await
            .int_err()?;

        Ok(sync_result)
    }

    async fn push_protocol_client_flow(
        &self,
        src: &dyn Dataset,
        http_dst_url: &Url,
        dst_head: Option<&Multihash>,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let src_head = src
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .int_err()?;

        let size_estimation =
            match prepare_dataset_transfer_estimaton(src.as_metadata_chain(), &src_head, dst_head)
                .await
            {
                Ok(size_estimation) => size_estimation,
                Err(e) => return Err(SyncError::Internal(e.int_err())),
            };

        if size_estimation.num_blocks == 0 {
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
            .push_send_request(&mut ws_stream, size_estimation, dst_head)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        match self
            .push_send_metadata_request(&mut ws_stream, src, &src_head, dst_head)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                tracing::debug!("Push process aborted with error: {}", e);
                return Err(SyncError::Internal(e.int_err()));
            }
        };

        let missing_objects =
            match collect_missing_object_references_from_interval(src, &src_head, dst_head).await {
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

        use futures::stream::{StreamExt, TryStreamExt};
        futures::stream::iter(push_objects_response.object_transfer_strategies)
            .map(Ok)
            .try_for_each_concurrent(
                /* limit */ 4, // TODO: external configuration?
                |s| async move { dataset_export_object_file(src, &s).await },
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
                reason: Cow::Borrowed("Client push flow succeeded"),
            }))
            .await
            .int_err()?;

        Ok(SyncResult::Updated {
            old_head: dst_head.cloned(),
            new_head: src_head,
            num_blocks: size_estimation.num_blocks as usize,
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

#[derive(Debug)]
struct ClientInternalError {
    details: String,
}

impl ClientInternalError {
    fn new(msg: &str) -> Self {
        Self {
            details: msg.to_string(),
        }
    }
}

impl fmt::Display for ClientInternalError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl std::error::Error for ClientInternalError {
    fn description(&self) -> &str {
        &self.details
    }
}

/////////////////////////////////////////////////////////////////////////////////
