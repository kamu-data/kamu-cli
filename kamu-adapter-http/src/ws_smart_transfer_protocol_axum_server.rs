// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use axum::extract::ws::Message;
use opendatafabric::MetadataBlock;
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use url::Url;

use crate::{
    dataset_protocol_helper::*,
    messages::*,
    ws_common::{self, ReadMessageError, WriteMessageError},
};
use kamu::domain::*;

/////////////////////////////////////////////////////////////////////////////////

async fn read_payload<TMessagePayload: DeserializeOwned>(
    socket: &mut axum::extract::ws::WebSocket,
) -> Result<TMessagePayload, ReadMessageError> {
    match socket.recv().await {
        Some(msg) => match msg {
            Ok(Message::Text(raw_message)) => {
                ws_common::parse_payload::<TMessagePayload>(raw_message.as_str())
            }
            Ok(Message::Close(_)) => Err(ReadMessageError::Closed),
            Ok(_) => Err(ReadMessageError::NonTextMessageReceived),
            Err(e) => Err(ReadMessageError::SocketError(Box::new(e))),
        },
        None => Err(ReadMessageError::ClientDisconnected),
    }
}

/////////////////////////////////////////////////////////////////////////////////

async fn write_payload<TMessagePayload: Serialize>(
    socket: &mut axum::extract::ws::WebSocket,
    payload: TMessagePayload,
) -> Result<(), WriteMessageError> {
    let payload_as_json_string = ws_common::payload_to_json::<TMessagePayload>(payload)?;

    let message = axum::extract::ws::Message::Text(payload_as_json_string);
    let send_result = socket.send(message).await;
    match send_result {
        Ok(_) => Ok(()),
        Err(e) => Err(WriteMessageError::SocketError(Box::new(e))),
    }
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
enum SmartProtocolPullServerError {
    #[error(transparent)]
    PullRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PullResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    PullMetadataRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PullMetadataResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    PullObjectRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PullObjectResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
enum SmartProtocolPushServerError {
    #[error(transparent)]
    PushRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PushResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushMetadataRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PushMetadataResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushObjectRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PushObjectResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    PushCompleteRequestReadFailed(ReadMessageError),

    #[error(transparent)]
    PushCompleteResponseWriteFailed(WriteMessageError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////

async fn handle_pull_request_initiation(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
) -> Result<DatasetPullRequest, SmartProtocolPullServerError> {
    let pull_request = read_payload::<DatasetPullRequest>(socket)
        .await
        .map_err(|e| SmartProtocolPullServerError::PullRequestReadFailed(e))?;

    tracing::debug!(
        "Pull client sent a pull request: beginAfter={:?} stopAt={:?}",
        pull_request
            .begin_after
            .as_ref()
            .map(|ba| ba.to_string())
            .ok_or("None"),
        pull_request
            .stop_at
            .as_ref()
            .map(|sa| sa.to_string())
            .ok_or("None")
    );

    let metadata_chain = dataset.as_metadata_chain();
    let head = metadata_chain
        .get_ref(&BlockRef::Head)
        .await
        .map_err(|e| SmartProtocolPullServerError::Internal(e.int_err()))?;

    let size_estimate_result = prepare_dataset_transfer_estimate(
        dataset.as_metadata_chain(),
        pull_request.stop_at.as_ref().or(Some(&head)).unwrap(),
        pull_request.begin_after.as_ref(),
    )
    .await;

    write_payload::<DatasetPullResponse>(
        socket,
        match size_estimate_result {
            Ok(size_estimate) => {
                tracing::debug!("Sending size estimate: {:?}", size_estimate);
                DatasetPullResponse::Ok(DatasetPullSuccessResponse { size_estimate })
            }
            Err(PrepareDatasetTransferEstimateError::InvalidInterval(e)) => {
                tracing::debug!("Sending invalid interval error: {:?}", e);
                DatasetPullResponse::Err(DatasetPullRequestError::InvalidInterval {
                    head: e.head.clone(),
                    tail: e.tail.clone(),
                })
            }
            Err(PrepareDatasetTransferEstimateError::Internal(e)) => {
                tracing::debug!("Sending internal error: {:?}", e);
                DatasetPullResponse::Err(DatasetPullRequestError::Internal(DatasetInternalError {
                    error_message: e.to_string(),
                }))
            }
        },
    )
    .await
    .map_err(|e| SmartProtocolPullServerError::PullResponseWriteFailed(e))?;

    Ok(pull_request)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_pull_metadata_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    pull_request: DatasetPullRequest,
) -> Result<bool, SmartProtocolPullServerError> {
    let maybe_pull_metadata_request = read_payload::<DatasetPullMetadataRequest>(socket).await;

    match maybe_pull_metadata_request {
        Ok(_) => {
            tracing::debug!("Pull client sent a pull metadata request");

            let metadata_chain = dataset.as_metadata_chain();
            let head = metadata_chain
                .get_ref(&BlockRef::Head)
                .await
                .map_err(|e| SmartProtocolPullServerError::Internal(e.int_err()))?;

            let metadata_batch = prepare_dataset_metadata_batch(
                dataset.as_metadata_chain(),
                pull_request.stop_at.as_ref().or(Some(&head)).unwrap(),
                pull_request.begin_after.as_ref(),
            )
            .await
            .map_err(|e| SmartProtocolPullServerError::Internal(e))?;

            tracing::debug!(
                "Metadata batch of {} blocks formed, payload size {} bytes",
                metadata_batch.objects_count,
                metadata_batch.payload.len()
            );

            write_payload::<DatasetMetadataPullResponse>(
                socket,
                DatasetMetadataPullResponse {
                    blocks: metadata_batch,
                },
            )
            .await
            .map_err(|e| SmartProtocolPullServerError::PullMetadataResponseWriteFailed(e))?;

            Ok(true)
        }
        Err(ReadMessageError::Closed) => Ok(false),
        Err(e) => Err(SmartProtocolPullServerError::PullMetadataRequestReadFailed(
            e,
        )),
    }
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_pull_objects_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    dataset_url: &Url,
) -> Result<bool, SmartProtocolPullServerError> {
    let maybe_pull_objects_transfer_request =
        read_payload::<DatasetPullObjectsTransferRequest>(socket).await;

    match maybe_pull_objects_transfer_request {
        Ok(request) => {
            tracing::debug!(
                "Pull client sent a pull objects request for {} objects",
                request.object_files.len(),
            );

            let mut object_transfer_strategies: Vec<PullObjectTransferStrategy> = Vec::new();
            for r in request.object_files {
                let transfer_strategy =
                    prepare_pull_object_transfer_strategy(dataset, &r, dataset_url)
                        .await
                        .map_err(|e| SmartProtocolPullServerError::Internal(e))?;

                object_transfer_strategies.push(transfer_strategy);
            }

            tracing::debug!("Object transfer strategies defined");

            write_payload::<DatasetPullObjectsTransferResponse>(
                socket,
                DatasetPullObjectsTransferResponse {
                    object_transfer_strategies,
                },
            )
            .await
            .map_err(|e| SmartProtocolPullServerError::PullObjectResponseWriteFailed(e))?;

            Ok(true)
        }
        Err(ReadMessageError::Closed) => Ok(false),
        Err(e) => Err(SmartProtocolPullServerError::PullObjectRequestReadFailed(e)),
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_pull_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    dataset: Arc<dyn Dataset>,
    dataset_url: Url,
) {
    let pull_request = match handle_pull_request_initiation(&mut socket, dataset.as_ref()).await {
        Ok(pull_request) => pull_request,
        Err(e) => {
            tracing::debug!("Pull process aborted with error: {}", e);
            return;
        }
    };

    let received_pull_metadata_request =
        match try_handle_pull_metadata_request(&mut socket, dataset.as_ref(), pull_request).await {
            Ok(received) => received,
            Err(e) => {
                tracing::debug!("Pull process aborted with error: {}", e);
                return;
            }
        };

    if received_pull_metadata_request {
        loop {
            let should_continue =
                match try_handle_pull_objects_request(&mut socket, dataset.as_ref(), &dataset_url)
                    .await
                {
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

/////////////////////////////////////////////////////////////////////////////////

async fn handle_push_request_initiation(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
) -> Result<DatasetPushRequest, SmartProtocolPushServerError> {
    let push_request = read_payload::<DatasetPushRequest>(socket)
        .await
        .map_err(|e| SmartProtocolPushServerError::PushRequestReadFailed(e))?;

    tracing::debug!(
        "Push client sent a push request: currentHead={:?} sizeEstimate={:?}",
        push_request
            .current_head
            .as_ref()
            .map(|ba| ba.to_string())
            .ok_or("None"),
        push_request.size_estimate
    );

    // TODO: consider size estimate and maybe cancel too large pushes

    let metadata_chain = dataset.as_metadata_chain();
    let actual_head = match metadata_chain.get_ref(&BlockRef::Head).await {
        Ok(head) => Some(head),
        Err(kamu::domain::GetRefError::NotFound(_)) => None,
        Err(e) => return Err(SmartProtocolPushServerError::Internal(e.int_err())),
    };

    let response = if push_request.current_head == actual_head {
        DatasetPushResponse::Ok(DatasetPushRequestAccepted {})
    } else {
        DatasetPushResponse::Err(DatasetPushRequestError::InvalidHead {
            suggested_head: push_request.current_head.clone(),
            actual_head,
        })
    };

    write_payload::<DatasetPushResponse>(socket, response)
        .await
        .map_err(|e| SmartProtocolPushServerError::PushResponseWriteFailed(e))?;

    Ok(push_request)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_push_metadata_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    push_request: DatasetPushRequest,
) -> Result<Vec<MetadataBlock>, SmartProtocolPushServerError> {
    let maybe_push_metadata_request = read_payload::<DatasetPushMetadataRequest>(socket).await;

    match maybe_push_metadata_request {
        Ok(push_metadata_request) => {
            tracing::debug!(
                "Obtained object batch with {} objects of type {:?}, media type {:?}, encoding {:?}, bytes in compressed blocks {}",
                push_metadata_request.new_blocks.objects_count,
                push_metadata_request.new_blocks.object_type,
                push_metadata_request.new_blocks.media_type,
                push_metadata_request.new_blocks.encoding,
                push_metadata_request.new_blocks.payload.len()
            );

            assert_eq!(
                push_request.size_estimate.num_blocks,
                push_metadata_request.new_blocks.objects_count
            );

            let new_blocks =
                dataset_import_pushed_metadata(dataset, push_metadata_request.new_blocks).await;

            write_payload::<DatasetPushMetadataAccepted>(socket, DatasetPushMetadataAccepted {})
                .await
                .map_err(|e| SmartProtocolPushServerError::PushMetadataResponseWriteFailed(e))?;

            Ok(new_blocks)
        }
        Err(ReadMessageError::Closed) => Ok(vec![]),
        Err(e) => Err(SmartProtocolPushServerError::PushMetadataRequestReadFailed(
            e,
        )),
    }
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_push_objects_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    dataset_url: &Url,
) -> Result<bool, SmartProtocolPushServerError> {
    let request = read_payload::<DatasetPushObjectsTransferRequest>(socket)
        .await
        .map_err(|e| SmartProtocolPushServerError::PushObjectRequestReadFailed(e))?;

    tracing::debug!(
        "Push client sent a push objects request for {} objects",
        request.object_files.len(),
    );

    let mut object_transfer_strategies: Vec<PushObjectTransferStrategy> = Vec::new();
    for r in request.object_files {
        let transfer_strategy = prepare_push_object_transfer_strategy(dataset, &r, &dataset_url)
            .await
            .map_err(|e| SmartProtocolPushServerError::Internal(e))?;

        object_transfer_strategies.push(transfer_strategy);
    }

    tracing::debug!("Object transfer strategies defined");

    write_payload::<DatasetPushObjectsTransferResponse>(
        socket,
        DatasetPushObjectsTransferResponse {
            object_transfer_strategies,
        },
    )
    .await
    .map_err(|e| SmartProtocolPushServerError::PushObjectResponseWriteFailed(e))?;

    Ok(request.is_truncated)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_push_complete(
    socket: &mut axum::extract::ws::WebSocket,
    dataset_builder: &dyn DatasetBuilder,
) -> Result<(), SmartProtocolPushServerError> {
    read_payload::<DatasetPushComplete>(socket)
        .await
        .map_err(|e| SmartProtocolPushServerError::PushCompleteRequestReadFailed(e))?;

    tracing::debug!("Push client sent a complete request. Commiting the dataset");

    match dataset_builder.finish().await {
        Ok(_) => {}
        Err(e) => {
            tracing::debug!("Committing dataset failed with error: {}", e);
            return Err(SmartProtocolPushServerError::Internal(e.int_err()));
        }
    }

    tracing::debug!("Sending complete confirmation");

    write_payload::<DatasetPushCompleteConfirmed>(socket, DatasetPushCompleteConfirmed {})
        .await
        .map_err(|e| SmartProtocolPushServerError::PushCompleteResponseWriteFailed(e))?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////

async fn discard_dataset_building_on_error(
    dataset_builder: &dyn DatasetBuilder,
    e: SmartProtocolPushServerError,
) {
    tracing::debug!("Push process aborted with error: {}", e);
    match dataset_builder.discard().await {
        Ok(_) => {}
        Err(e) => {
            tracing::debug!("Discard dataset build error: {}", e);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    dataset_builder: Arc<Box<dyn DatasetBuilder>>,
    dataset_url: Url,
) {
    let dataset = dataset_builder.as_dataset();
    let push_request = match handle_push_request_initiation(&mut socket, dataset).await {
        Ok(push_request) => push_request,
        Err(e) => {
            discard_dataset_building_on_error(dataset_builder.as_ref().as_ref(), e).await;
            return;
        }
    };

    let _new_blocks =
        match try_handle_push_metadata_request(&mut socket, dataset, push_request).await {
            Ok(received) => received,
            Err(e) => {
                discard_dataset_building_on_error(dataset_builder.as_ref().as_ref(), e).await;
                return;
            }
        };

    loop {
        let should_continue =
            match try_handle_push_objects_request(&mut socket, dataset, &dataset_url).await {
                Ok(should_continue) => should_continue,
                Err(e) => {
                    discard_dataset_building_on_error(dataset_builder.as_ref().as_ref(), e).await;
                    return;
                }
            };
        if !should_continue {
            break;
        }
    }

    if let Err(e) = try_handle_push_complete(&mut socket, dataset_builder.as_ref().as_ref()).await {
        discard_dataset_building_on_error(dataset_builder.as_ref().as_ref(), e).await;
        return;
    }

    // Success
    tracing::debug!("Push process success");
}

/////////////////////////////////////////////////////////////////////////////////
