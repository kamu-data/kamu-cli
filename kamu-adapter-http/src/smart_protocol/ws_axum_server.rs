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

use ::serde::de::DeserializeOwned;
use ::serde::Serialize;
use axum::extract::ws::Message;
use kamu::domain::*;
use opendatafabric::*;
use url::Url;

use crate::smart_protocol::dataset_helper::*;
use crate::smart_protocol::errors::*;
use crate::smart_protocol::messages::*;
use crate::smart_protocol::phases::*;
use crate::ws_common::{self, ReadMessageError, WriteMessageError};

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

async fn handle_pull_request_initiation(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
) -> Result<DatasetPullRequest, PullServerError> {
    let pull_request = read_payload::<DatasetPullRequest>(socket)
        .await
        .map_err(|e| {
            PullServerError::ReadFailed(PullReadError::new(e, PullPhase::InitialRequest))
        })?;

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
        .map_err(|e| PullServerError::Internal(e.int_err()))?;

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
                DatasetPullResponse::Err(DatasetPullRequestError::InvalidInterval(
                    DatasetPullInvalidIntervalError {
                        head: e.head.clone(),
                        tail: e.tail.clone(),
                    },
                ))
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
    .map_err(|e| PullServerError::WriteFailed(PullWriteError::new(e, PullPhase::InitialRequest)))?;

    Ok(pull_request)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_pull_metadata_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    pull_request: DatasetPullRequest,
) -> Result<bool, PullServerError> {
    let maybe_pull_metadata_request = read_payload::<DatasetPullMetadataRequest>(socket).await;

    match maybe_pull_metadata_request {
        Ok(_) => {
            tracing::debug!("Pull client sent a pull metadata request");

            let metadata_chain = dataset.as_metadata_chain();
            let head = metadata_chain
                .get_ref(&BlockRef::Head)
                .await
                .map_err(|e| PullServerError::Internal(e.int_err()))?;

            let metadata_batch = prepare_dataset_metadata_batch(
                dataset.as_metadata_chain(),
                pull_request.stop_at.as_ref().or(Some(&head)).unwrap(),
                pull_request.begin_after.as_ref(),
            )
            .await
            .int_err()?;

            tracing::debug!(
                num_blocks = % metadata_batch.objects_count,
                payload_size = % metadata_batch.payload.len(),
                "Metadata batch of blocks formed",
            );

            write_payload::<DatasetMetadataPullResponse>(
                socket,
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

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_pull_objects_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    dataset_url: &Url,
) -> Result<bool, PullServerError> {
    let maybe_pull_objects_transfer_request =
        read_payload::<DatasetPullObjectsTransferRequest>(socket).await;

    match maybe_pull_objects_transfer_request {
        Ok(request) => {
            tracing::debug!(
                objects_count = %request.object_files.len(),
                "Pull client sent a pull objects request"
            );

            let mut object_transfer_strategies: Vec<PullObjectTransferStrategy> = Vec::new();
            for r in request.object_files {
                let transfer_strategy =
                    prepare_pull_object_transfer_strategy(dataset, &r, dataset_url)
                        .await
                        .int_err()?;

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
    dataset: Option<Arc<dyn Dataset>>,
) -> Result<DatasetPushRequest, PushServerError> {
    let push_request = read_payload::<DatasetPushRequest>(socket)
        .await
        .map_err(|e| {
            PushServerError::ReadFailed(PushReadError::new(e, PushPhase::InitialRequest))
        })?;

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

    let actual_head = if let Some(dataset) = dataset {
        match dataset.as_metadata_chain().get_ref(&BlockRef::Head).await {
            Ok(head) => Some(head),
            Err(kamu::domain::GetRefError::NotFound(_)) => None,
            Err(e) => return Err(PushServerError::Internal(e.int_err())),
        }
    } else {
        None
    };

    let response = if push_request.current_head == actual_head {
        DatasetPushResponse::Ok(DatasetPushRequestAccepted {})
    } else {
        DatasetPushResponse::Err(DatasetPushRequestError::InvalidHead(
            DatasetPushInvalidHeadError {
                actual_head: push_request.current_head.clone(),
                expected_head: actual_head,
            },
        ))
    };

    write_payload::<DatasetPushResponse>(socket, response)
        .await
        .map_err(|e| {
            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::InitialRequest))
        })?;

    Ok(push_request)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_push_metadata_request(
    socket: &mut axum::extract::ws::WebSocket,
    push_request: DatasetPushRequest,
) -> Result<VecDeque<(Multihash, MetadataBlock)>, PushServerError> {
    let push_metadata_request = match read_payload::<DatasetPushMetadataRequest>(socket).await {
        Ok(push_metadata_request) => Ok(push_metadata_request),
        Err(e) => Err(PushServerError::ReadFailed(PushReadError::new(
            e,
            PushPhase::MetadataRequest,
        ))),
    }?;

    tracing::debug!(
        objects_count = %push_metadata_request.new_blocks.objects_count,
        object_type = ?push_metadata_request.new_blocks.object_type,
        media_type = %push_metadata_request.new_blocks.media_type,
        encoding = %push_metadata_request.new_blocks.encoding,
        payload_length = %push_metadata_request.new_blocks.payload.len(),
        "Obtained compressed object batch",
    );

    assert_eq!(
        push_request.size_estimate.num_blocks,
        push_metadata_request.new_blocks.objects_count
    );

    let new_blocks = decode_metadata_batch(push_metadata_request.new_blocks)
        .await
        .int_err()?;

    write_payload::<DatasetPushMetadataAccepted>(socket, DatasetPushMetadataAccepted {})
        .await
        .map_err(|e| {
            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::MetadataRequest))
        })?;

    Ok(new_blocks)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_push_objects_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    dataset_url: &Url,
) -> Result<bool, PushServerError> {
    let request = read_payload::<DatasetPushObjectsTransferRequest>(socket)
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
        let transfer_strategy = prepare_push_object_transfer_strategy(dataset, &dataset_url, &r)
            .await
            .map_err(|e| PushServerError::Internal(e))?;

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
    .map_err(|e| PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::ObjectsRequest)))?;

    loop {
        let progress = read_payload::<DatasetPushObjectsUploadInProgress>(socket)
            .await
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
            ObjectsUploadProgressDetails::Complete => break,
        }
    }

    Ok(request.is_truncated)
}

/////////////////////////////////////////////////////////////////////////////////

async fn try_handle_push_complete(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: Option<Arc<dyn Dataset>>,
    new_blocks: VecDeque<(Multihash, MetadataBlock)>,
) -> Result<(), PushServerError> {
    read_payload::<DatasetPushComplete>(socket)
        .await
        .map_err(|e| {
            PushServerError::ReadFailed(PushReadError::new(e, PushPhase::CompleteRequest))
        })?;

    tracing::debug!("Push client sent a complete request. Commiting the dataset");

    if new_blocks.len() > 0 {
        dataset_append_metadata(dataset.unwrap().as_ref(), new_blocks)
            .await
            .map_err(|e| {
                tracing::debug!("Appending dataset metadata failed with error: {}", e);
                PushServerError::Internal(e.int_err())
            })?;
    }

    tracing::debug!("Sending completion confirmation");

    write_payload::<DatasetPushCompleteConfirmed>(socket, DatasetPushCompleteConfirmed {})
        .await
        .map_err(|e| {
            PushServerError::WriteFailed(PushWriteError::new(e, PushPhase::CompleteRequest))
        })?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    dataset_ref: DatasetRef,
    dataset: Option<Arc<dyn Dataset>>,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_url: Url,
) {
    match dataset_push_ws_main_flow(&mut socket, dataset_ref, dataset, dataset_repo, dataset_url)
        .await
    {
        Ok(_) => {
            tracing::debug!("Push process success");
        }
        Err(e) => {
            tracing::debug!("Push process aborted with error: {}", e);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_main_flow(
    socket: &mut axum::extract::ws::WebSocket,
    dataset_ref: DatasetRef,
    mut dataset: Option<Arc<dyn Dataset>>,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_url: Url,
) -> Result<(), PushServerError> {
    let push_request = handle_push_request_initiation(socket, dataset.clone()).await?;

    let mut new_blocks = try_handle_push_metadata_request(socket, push_request).await?;
    if new_blocks.len() > 0 {
        if dataset.is_none() {
            tracing::info!("Dataset does not exist, trying to create from Seed block");

            let dataset_alias = dataset_ref.alias().expect("Dataset ref is not an alias");

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

            let create_result = dataset_repo
                .create_dataset(dataset_alias, seed_block)
                .await
                .int_err()?;

            dataset = Some(create_result.dataset);
        }

        loop {
            let should_continue = try_handle_push_objects_request(
                socket,
                dataset.as_ref().unwrap().as_ref(),
                &dataset_url,
            )
            .await?;

            if !should_continue {
                break;
            }
        }
    }

    try_handle_push_complete(socket, dataset, new_blocks).await?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////
