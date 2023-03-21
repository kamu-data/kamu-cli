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
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;
use url::Url;

use crate::{
    dataset_protocol_helper::*,
    messages::*,
    ws_common::{self, ReadMessageError, WriteMessageError},
};
use kamu::domain::{BlockRef, Dataset, InternalError};

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

async fn handle_pull_request_initiation(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
) -> Result<DatasetPullRequest, SmartProtocolPullServerError> {
    let maybe_pull_request = read_payload::<DatasetPullRequest>(socket).await;

    match maybe_pull_request {
        Ok(pull_request) => {
            // TODO: professional logging
            println!(
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
            let head = metadata_chain.get_ref(&BlockRef::Head).await.unwrap();

            let size_estimation_result = prepare_dataset_transfer_estimaton(
                dataset.as_metadata_chain(),
                pull_request
                    .stop_at
                    .as_ref()
                    .or(Some(&head))
                    .unwrap()
                    .clone(),
                pull_request.begin_after.clone(),
            )
            .await;
            if let Err(e) = size_estimation_result {
                return Err(SmartProtocolPullServerError::Internal(e));
            }

            let response_result = write_payload::<DatasetPullResponse>(
                socket,
                DatasetPullResponse {
                    size_estimation: size_estimation_result.unwrap(),
                },
            )
            .await;
            if let Err(e) = response_result {
                Err(SmartProtocolPullServerError::PullResponseWriteFailed(e))
            } else {
                Ok(pull_request)
            }
        }
        Err(e) => Err(SmartProtocolPullServerError::PullRequestReadFailed(e)),
    }
}

/////////////////////////////////////////////////////////////////////////////////

async fn handle_pull_metadata_request(
    socket: &mut axum::extract::ws::WebSocket,
    dataset: &dyn Dataset,
    pull_request: DatasetPullRequest,
) -> Result<(), SmartProtocolPullServerError> {
    let maybe_pull_metadata_request = read_payload::<DatasetPullMetadataRequest>(socket).await;

    match maybe_pull_metadata_request {
        Ok(_) => {
            // TODO: professional logging
            println!("Pull client sent a pull metadata request");

            let metadata_chain = dataset.as_metadata_chain();
            let head = metadata_chain.get_ref(&BlockRef::Head).await.unwrap();

            let metadata_batch = prepare_dataset_metadata_batch(
                dataset.as_metadata_chain(),
                pull_request.stop_at.or(Some(head)).unwrap(),
                pull_request.begin_after,
            )
            .await;

            let response_result_metadata = write_payload::<DatasetMetadataPullResponse>(
                socket,
                DatasetMetadataPullResponse {
                    blocks: metadata_batch,
                },
            )
            .await;

            if let Err(e) = response_result_metadata {
                Err(SmartProtocolPullServerError::PullMetadataResponseWriteFailed(e))
            } else {
                Ok(())
            }
        }
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
            // TODO: professional logging
            println!(
                "Pull client sent a pull objects request for {} objects",
                request.object_files.len(),
            );
            let mut object_transfer_strategies: Vec<PullObjectTransferStrategy> = Vec::new();
            for r in request.object_files {
                let transfer_strategy =
                    prepare_object_transfer_strategy(dataset, &r, dataset_url).await;

                match transfer_strategy {
                    Ok(strategy) => object_transfer_strategies.push(strategy),
                    Err(e) => match e {
                        PrepareObjectTransferStrategyError::Internal(e) => {
                            return Err(SmartProtocolPullServerError::Internal(e))
                        }
                    },
                }
            }

            let response_result_objects = write_payload::<DatasetPullObjectsTransferResponse>(
                socket,
                DatasetPullObjectsTransferResponse {
                    object_transfer_strategies,
                },
            )
            .await;

            if let Err(e) = response_result_objects {
                Err(SmartProtocolPullServerError::PullObjectResponseWriteFailed(
                    e,
                ))
            } else {
                Ok(true)
            }
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
    // TODO: error handling
    let pull_request = handle_pull_request_initiation(&mut socket, dataset.as_ref())
        .await
        .unwrap();

    // TODO: the flow might abort at this point, handle optionality
    handle_pull_metadata_request(&mut socket, dataset.as_ref(), pull_request)
        .await
        .unwrap();

    loop {
        let should_continue =
            try_handle_pull_objects_request(&mut socket, dataset.as_ref(), &dataset_url)
                .await
                .unwrap();
        if !should_continue {
            break;
        }
    }

    // Success
    println!("Pull process succeded");
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_handler(
    mut socket: axum::extract::ws::WebSocket,
    _dataset: Arc<dyn Dataset>,
) {
    while let Some(msg) = socket.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            // client disconnected
            return;
        };
        println!("Push client sent: {}", msg.to_text().unwrap());

        let reply = axum::extract::ws::Message::Text(String::from("Hi push client!"));
        if socket.send(reply).await.is_err() {
            // client disconnected
            return;
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////
