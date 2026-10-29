// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{CloseFrame, Message};
use database_common::DatabaseTransactionRunner;
use dill::Catalog;
use internal_error::InternalError;
use kamu_datasets::DatasetRegistry;
use serde::Serialize;
use serde::de::DeserializeOwned;
use url::Url;

use super::error_close::ErrorReplyClose;
use super::messages::ObjectFileReference;
use super::protocol_dataset_helper::ObjectTransferStrategy;
use crate::BearerHeader;
use crate::ws_common::{self, ReadMessageError, WriteMessageError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn axum_read_payload<TMessagePayload: DeserializeOwned>(
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn axum_write_payload<TMessagePayload: Serialize>(
    socket: &mut axum::extract::ws::WebSocket,
    payload: TMessagePayload,
) -> Result<(), WriteMessageError> {
    let payload_as_json_string = ws_common::payload_to_json::<TMessagePayload>(payload)?;

    let message = axum::extract::ws::Message::Text(payload_as_json_string.into());
    let send_result = socket.send(message).await;
    match send_result {
        Ok(_) => Ok(()),
        Err(e) => Err(WriteMessageError::SocketError(Box::new(e))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Ends the session with an error reply in the close frame
pub(crate) async fn axum_write_error_close(
    socket: &mut axum::extract::ws::WebSocket,
    close: Result<ErrorReplyClose, WriteMessageError>,
) {
    let ErrorReplyClose { code, reason } = match close {
        Ok(close) => close,
        Err(err) => {
            tracing::error!(
              error = ?err,
              error_msg = %err,
              "Failed to serialize the error reply",
            );
            return;
        }
    };

    let message = Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }));
    if let Err(err) = socket.send(message).await {
        tracing::error!(
          error = ?err,
          error_msg = %err,
          "Failed to send error to client with error",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Sends an error reply as a text message, for a reply that may not fit the
/// 123 bytes a close frame reason allows. The client reads it where it reads
/// the phase's response
pub(crate) async fn axum_write_error_message<TMessagePayload: Serialize>(
    socket: &mut axum::extract::ws::WebSocket,
    payload: TMessagePayload,
) {
    if let Err(err) = axum_write_payload(socket, payload).await {
        tracing::error!(
          error = ?err,
          error_msg = %err,
          "Failed to send error to client with error",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Waits for the client to close the session after the server's last reply.
/// A client that never closes is given a few seconds
pub(crate) async fn axum_await_client_close(socket: &mut axum::extract::ws::WebSocket) {
    const CLIENT_CLOSE_TIMEOUT: Duration = Duration::from_secs(5);

    if tokio::time::timeout(CLIENT_CLOSE_TIMEOUT, wait_for_close(socket))
        .await
        .is_err()
    {
        tracing::debug!("Timeout reached, closing connection");
    }
}

async fn wait_for_close(socket: &mut axum::extract::ws::WebSocket) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Close(_)) | Err(_) => {
                break;
            }
            Ok(_) => {}
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// One transfer strategy per object a client asks for, read from the dataset
/// in a transaction of its own
pub(crate) async fn prepare_object_transfer_strategies<
    TStrategy: ObjectTransferStrategy + 'static,
>(
    catalog: &Catalog,
    dataset_handle: odf::DatasetHandle,
    object_files: Vec<ObjectFileReference>,
    dataset_url: Url,
    maybe_bearer_header: Option<BearerHeader>,
) -> Result<Vec<TStrategy>, InternalError> {
    DatabaseTransactionRunner::new(catalog.clone())
        .transactional_with(|dataset_registry: Arc<dyn DatasetRegistry>| async move {
            let resolved_dataset = dataset_registry
                .get_dataset_by_handle(&dataset_handle)
                .await;

            let mut object_transfer_strategies = Vec::with_capacity(object_files.len());
            for object_file_ref in &object_files {
                object_transfer_strategies.push(
                    TStrategy::prepare(
                        resolved_dataset.as_ref(),
                        object_file_ref,
                        &dataset_url,
                        maybe_bearer_header.as_ref(),
                    )
                    .await?,
                );
            }

            Ok(object_transfer_strategies)
        })
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
