// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::ws::{CloseFrame, Message};
use serde::de::DeserializeOwned;
use serde::Serialize;

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

pub(crate) async fn axum_write_close_payload<TMessagePayload: Serialize>(
    socket: &mut axum::extract::ws::WebSocket,
    payload: TMessagePayload,
) -> Result<(), WriteMessageError> {
    let payload_as_json_string = ws_common::payload_to_json::<TMessagePayload>(payload)?;
    let close_frame = CloseFrame {
        // Code will be mapped CloseCode::Error type on client side
        code: 1011,
        reason: payload_as_json_string.into(),
    };

    let message = axum::extract::ws::Message::Close(Some(close_frame));
    let send_result = socket.send(message).await;
    match send_result {
        Ok(_) => Ok(()),
        Err(e) => Err(WriteMessageError::SocketError(Box::new(e))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn wait_for_close(socket: &mut axum::extract::ws::WebSocket) {
    while let Some(msg) = socket.recv().await {
        match msg {
            Ok(Message::Close(_)) | Err(_) => {
                break;
            }
            Ok(_) => {}
        }
    }
}
