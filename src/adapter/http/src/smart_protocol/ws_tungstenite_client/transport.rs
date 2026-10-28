// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::SinkExt;
use headers::Header;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use url::Url;

use super::WsSmartTransferProtocolClient;
use crate::OdfSmtpVersion;
use crate::smart_protocol::messages::*;
use crate::ws_common::{self, ReadMessageError, WriteMessageError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl WsSmartTransferProtocolClient {
    /// Opens the protocol's web socket at `flow` ("pull" or "push") of the
    /// dataset endpoint
    pub(super) async fn connect(
        http_dataset_url: &Url,
        flow: &str,
        maybe_access_token: Option<&str>,
    ) -> Result<TungsteniteStream, SyncError> {
        let ws_url = Self::generate_ws_url(http_dataset_url, flow);

        tracing::debug!(%ws_url, flow, "Connecting smart protocol web socket");

        let mut request = ws_url.into_client_request().int_err()?;
        request.headers_mut().append(
            OdfSmtpVersion::name(),
            http::HeaderValue::from(SMART_TRANSFER_PROTOCOL_VERSION),
        );
        if let Some(access_token) = maybe_access_token {
            request.headers_mut().append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(&format!("Bearer {access_token}")).int_err()?,
            );
        }

        match connect_async(request).await {
            Ok((ws_stream, _)) => Ok(ws_stream),
            Err(e) => {
                tracing::debug!(error = %e, flow, "Failed to connect the web socket");
                Err(map_tungstenite_error(e, http_dataset_url))
            }
        }
    }

    fn generate_ws_url(http_base_url: &Url, additional_path_segment: &str) -> Url {
        let mut url = http_base_url.join(additional_path_segment).unwrap();
        let new_scheme = if url.scheme() == "https" { "wss" } else { "ws" };

        assert!(url.set_scheme(new_scheme).is_ok());

        url
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) type TungsteniteStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) async fn read_payload<TMessagePayload: DeserializeOwned>(
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

pub(super) async fn write_payload<TMessagePayload: Serialize>(
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
                return SyncError::Access(odf::AccessError::Unauthenticated(Box::new(error)));
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
