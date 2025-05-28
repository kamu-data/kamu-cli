// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::BoxedError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ReadMessageError {
    #[error("Client has disconnected unexpectedly")]
    ClientDisconnected,

    #[error("Close message received")]
    Closed,

    #[error("Message is not in text format")]
    NonTextMessageReceived,

    #[error("Incompatible message version. Please upgrade client version")]
    IncompatibleVersion,

    #[error(transparent)]
    SerdeError(
        #[from]
        #[backtrace]
        serde_json::Error,
    ),

    #[error(transparent)]
    SocketError(
        #[from]
        #[backtrace]
        BoxedError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum WriteMessageError {
    #[error(transparent)]
    SerdeError(
        #[from]
        #[backtrace]
        serde_json::Error,
    ),

    #[error(transparent)]
    SocketError(
        #[from]
        #[backtrace]
        BoxedError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn parse_payload<TMessagePayload: DeserializeOwned>(
    raw_message: &str,
) -> Result<TMessagePayload, ReadMessageError> {
    let parse_result = serde_json::from_str::<TMessagePayload>(raw_message);

    match parse_result {
        Ok(payload) => Ok(payload),
        Err(e) => Err(ReadMessageError::SerdeError(e)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn payload_to_json<TMessagePayload: Serialize>(
    payload: TMessagePayload,
) -> Result<String, WriteMessageError> {
    let maybe_payload_as_json_string = serde_json::to_string(&payload);

    match maybe_payload_as_json_string {
        Ok(payload_as_json_string) => Ok(payload_as_json_string),
        Err(e) => Err(WriteMessageError::SerdeError(e)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
