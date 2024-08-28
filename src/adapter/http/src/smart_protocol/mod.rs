// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub mod protocol_dataset_helper;

pub mod axum_server_protocol_common;
mod axum_server_pull_protocol;
mod axum_server_push_protocol;

pub(crate) use axum_server_protocol_common::*;
pub(crate) use axum_server_pull_protocol::*;
pub(crate) use axum_server_push_protocol::*;

mod errors;
pub mod messages;
mod phases;

pub mod ws_tungstenite_client;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type BearerHeader =
    axum::TypedHeader<axum::headers::Authorization<axum::headers::authorization::Bearer>>;
pub type VersionHeaderTyped = axum::TypedHeader<VersionHeader>;

#[derive(Debug)]
pub struct VersionHeader(pub i32);

impl axum::headers::Header for VersionHeader {
    fn name() -> &'static axum::headers::HeaderName {
        static NAME: axum::headers::HeaderName =
            axum::headers::HeaderName::from_static("x-websocket-version");
        &NAME
    }

    fn decode<'i, I>(values: &mut I) -> Result<Self, axum::headers::Error>
    where
        I: Iterator<Item = &'i axum::headers::HeaderValue>,
    {
        if let Some(value) = values.next() {
            let value_str = value
                .to_str()
                .map_err(|_| axum::headers::Error::invalid())?;
            let value_int = value_str
                .parse::<i32>()
                .map_err(|_| axum::headers::Error::invalid())?;
            Ok(VersionHeader(value_int))
        } else {
            Err(axum::headers::Error::invalid())
        }
    }

    fn encode<E: Extend<axum::headers::HeaderValue>>(&self, values: &mut E) {
        let value_str = self.0.to_string();
        let value = axum::headers::HeaderValue::from_str(&value_str).expect("Invalid header value");
        values.extend(std::iter::once(value));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
