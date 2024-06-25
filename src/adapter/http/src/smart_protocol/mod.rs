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

////////////////////////////////////////////////////////////////////////////////

pub type BearerHeader =
    axum::TypedHeader<axum::headers::Authorization<axum::headers::authorization::Bearer>>;

////////////////////////////////////////////////////////////////////////////////
