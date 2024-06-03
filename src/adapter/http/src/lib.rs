// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]
#![feature(let_chains)]

mod http_server_dataset_router;
pub use http_server_dataset_router::*;
mod middleware;
pub use middleware::*;
mod access_token;
pub use access_token::*;
pub mod api_error;
mod axum_utils;
pub mod data;
mod simple_protocol;
pub mod smart_protocol;
mod upload;
mod ws_common;
pub use upload::*;

pub type SmartTransferProtocolClientWs =
    smart_protocol::ws_tungstenite_client::WsSmartTransferProtocolClient;
