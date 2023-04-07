// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(error_generic_member_access)]
#![feature(provide_any)]

mod http_server_dataset_router;
pub use http_server_dataset_router::*;

mod http_server_middleware;
pub use http_server_middleware::*;

mod ws_smart_transfer_protocol_axum_server;

mod ws_smart_transfer_protocol_tungstenite_client;
pub use ws_smart_transfer_protocol_tungstenite_client::*;

mod http_server_simple_transfer_protocol;
pub use http_server_simple_transfer_protocol::*;

mod dataset_protocol_helper;
mod smart_protocol;
mod ws_common;
