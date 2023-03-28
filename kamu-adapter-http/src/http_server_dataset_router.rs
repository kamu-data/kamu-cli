// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::http_server_simple_transfer_protocol::*;

/////////////////////////////////////////////////////////////////////////////////

pub fn smart_transfer_protocol_routes() -> axum::Router {
    axum::Router::new()
        .route("/refs/:reference", axum::routing::get(dataset_refs_handler))
        .route(
            "/blocks/:block_hash",
            axum::routing::get(dataset_blocks_handler),
        )
        .route(
            "/data/:physical_hash",
            axum::routing::get(dataset_data_handler),
        )
        .route(
            "/checkpoints/:physical_hash",
            axum::routing::get(dataset_checkpoints_handler),
        )
        .route("/pull", axum::routing::get(dataset_pull_ws_upgrade_handler))
        .route("/push", axum::routing::get(dataset_push_ws_upgrade_handler))
}

/////////////////////////////////////////////////////////////////////////////////
