// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu::domain::LocalDatasetRepository;

use crate::http_server_constants::*;
use crate::http_server_simple_transfer_protocol::*;


pub fn create_dataset_routes(local_repo: Arc<dyn LocalDatasetRepository>) -> axum::Router {
    axum::Router::new()
        .route(
            format!("/refs/:{}", PARAMETER_REF).as_str(), 
            axum::routing::get(dataset_refs_handler)
        )
        .route(
            format!("/blocks/:{}", PARAMETER_BLOCK_HASH).as_str(), 
            axum::routing::get(dataset_blocks_handler)
        )
        .route(
            format!("/data/:{}", PARAMETER_PHYSICAL_HASH).as_str(), 
            axum::routing::get(dataset_data_handler)
        )
        .route(
            format!("/checkpoints/:{}", PARAMETER_PHYSICAL_HASH).as_str(), 
            axum::routing::get(dataset_checkpoints_handler)
        )
        .route(
            "/pull", 
            axum::routing::get(dataset_pull_ws_upgrade_handler)
        )
        .route(
            "/push", 
            axum::routing::get(dataset_push_ws_upgrade_handler)
        )
        .layer(
            axum::extract::Extension(local_repo)
        )
}

