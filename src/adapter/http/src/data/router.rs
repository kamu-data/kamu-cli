// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn root_router() -> axum::Router {
    axum::Router::new()
        .route(
            "/query",
            axum::routing::get(super::query_handler::query_handler)
                .post(super::query_handler::query_handler_post),
        )
        .route(
            "/verify",
            axum::routing::post(super::verify_handler::verify_handler),
        )
        .route(
            "/workspace/info",
            axum::routing::get(super::workspace_info_handler::workspace_info_handler),
        )
        .route(
            "/me",
            axum::routing::get(super::account_handler::account_handler),
        )
        .route(
            "/datasets/:id",
            axum::routing::get(super::dataset_info_handler::dataset_info_handler),
        )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn dataset_router() -> axum::Router {
    axum::Router::new()
        .route(
            "/tail",
            axum::routing::get(super::tail_handler::dataset_tail_handler),
        )
        .route(
            "/metadata",
            axum::routing::get(super::metadata_handler::dataset_metadata_handler),
        )
        .route(
            "/ingest",
            axum::routing::post(super::ingest_handler::dataset_ingest_handler),
        )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
