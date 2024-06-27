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

use super::ingest_handler::dataset_ingest_handler;
use super::query_handler::{dataset_query_handler, dataset_query_handler_post};
use super::tail_handler::dataset_tail_handler;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn root_router() -> axum::Router {
    axum::Router::new().route(
        "/query",
        axum::routing::get(dataset_query_handler).post(dataset_query_handler_post),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn dataset_router() -> axum::Router {
    axum::Router::new()
        .route("/tail", axum::routing::get(dataset_tail_handler))
        .route("/ingest", axum::routing::post(dataset_ingest_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
