// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use tokio::sync::Notify;

/////////////////////////////////////////////////////////////////////////////////

pub fn e2e_router(shutdown_notify: Arc<Notify>) -> Router {
    Router::new()
        .route("/health", get(|| async { "OK" }))
        .route(
            "/shutdown",
            post(|| async move {
                shutdown_notify.notify_one();
                "Shutting down HTTP server..."
            }),
        )
}

/////////////////////////////////////////////////////////////////////////////////
