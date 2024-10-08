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
            "/info",
            axum::routing::get(super::node_info_handler::node_info_handler),
        )
        .route(
            "/accounts/me",
            axum::routing::get(super::account_handler::account_handler),
        )
        .route(
            "/datasets/:id",
            axum::routing::get(super::dataset_info_handler::dataset_info_handler),
        )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
