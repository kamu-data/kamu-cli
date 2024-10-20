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

use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn root_router() -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(
            super::query_handler::query_handler,
            super::query_handler::query_handler_post
        ))
        .routes(routes!(super::verify_handler::verify_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn dataset_router() -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(super::tail_handler::dataset_tail_handler))
        .routes(routes!(super::metadata_handler::dataset_metadata_handler))
        .routes(routes!(super::ingest_handler::dataset_ingest_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
