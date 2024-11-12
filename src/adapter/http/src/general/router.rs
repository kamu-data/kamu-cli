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
        .routes(routes!(super::node_info_handler::node_info_handler))
        .routes(routes!(super::account_handler::account_handler))
        .routes(routes!(super::dataset_info_handler::dataset_info_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
