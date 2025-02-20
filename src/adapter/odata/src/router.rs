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

use crate::handler::*;

pub fn router_single_tenant() -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(odata_service_handler_st))
        .routes(routes!(odata_metadata_handler_st))
        .routes(routes!(odata_collection_handler_st))
}

pub fn router_multi_tenant() -> OpenApiRouter {
    // TODO: FIXME: This is an ugly hack to support optional trailing slashes
    OpenApiRouter::new()
        .routes(routes!(odata_service_handler_mt))
        .route(
            "/{account_name}/",
            axum::routing::get(odata_service_handler_mt),
        )
        .routes(routes!(odata_metadata_handler_mt))
        .route(
            "/{account_name}/$metadata/",
            axum::routing::get(odata_metadata_handler_mt),
        )
        .routes(routes!(odata_collection_handler_mt))
        .route(
            "/{account_name}/{dataset_name}/",
            axum::routing::get(odata_collection_handler_mt),
        )
}
