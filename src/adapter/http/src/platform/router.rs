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
    use crate::platform::handlers;

    OpenApiRouter::new()
        .routes(routes!(handlers::login_handler))
        .routes(routes!(handlers::token_validate_handler))
        .routes(routes!(handlers::token_device_authorization_handler))
        .routes(routes!(handlers::token_device_handler))
        .routes(routes!(handlers::file_upload_prepare_post_handler))
        .routes(routes!(handlers::file_upload_post_handler))
        .routes(routes!(handlers::file_upload_get_handler))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
