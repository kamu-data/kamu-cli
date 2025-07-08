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

use crate::AuthPolicyLayer;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn root_router(allow_anonymous: bool) -> OpenApiRouter {
    use crate::platform::handlers;

    let mut router = OpenApiRouter::new();

    router = router
        .routes(routes!(handlers::file_upload_prepare_post_handler))
        .routes(routes!(handlers::file_upload_post_handler))
        .routes(routes!(handlers::file_upload_get_handler));

    if !allow_anonymous {
        router = router.layer(AuthPolicyLayer::new()); // Mutate router without moving
    }

    router = router
        .routes(routes!(handlers::login_handler))
        .routes(routes!(handlers::token_validate_handler))
        .routes(routes!(handlers::token_device_authorization_handler))
        .routes(routes!(handlers::token_device_handler));

    router
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
