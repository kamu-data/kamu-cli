// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::http::{HeaderMap, HeaderValue, StatusCode, Uri, Version};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::unused_async)]
#[tracing::instrument(
    level = "warn",
    name = "HTTP: fallback request",
    skip_all,
    fields(%uri, ?version, ?headers)
)]
pub async fn unknown_handler(
    uri: Uri,
    version: Version,
    headers: HeaderMap<HeaderValue>,
) -> impl axum::response::IntoResponse {
    (StatusCode::NOT_FOUND, "Not Found")
}
