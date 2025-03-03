// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::http::{Uri, Version};
use http::{HeaderMap, HeaderValue, StatusCode};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn unknown_handler(
    uri: Uri,
    version: Version,
    headers: HeaderMap<HeaderValue>,
) -> impl axum::response::IntoResponse {
    tracing::info!(
        uri = %uri,
        version = ?version,
        headers = ?headers,
        "Unknown HTTP request",
    );
    (StatusCode::NOT_FOUND, "Not Found")
}
