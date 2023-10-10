// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn bad_request_response() -> axum::response::Response {
    error_response(http::status::StatusCode::BAD_REQUEST)
}

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn not_found_response() -> axum::response::Response {
    error_response(http::status::StatusCode::NOT_FOUND)
}

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn unauthorized_access_response() -> axum::response::Response {
    error_response(http::status::StatusCode::UNAUTHORIZED)
}

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn forbidden_access_response() -> axum::response::Response {
    error_response(http::status::StatusCode::FORBIDDEN)
}

/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn internal_server_error_response() -> axum::response::Response {
    error_response(http::status::StatusCode::INTERNAL_SERVER_ERROR)
}

/////////////////////////////////////////////////////////////////////////////////

fn error_response(status: http::status::StatusCode) -> axum::response::Response {
    axum::response::Response::builder()
        .status(status)
        .body(Default::default())
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////
