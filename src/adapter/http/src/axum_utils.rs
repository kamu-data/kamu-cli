// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////

use kamu_accounts::AnonymousAccountReason;
use thiserror::Error;

use crate::api_error::ApiError;

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
// Misc
/////////////////////////////////////////////////////////////////////////////////

pub(crate) fn body_into_async_read(
    body_stream: axum::extract::BodyStream,
) -> impl tokio::io::AsyncRead {
    use futures::TryStreamExt;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    body_stream
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read()
        .compat()
}

/////////////////////////////////////////////////////////////////////////////////

pub fn response_for_anonymous_denial(reason: AnonymousAccountReason) -> ApiError {
    #[derive(Debug, Error)]
    #[error("{reason}")]
    struct AnonymousAccessError {
        pub reason: &'static str,
    }

    match reason {
        AnonymousAccountReason::AuthenticationExpired => {
            ApiError::new_unauthorized_custom(AnonymousAccessError {
                reason: "Authentication token expired",
            })
        }
        AnonymousAccountReason::AuthenticationInvalid => {
            ApiError::new_unauthorized_custom(AnonymousAccessError {
                reason: "Authentication token invalid",
            })
        }
        AnonymousAccountReason::NoAuthenticationProvided => {
            ApiError::new_unauthorized_custom(AnonymousAccessError {
                reason: "No authentication token provided",
            })
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////
