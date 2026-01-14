// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::Catalog;
use http_common::{ApiError, IntoApiError};
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};
use kamu_core::auth;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Macros
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! from_catalog_n {
    ($catalog:ident, $T:ty) => {{
        $catalog.get_one::<$T>().unwrap()
    }};
    ($catalog:ident, $T:ty, $($Ts:ty),+) => {{
        ( $catalog.get_one::<$T>().unwrap(), $( $catalog.get_one::<$Ts>().unwrap() ),+ )
    }};
}

pub(crate) use from_catalog_n;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn bad_request_response<B>(body_maybe: Option<B>) -> axum::response::Response
where
    B: Into<axum::body::Body>,
{
    error_response(http::status::StatusCode::BAD_REQUEST, body_maybe)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn not_found_response() -> axum::response::Response {
    error_response::<axum::body::Body>(http::status::StatusCode::NOT_FOUND, None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn unauthorized_access_response<B>(body_maybe: Option<B>) -> axum::response::Response
where
    B: Into<axum::body::Body>,
{
    error_response(http::status::StatusCode::UNAUTHORIZED, body_maybe)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn forbidden_access_response() -> axum::response::Response {
    error_response::<axum::body::Body>(http::status::StatusCode::FORBIDDEN, None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn internal_server_error_response() -> axum::response::Response {
    error_response::<axum::body::Body>(http::status::StatusCode::INTERNAL_SERVER_ERROR, None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn error_response<B>(
    status: http::status::StatusCode,
    body_maybe: Option<B>,
) -> axum::response::Response
where
    B: Into<axum::body::Body>,
{
    axum::response::Response::builder()
        .status(status)
        .body(body_maybe.map(Into::into).unwrap_or_default())
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Misc
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn body_into_async_read(body: axum::body::Body) -> impl tokio::io::AsyncRead {
    use futures::TryStreamExt;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    body.into_data_stream()
        .map_err(std::io::Error::other)
        .into_async_read()
        .compat()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn get_dataset_action_for_request(
    request: &http::Request<axum::body::Body>,
) -> auth::DatasetAction {
    if !request.method().is_safe() || request.uri().path() == "/push" {
        auth::DatasetAction::Write
    } else {
        auth::DatasetAction::Read
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("{reason}")]
pub struct AnonymousAccessError {
    pub reason: &'static str,
}

impl IntoApiError for AnonymousAccessError {
    fn api_err(self) -> ApiError {
        ApiError::new_unauthorized_from(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_authenticated_account(
    catalog: &Catalog,
) -> Result<odf::AccountID, AnonymousAccessError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();

    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(l) => Ok(l.account_id.clone()),
        CurrentAccountSubject::Anonymous(reason) => Err(match reason {
            AnonymousAccountReason::AuthenticationExpired => AnonymousAccessError {
                reason: "Authentication token expired",
            },
            AnonymousAccountReason::AuthenticationInvalid => AnonymousAccessError {
                reason: "Authentication token invalid",
            },
            AnonymousAccountReason::NoAuthenticationProvided => AnonymousAccessError {
                reason: "No authentication token provided",
            },
        }),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
