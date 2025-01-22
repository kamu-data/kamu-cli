// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use dill::Catalog;
use http_common::{ApiError, IntoApiError, ResultIntoApiError};
use internal_error::ResultIntoInternal;
use kamu_accounts::{AnonymousAccountReason, CurrentAccountSubject};
use kamu_core::auth;
use kamu_core::auth::DatasetActionAuthorizerExt;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn bad_request_response() -> axum::response::Response {
    error_response(http::status::StatusCode::BAD_REQUEST)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn not_found_response() -> axum::response::Response {
    error_response(http::status::StatusCode::NOT_FOUND)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(dead_code)]
pub(crate) fn unauthorized_access_response() -> axum::response::Response {
    error_response(http::status::StatusCode::UNAUTHORIZED)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(dead_code)]
pub(crate) fn forbidden_access_response() -> axum::response::Response {
    error_response(http::status::StatusCode::FORBIDDEN)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn internal_server_error_response() -> axum::response::Response {
    error_response(http::status::StatusCode::INTERNAL_SERVER_ERROR)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn error_response(status: http::status::StatusCode) -> axum::response::Response {
    axum::response::Response::builder()
        .status(status)
        .body(Default::default())
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Misc
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn body_into_async_read(body: axum::body::Body) -> impl tokio::io::AsyncRead {
    use futures::TryStreamExt;
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    body.into_data_stream()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read()
        .compat()
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

pub(crate) async fn check_dataset_read_access(
    catalog: &Catalog,
    dataset_handle: &odf::DatasetHandle,
) -> Result<(), ApiError> {
    check_dataset_access(catalog, dataset_handle, auth::DatasetAction::Read).await
}

#[expect(dead_code)]
pub(crate) async fn check_dataset_write_access(
    catalog: &Catalog,
    dataset_handle: &odf::DatasetHandle,
) -> Result<(), ApiError> {
    check_dataset_access(catalog, dataset_handle, auth::DatasetAction::Write).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn check_dataset_access(
    catalog: &Catalog,
    dataset_handle: &odf::DatasetHandle,
    action: auth::DatasetAction,
) -> Result<(), ApiError> {
    let dataset_action_authorizer = catalog
        .get_one::<dyn auth::DatasetActionAuthorizer>()
        .int_err()
        .api_err()?;

    let accessible = dataset_action_authorizer
        .is_action_allowed(&dataset_handle.id, action)
        .await?;

    if accessible {
        Ok(())
    } else {
        Err(ApiError::not_found_without_reason())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
