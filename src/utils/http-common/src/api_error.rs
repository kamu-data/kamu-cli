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

use internal_error::InternalError;
use kamu_core::*;
use serde::{Deserialize, Serialize};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This type is used to simplify error handling in HTTP handler and unify
/// logging of API errors.
///
/// The typical usage pattern is:
///
/// ```
/// async fn handler() -> Result<(), ApiError> {
///     operation().await.api_err()?;
///     Ok(())
/// }
/// ````
///
/// A conversion between the domain error and [`ApiError`] has to exist. We on
/// purpose avoid [From] and [Into] traits and using [`IntoApiError`] instead as
/// we want this conversion to be explicit - it's too easy to put a question
/// mark operator on a fallible operation without thinking what it will actually
/// do.
///
/// Note that in between handlers different errors have different meaning, e.g.
/// an absence of a dataset in one handler should lead to `404 Not Found`, while
/// in the other it can be `400 Bad Request` at first and then treated as `500
/// Internal Server Error` later. This is why it's not advisable to provide
/// direct conversion via [`IntoApiError`], but rather by implementing the
/// [`ApiErrorCategorizable`] trait instead. This trait only deals with
/// high-level categories of errors like [`AccessError`] while mapping all other
/// errors to `500 Internal Server Error` response.
///
/// When you need to return different status codes you can still take advantage
/// of uniform error handling using this pattern:
///
/// ```
/// async fn handler() -> Result<(), ApiError> {
///     match operation().await {
///         Ok(_) => Ok(()),
///         Err(OperationError::NotFound(e)) => Err(ApiError::not_found(e)),
///         Err(e) => Err(e.api_err())
///     }
/// }
/// ```
#[derive(Debug, Error)]
#[error("api error {status_code:?}")]
pub struct ApiError {
    pub status_code: http::StatusCode,
    source: Box<dyn std::error::Error + Send + Sync + 'static>,
}

impl ApiError {
    pub fn new(
        source: impl std::error::Error + Send + Sync + 'static,
        status_code: http::StatusCode,
    ) -> Self {
        Self {
            status_code,
            source: source.into(),
        }
    }

    pub fn new_unauthorized() -> Self {
        odf::AccessError::Unauthorized("Unauthorized access".into()).api_err()
    }

    pub fn new_unauthorized_from(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new(source, http::StatusCode::UNAUTHORIZED)
    }

    pub fn new_forbidden() -> Self {
        odf::AccessError::Forbidden("Forbidden access".into()).api_err()
    }

    pub fn bad_request(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new(source, http::StatusCode::BAD_REQUEST)
    }

    pub fn not_found(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new(source, http::StatusCode::NOT_FOUND)
    }

    pub fn not_implemented(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new(source, http::StatusCode::NOT_IMPLEMENTED)
    }

    pub fn not_found_without_reason() -> Self {
        Self {
            source: "Not Found".into(),
            status_code: http::StatusCode::NOT_FOUND,
        }
    }

    pub fn new_unsupported_media_type() -> Self {
        Self {
            source: "Unsupported media type".into(),
            status_code: http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        }
    }

    pub fn no_content(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::new(source, http::StatusCode::NO_CONTENT)
    }

    pub fn incompatible_client_version() -> Self {
        Self {
            source: "Incompatible client version".into(),
            status_code: http::StatusCode::BAD_REQUEST,
        }
    }
}

impl From<InternalError> for ApiError {
    fn from(e: InternalError) -> Self {
        e.api_err()
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct ApiErrorResponse {
    pub message: String,
}

impl axum::response::IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        // TODO: Logging as a side effect of conversion is not great - we should move
        // this into a middleware
        if self.status_code == http::StatusCode::INTERNAL_SERVER_ERROR {
            tracing::error!(
                error = ?self.source,
                error_msg = %self.source,
                status_code = %self.status_code,
                "Internal API error",
            );
            (self.status_code, "").into_response()
        } else {
            tracing::warn!(
                error = ?self.source,
                error_msg = %self.source,
                status_code = %self.status_code,
                "API error",
            );

            let response_body = axum::response::Json(ApiErrorResponse {
                message: self.source.to_string(),
            });

            (self.status_code, response_body).into_response()
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Provides explicit conversion into [`ApiError`].
///
/// See also [`ApiErrorCategorizable`].
pub trait IntoApiError {
    fn api_err(self) -> ApiError;
}

/// Allows using `.api_err()` method on [Result] types.
pub trait ResultIntoApiError<K, E>
where
    E: IntoApiError,
{
    fn api_err(self) -> Result<K, ApiError>;
}
impl<K, E> ResultIntoApiError<K, E> for Result<K, E>
where
    E: IntoApiError,
{
    fn api_err(self) -> Result<K, ApiError> {
        match self {
            Ok(v) => Ok(v),
            Err(e) => Err(e.api_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum ApiErrorCategory<'a> {
    Access(&'a odf::AccessError),
    Internal(&'a InternalError),
    Other,
}

/// Categorizes an error into a certain group. As explained in [`ApiError`] docs
/// these categories are very general in order to be applicable regardless of
/// context.
trait ApiErrorCategorizable {
    fn categorize(&self) -> ApiErrorCategory<'_>;
}

impl<E> IntoApiError for E
where
    E: ApiErrorCategorizable,
    E: std::error::Error + Send + Sync + 'static,
{
    fn api_err(self) -> ApiError {
        match self.categorize() {
            ApiErrorCategory::Access(odf::AccessError::Unauthorized(_)) => {
                ApiError::new(self, http::StatusCode::UNAUTHORIZED)
            }
            ApiErrorCategory::Access(
                odf::AccessError::Forbidden(_) | odf::AccessError::ReadOnly(_),
            ) => ApiError::new(self, http::StatusCode::FORBIDDEN),
            ApiErrorCategory::Internal(_e) => {
                ApiError::new(self, http::StatusCode::INTERNAL_SERVER_ERROR)
            }
            ApiErrorCategory::Other => ApiError::new(self, http::StatusCode::INTERNAL_SERVER_ERROR),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TODO: These categories seem like the same kind of problem we have when
// propagating different classes of errors. We should explore how to simplify
// this in future.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ApiErrorCategorizable for InternalError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        ApiErrorCategory::Internal(self)
    }
}

impl ApiErrorCategorizable for odf::AccessError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        ApiErrorCategory::Access(self)
    }
}

impl ApiErrorCategorizable for PushIngestError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Access(e) => ApiErrorCategory::Access(e),
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}

impl ApiErrorCategorizable for odf::dataset::GetDatasetError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}

impl ApiErrorCategorizable for odf::storage::GetRefError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Access(e) => ApiErrorCategory::Access(e),
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}

impl ApiErrorCategorizable for odf::storage::GetBlockError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Access(e) => ApiErrorCategory::Access(e),
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}

impl ApiErrorCategorizable for odf::storage::GetError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Access(e) => ApiErrorCategory::Access(e),
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}

impl ApiErrorCategorizable for odf::storage::InsertError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Access(e) => ApiErrorCategory::Access(e),
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}

impl ApiErrorCategorizable for QueryError {
    fn categorize(&self) -> ApiErrorCategory<'_> {
        match &self {
            Self::Access(e) => ApiErrorCategory::Access(e),
            Self::Internal(e) => ApiErrorCategory::Internal(e),
            _ => ApiErrorCategory::Other,
        }
    }
}
