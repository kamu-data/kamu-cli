// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use axum::response::Json;

use super::query_types as query;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerifyRequest {
    /// Inputs that will be used to reproduce the query
    pub input: query::QueryRequest,

    /// Information about processing performed by other nodes as part of the
    /// original operation
    #[schema(example = json!([]))]
    pub sub_queries: Vec<query::SubQuery>,

    /// Commitment created by the original operation
    pub commitment: query::Commitment,

    /// Signature block
    pub proof: query::Proof,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifyResponse {
    /// Whether validation was successful
    pub ok: bool,

    /// Will contain error details if validation was unsuccessful
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = ValidationError)]
    pub error: Option<ValidationError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum ValidationError {
    #[error(transparent)]
    InvalidRequest(InvalidRequest),
    #[error(transparent)]
    VerificationFailed(VerificationFailed),
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(tag = "kind")]
pub enum InvalidRequest {
    #[serde(rename = "InvalidRequest::InputHash")]
    #[error(transparent)]
    InputHash(#[from] InvalidRequestInputHash),

    #[serde(rename = "InvalidRequest::SubQueriesHash")]
    #[error(transparent)]
    SubQueriesHash(#[from] InvalidRequestSubQueriesHash),

    #[serde(rename = "InvalidRequest::BadSignature")]
    #[error(transparent)]
    BadSignature(#[from] InvalidRequestBadSignature),
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct InvalidRequestInputHash {
    pub message: Cow<'static, str>,
}

impl InvalidRequestInputHash {
    pub fn new() -> Self {
        Self {
            message: Cow::from(
                "The commitment is invalid and cannot be disputed: commitment.inputHash doesn't \
                 match the hash of input object",
            ),
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct InvalidRequestSubQueriesHash {
    pub message: Cow<'static, str>,
}

impl InvalidRequestSubQueriesHash {
    pub fn new() -> Self {
        Self {
            message: Cow::from(
                "The commitment is invalid and cannot be disputed: commitment.subQueriesHash \
                 doesn't match the hash of subQueries object",
            ),
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct InvalidRequestBadSignature {
    pub message: String,
}

impl InvalidRequestBadSignature {
    pub fn new(message: &str) -> Self {
        Self {
            message: format!("The commitment is invalid and cannot be disputed: {message}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[serde(tag = "kind")]
pub enum VerificationFailed {
    #[serde(rename = "VerificationFailed::OutputMismatch")]
    #[error(transparent)]
    OutputMismatch(#[from] OutputMismatch),

    #[serde(rename = "VerificationFailed::DatasetNotFound")]
    #[error(transparent)]
    DatasetNotFound(#[from] DatasetNotFound),

    #[serde(rename = "VerificationFailed::DatasetBlockNotFound")]
    #[error(transparent)]
    DatasetBlockNotFound(#[from] DatasetBlockNotFound),
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct OutputMismatch {
    pub message: String,

    pub expected_hash: odf::Multihash,

    pub actual_hash: odf::Multihash,
}

impl OutputMismatch {
    pub fn new(expected_hash: odf::Multihash, actual_hash: odf::Multihash) -> Self {
        Self {
            // TODO: V1: Make message more assertive once we have high confidence in query
            // reproducibility
            message: "Query was reproduced but resulted in output hash different from expected. \
                      This means that the output was either falsified, or the query \
                      reproducibility was not guaranteed by the system."
                .to_string(),
            expected_hash,
            actual_hash,
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct DatasetNotFound {
    pub message: String,

    pub dataset_id: odf::DatasetID,
}

impl DatasetNotFound {
    pub fn new(dataset_id: odf::DatasetID) -> Self {
        Self {
            message: "Unable to reproduce the query as one of the input datasets cannot be found. \
                      The owner of dataset either deleted it or made private or this node \
                      requires additional configuration in order to locate it."
                .to_string(),
            dataset_id,
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, serde::Deserialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct DatasetBlockNotFound {
    pub message: String,

    pub dataset_id: odf::DatasetID,

    pub block_hash: odf::Multihash,
}

impl DatasetBlockNotFound {
    pub fn new(dataset_id: odf::DatasetID, block_hash: odf::Multihash) -> Self {
        Self {
            // TODO: V1: We need a way do differentiate history-altering changes from response
            // spoofing, otherwise we will not be able to assign the blame. We may need to consider
            // including these events into dataset metadata.
            message: "Unable to reproduce the query as one of the input datasets does not contain \
                      a block with specified hash. Under normal circumstances a block can \
                      disappear only when the owner of dataset performs history-altering \
                      operation such as reset or hard compaction. There is also a probability \
                      that block hash was spoofed in the original request to falsify the results."
                .to_string(),
            dataset_id,
            block_hash,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl VerifyResponse {
    pub fn success() -> Self {
        Self {
            ok: true,
            error: None,
        }
    }
}

impl<T> From<T> for VerifyResponse
where
    T: Into<ValidationError>,
{
    fn from(value: T) -> Self {
        Self {
            ok: false,
            error: Some(value.into()),
        }
    }
}

impl<T> From<T> for ValidationError
where
    T: Into<InvalidRequest>,
{
    fn from(value: T) -> Self {
        Self::InvalidRequest(value.into())
    }
}

impl From<OutputMismatch> for ValidationError {
    fn from(value: OutputMismatch) -> Self {
        Self::VerificationFailed(value.into())
    }
}

impl From<DatasetNotFound> for ValidationError {
    fn from(value: DatasetNotFound) -> Self {
        Self::VerificationFailed(value.into())
    }
}

impl From<DatasetBlockNotFound> for ValidationError {
    fn from(value: DatasetBlockNotFound) -> Self {
        Self::VerificationFailed(value.into())
    }
}

impl axum::response::IntoResponse for VerifyResponse {
    fn into_response(self) -> axum::response::Response {
        match &self.error {
            None => (http::StatusCode::OK, Json(self)).into_response(),
            Some(ValidationError::InvalidRequest(_) | ValidationError::VerificationFailed(_)) => {
                (http::StatusCode::BAD_REQUEST, Json(self)).into_response()
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
