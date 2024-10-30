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

use std::error::Error;

use axum::extract::Extension;
use axum::response::Json;
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use kamu_core::QueryError;
use opendatafabric as odf;

use super::{query_handler, query_types as query};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VerifyRequest {
    /// Inputs that will be used to reproduce the query
    pub input: query::QueryRequest,

    /// Information about processing performed by other nodes as part of the
    /// original operation
    pub sub_queries: Vec<query::SubQuery>,

    /// Commitment created by the original operation
    pub commitment: query::Commitment,

    /// Signature block
    pub proof: query::Proof,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct VerifyResponse {
    /// Whether validation was successful
    pub ok: bool,

    /// Will contain error details if validation was unsuccessful
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ValidationError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Verify query commitment
///
/// See [commitments documentation](https://docs.kamu.dev/node/commitments/) for details.
#[utoipa::path(
    post,
    path = "/verify",
    request_body = VerifyRequest,
    responses(
        (status = OK, body = VerifyResponse),
        (status = BAD_REQUEST, body = VerifyResponse),
    ),
    tag = "odf-query",
    security(
        (),
        ("api_key" = [])
    )
)]
#[tracing::instrument(level = "info", skip_all)]
#[transactional_handler]
pub async fn verify_handler(
    Extension(catalog): Extension<Catalog>,
    Json(request): Json<VerifyRequest>,
) -> Result<VerifyResponse, ApiError> {
    tracing::debug!(?request, "Verification request");

    assert_eq!(
        request.proof.r#type,
        query::ProofType::Ed25519Signature2020,
        "UnsupportedProofType",
    );

    let response = verify(catalog, request).await?;
    tracing::debug!(?response, "Verification response");
    Ok(response)
}

async fn verify(catalog: Catalog, request: VerifyRequest) -> Result<VerifyResponse, ApiError> {
    // 1. Validate request
    if request.commitment.input_hash
        != odf::Multihash::from_digest_sha3_256(&query::to_canonical_json(&request.input))
    {
        return Ok(VerifyResponse::from(InvalidRequestInputHash::new()));
    }

    if request.commitment.sub_queries_hash
        != odf::Multihash::from_digest_sha3_256(&query::to_canonical_json(&request.sub_queries))
    {
        return Ok(VerifyResponse::from(InvalidRequestSubQueriesHash::new()));
    }

    let did = request.proof.verification_method;
    let signature = request.proof.proof_value;
    if let Err(err) = did.verify(&query::to_canonical_json(&request.commitment), &signature) {
        return Ok(VerifyResponse::from(InvalidRequestBadSignature::new(
            &err.source()
                .map(std::string::ToString::to_string)
                .unwrap_or_default(),
        )));
    }

    // 2. Reproduce the query and compare data hashes
    let mut data_request = request.input.clone();

    // We are only interested in the output
    data_request.include.clear();

    let query_result =
        match query_handler::query_handler_post(axum::Extension(catalog), Json(data_request)).await
        {
            Ok(Json(v)) => Ok(v),
            Err(err) => match err.source().unwrap().downcast_ref::<QueryError>() {
                Some(QueryError::DatasetNotFound(err)) => {
                    return Ok(VerifyResponse::from(DatasetNotFound::new(
                        err.dataset_ref.id().unwrap().clone(),
                    )));
                }
                Some(QueryError::DatasetBlockNotFound(err)) => {
                    return Ok(VerifyResponse::from(DatasetBlockNotFound::new(
                        err.dataset_id.clone(),
                        err.block_hash.clone(),
                    )));
                }
                _ => Err(err),
            },
        }?;

    let output_hash_actual =
        odf::Multihash::from_digest_sha3_256(&query::to_canonical_json(&query_result.output));

    if request.commitment.output_hash != output_hash_actual {
        return Ok(VerifyResponse::from(OutputMismatch::new(
            request.commitment.output_hash,
            output_hash_actual,
        )));
    }

    Ok(VerifyResponse::success())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[serde(untagged)]
pub enum ValidationError {
    #[error(transparent)]
    InvalidRequest(InvalidRequest),
    #[error(transparent)]
    VerificationFailed(VerificationFailed),
}

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
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

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct InvalidRequestInputHash {
    pub message: &'static str,
}

impl InvalidRequestInputHash {
    fn new() -> Self {
        Self {
            message: "The commitment is invalid and cannot be disputed: commitment.inputHash \
                      doesn't match the hash of input object",
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct InvalidRequestSubQueriesHash {
    pub message: &'static str,
}

impl InvalidRequestSubQueriesHash {
    fn new() -> Self {
        Self {
            message: "The commitment is invalid and cannot be disputed: commitment.subQueriesHash \
                      doesn't match the hash of subQueries object",
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct InvalidRequestBadSignature {
    pub message: String,
}

impl InvalidRequestBadSignature {
    fn new(message: &str) -> Self {
        Self {
            message: format!("The commitment is invalid and cannot be disputed: {message}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
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

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct OutputMismatch {
    pub message: String,

    #[schema(value_type = String)]
    pub expected_hash: odf::Multihash,

    #[schema(value_type = String)]
    pub actual_hash: odf::Multihash,
}

impl OutputMismatch {
    fn new(expected_hash: odf::Multihash, actual_hash: odf::Multihash) -> Self {
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

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct DatasetNotFound {
    pub message: String,

    #[schema(value_type = String)]
    pub dataset_id: odf::DatasetID,
}

impl DatasetNotFound {
    fn new(dataset_id: odf::DatasetID) -> Self {
        Self {
            message: "Unable to reproduce the query as one of the input datasets cannot be found. \
                      The owner of dataset either deleted it or made private or this node \
                      requires additional configuration in order to locate it."
                .to_string(),
            dataset_id,
        }
    }
}

#[derive(Debug, thiserror::Error, serde::Serialize, utoipa::ToSchema)]
#[error("{message}")]
pub struct DatasetBlockNotFound {
    pub message: String,

    #[schema(value_type = String)]
    pub dataset_id: odf::DatasetID,

    #[schema(value_type = String)]
    pub block_hash: odf::Multihash,
}

impl DatasetBlockNotFound {
    fn new(dataset_id: odf::DatasetID, block_hash: odf::Multihash) -> Self {
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
    fn success() -> Self {
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
