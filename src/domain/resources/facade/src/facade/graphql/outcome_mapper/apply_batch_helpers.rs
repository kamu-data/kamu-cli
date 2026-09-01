// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::GraphqlHttpRequestError;
use internal_error::InternalError;
use kamu_resources as domain;
use serde::Deserialize;

use crate::facade::graphql::cynic_api;
use crate::{
    ApplyManifestBatchItemResult,
    ApplyManifestBatchResponse,
    ApplyManifestError,
    BatchResourceError,
    ParseResourceManifestError,
    ResourceAccountResolutionError,
    ResourceAccountResolutionProblemCode,
    ResourceHeadersValidationProblemCode,
    ResourceInvalidHeadersError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_apply_manifests_planning_outcome(
    result: cynic_api::operations::apply_batch::ResourceApplyManifestsResult,
) -> Result<ApplyManifestBatchResponse<domain::ApplyManifestPlanningDecision>, BatchResourceError> {
    let items = result
        .items
        .into_iter()
        .map(|item| {
            let request_index = to_request_index(item.request_index)?;
            let outcome = item.outcome.try_into_planning_decision();
            Ok(ApplyManifestBatchItemResult {
                request_index,
                outcome,
            })
        })
        .collect::<Result<Vec<_>, BatchResourceError>>()?;

    Ok(ApplyManifestBatchResponse {
        items,
        rolled_back_successes: Vec::new(),
    })
}

pub(crate) fn map_batch_apply_manifests_application_outcome(
    result: cynic_api::operations::apply_batch::ResourceApplyManifestsResult,
) -> Result<ApplyManifestBatchResponse<domain::ApplyManifestApplicationDecision>, BatchResourceError>
{
    let items = result
        .items
        .into_iter()
        .map(|item| {
            let request_index = to_request_index(item.request_index)?;
            let outcome = item.outcome.try_into_application_decision();
            Ok(ApplyManifestBatchItemResult {
                request_index,
                outcome,
            })
        })
        .collect::<Result<Vec<_>, BatchResourceError>>()?;

    Ok(ApplyManifestBatchResponse {
        items,
        rolled_back_successes: Vec::new(),
    })
}

fn to_request_index(raw_index: i32) -> Result<usize, BatchResourceError> {
    usize::try_from(raw_index).map_err(|_| {
        BatchResourceError::Internal(InternalError::new(format!(
            "Remote apply_manifests item index {raw_index} cannot be converted to usize",
        )))
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Decodes the `extensions.batch` payload the server attaches to the
/// transport-level GraphQL error it raises when a batch apply does not fully
/// succeed (see `ResourcesMut::apply_manifests` / `GqlError::gql_extended`).
/// The server rolled the whole transaction back before returning this error,
/// so — unlike the success path's `data` — there is nothing persisted for
/// items that individually succeeded before the rollback; their indexes are
/// carried separately in `rolled_back_successes` rather than misrepresented
/// as an item outcome. Rollback is batch transaction metadata, not a
/// per-item error.
#[derive(Debug, Deserialize)]
struct ApplyManifestBatchSummary {
    items: Vec<ApplyManifestItemSummary>,
    rolled_back_successes: Vec<usize>,
}

#[derive(Debug, Deserialize)]
struct ApplyManifestItemSummary {
    request_index: usize,
    outcome: ApplyManifestItemOutcomeSummary,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
enum ApplyManifestItemOutcomeSummary {
    Rejected {
        category: domain::ApplyResourceRejectionCategory,
        message: String,
    },
    ParseManifest {
        message: String,
    },
    UnsupportedDescriptor {
        schema: domain::TypeUri,
    },
    AccountResolution {
        code: ResourceAccountResolutionProblemCode,
        message: String,
    },
    InvalidHeaders {
        code: ResourceHeadersValidationProblemCode,
        message: String,
    },
    InvalidSpec {
        schema: domain::TypeUri,
        message: String,
    },
    IDNotFound {
        id: domain::ResourceID,
    },
    TypeMismatch {
        id: domain::ResourceID,
        expected_schema: domain::TypeUri,
        actual_schema: domain::TypeUri,
    },
    ConcurrentModification,
    Failed {
        message: String,
    },
}

fn decode_rollback_summary(error: &GraphqlHttpRequestError) -> Option<ApplyManifestBatchSummary> {
    let GraphqlHttpRequestError::Graphql {
        extensions: Some(extensions),
        ..
    } = error
    else {
        return None;
    };

    let batch = extensions.get("batch")?;
    serde_json::from_value(batch.clone()).ok()
}

pub(crate) fn map_batch_apply_manifests_planning_rollback(
    error: GraphqlHttpRequestError,
) -> Result<ApplyManifestBatchResponse<domain::ApplyManifestPlanningDecision>, BatchResourceError> {
    let Some(summary) = decode_rollback_summary(&error) else {
        return Err(error.into());
    };

    let items = summary
        .items
        .into_iter()
        .map(|item| ApplyManifestBatchItemResult {
            request_index: item.request_index,
            outcome: rollback_outcome(item.outcome)
                .map(domain::ApplyManifestPlanningDecision::Rejected),
        })
        .collect();

    Ok(ApplyManifestBatchResponse {
        items,
        rolled_back_successes: summary.rolled_back_successes,
    })
}

pub(crate) fn map_batch_apply_manifests_application_rollback(
    error: GraphqlHttpRequestError,
) -> Result<ApplyManifestBatchResponse<domain::ApplyManifestApplicationDecision>, BatchResourceError>
{
    let Some(summary) = decode_rollback_summary(&error) else {
        return Err(error.into());
    };

    let items = summary
        .items
        .into_iter()
        .map(|item| ApplyManifestBatchItemResult {
            request_index: item.request_index,
            outcome: rollback_outcome(item.outcome)
                .map(domain::ApplyManifestApplicationDecision::Rejected),
        })
        .collect();

    Ok(ApplyManifestBatchResponse {
        items,
        rolled_back_successes: summary.rolled_back_successes,
    })
}

/// A rejected item round-trips as a genuine business rejection, matching what
/// the local facade would have returned had the batch not rolled back. Typed
/// per-item failures are reconstructed from the rollback summary whenever the
/// original variant has stable serializable fields; transport/internal
/// failures remain opaque `Internal` errors.
fn rollback_outcome(
    summary: ApplyManifestItemOutcomeSummary,
) -> Result<domain::ApplyManifestRejection, ApplyManifestError> {
    match summary {
        ApplyManifestItemOutcomeSummary::Rejected { category, message } => {
            Ok(domain::ApplyManifestRejection { category, message })
        }
        ApplyManifestItemOutcomeSummary::ParseManifest { message } => Err(
            ApplyManifestError::ParseManifest(ParseResourceManifestError { message }),
        ),
        ApplyManifestItemOutcomeSummary::UnsupportedDescriptor { schema } => {
            Err(ApplyManifestError::UnsupportedDescriptor(
                domain::UnsupportedResourceDescriptorError::NotFound { schema },
            ))
        }
        ApplyManifestItemOutcomeSummary::AccountResolution { code, message } => Err(
            ApplyManifestError::AccountResolution(ResourceAccountResolutionError { code, message }),
        ),
        ApplyManifestItemOutcomeSummary::InvalidHeaders { code, message } => Err(
            ApplyManifestError::InvalidHeaders(ResourceInvalidHeadersError { code, message }),
        ),
        ApplyManifestItemOutcomeSummary::InvalidSpec { schema, message } => Err(
            ApplyManifestError::InvalidSpec(domain::ResourceInvalidSpecError { schema, message }),
        ),
        ApplyManifestItemOutcomeSummary::IDNotFound { id } => Err(ApplyManifestError::IDNotFound(
            domain::ResourceIDNotFoundError(id),
        )),
        ApplyManifestItemOutcomeSummary::TypeMismatch {
            id,
            expected_schema,
            actual_schema,
        } => Err(ApplyManifestError::TypeMismatch(
            domain::ResourceTypeMismatchError::new(id, expected_schema, actual_schema),
        )),
        ApplyManifestItemOutcomeSummary::ConcurrentModification => {
            Err(ApplyManifestError::ConcurrentModification(
                event_sourcing::ConcurrentModificationError {},
            ))
        }
        ApplyManifestItemOutcomeSummary::Failed { message } => {
            Err(ApplyManifestError::Internal(InternalError::new(message)))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
