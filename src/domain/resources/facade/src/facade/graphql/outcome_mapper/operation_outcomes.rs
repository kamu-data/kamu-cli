// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_resources as domain;

use super::batch_helpers::{
    collect_batch_problems,
    collect_batch_successes,
    validate_batch_response_indexes,
};
use super::problem_mappers::{
    account_resolution_problem_error,
    unsupported_selector_problem_error,
};
use crate::facade::graphql::cynic_api;
use crate::{
    BatchResourceError,
    BatchResourceResponse,
    ListResourcesError,
    RenderResourceManifestResult,
    ResourceLookupProblem,
    ResourcesSummaryError,
    SearchResourceHandlesResponse,
    SearchResourcesResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_summary_outcome(
    outcome: cynic_api::operations::summary::ResourcesSummaryOutcome,
) -> Result<domain::ResourcesSummary, ResourcesSummaryError> {
    use cynic_api::operations::summary::ResourcesSummaryOutcome as O;

    match outcome {
        O::ResourcesSummary(summary) => summary.try_into().map_err(ResourcesSummaryError::Internal),
        O::ResourceAccountResolutionProblem(problem) => Err(
            ResourcesSummaryError::AccountResolution(account_resolution_problem_error(problem)),
        ),
        O::Unknown => Err(ResourcesSummaryError::Internal(InternalError::new(
            "Remote summary returned an unrecognized ResourcesSummaryOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_search_outcome(
    outcome: cynic_api::operations::search_summaries::ResourceListOutcome,
) -> Result<SearchResourcesResponse, ListResourcesError> {
    use cynic_api::operations::search_summaries::ResourceListOutcome as O;

    match outcome {
        O::ResourceConnection(connection) => {
            let total_count = usize::try_from(connection.total_count).map_err(|_| {
                ListResourcesError::Internal(InternalError::new(format!(
                    "Remote search total_count {} cannot be converted to usize",
                    connection.total_count
                )))
            })?;

            Ok(SearchResourcesResponse {
                items: connection
                    .nodes
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>, InternalError>>()
                    .map_err(ListResourcesError::Internal)?,
                total_count,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceAccountResolutionProblem(problem) => Err(ListResourcesError::AccountResolution(
            account_resolution_problem_error(problem),
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote search returned an unrecognized ResourceListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_search_handles_outcome(
    outcome: cynic_api::operations::search::ResourceHandleListOutcome,
) -> Result<SearchResourceHandlesResponse, ListResourcesError> {
    use cynic_api::operations::search::ResourceHandleListOutcome as O;

    match outcome {
        O::ResourceHandleConnection(connection) => {
            let total_count = usize::try_from(connection.total_count).map_err(|_| {
                ListResourcesError::Internal(InternalError::new(format!(
                    "Remote search total_count {} cannot be converted to usize",
                    connection.total_count
                )))
            })?;

            Ok(SearchResourceHandlesResponse {
                items: connection.nodes.into_iter().map(Into::into).collect(),
                total_count,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceAccountResolutionProblem(problem) => Err(ListResourcesError::AccountResolution(
            account_resolution_problem_error(problem),
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote search returned an unrecognized ResourceHandleListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_get_resources_outcome(
    outcome: cynic_api::operations::get::BatchResourcesOutcome,
    resource_refs: &[domain::ResourceRef],
) -> Result<BatchResourceResponse<domain::Resource, ResourceLookupProblem>, BatchResourceError> {
    use cynic_api::operations::get::BatchResourcesOutcome as O;
    match outcome {
        O::BatchResourcesResult(batch) => {
            let successes =
                collect_batch_successes(resource_refs.len(), batch.resources, "resource", |s| {
                    Ok((
                        s.request_index,
                        s.resource
                            .try_into()
                            .map_err(BatchResourceError::Internal)?,
                    ))
                })?;
            let problems = collect_batch_problems(batch.problems, resource_refs.len(), "resource")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                resource_refs.len(),
                "resource",
            )?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceAccountResolutionProblem(problem) => Err(BatchResourceError::AccountResolution(
            account_resolution_problem_error(problem),
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote get returned an unrecognized BatchResourcesOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_get_handles_outcome(
    outcome: cynic_api::operations::handle::BatchResourceHandlesOutcome,
    resource_refs: &[domain::ResourceRef],
) -> Result<BatchResourceResponse<domain::ResourceHandle, ResourceLookupProblem>, BatchResourceError>
{
    use cynic_api::operations::handle::BatchResourceHandlesOutcome as O;
    match outcome {
        O::BatchResourceHandlesResult(batch) => {
            let successes =
                collect_batch_successes(resource_refs.len(), batch.handles, "handle", |s| {
                    Ok((s.request_index, s.handle.into()))
                })?;
            let problems = collect_batch_problems(batch.problems, resource_refs.len(), "handle")?;
            validate_batch_response_indexes(&successes, &problems, resource_refs.len(), "handle")?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceAccountResolutionProblem(problem) => Err(BatchResourceError::AccountResolution(
            account_resolution_problem_error(problem),
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote get_handles returned an unrecognized BatchResourceHandlesOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_render_manifests_outcome(
    outcome: cynic_api::operations::render_manifest::BatchResourceManifestsOutcome,
    resource_refs: &[domain::ResourceRef],
) -> Result<
    BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
    BatchResourceError,
> {
    use cynic_api::operations::render_manifest::BatchResourceManifestsOutcome as O;
    match outcome {
        O::BatchResourceManifestsResult(batch) => {
            let successes =
                collect_batch_successes(resource_refs.len(), batch.manifests, "manifest", |s| {
                    Ok((s.request_index, s.manifest.into()))
                })?;
            let problems = collect_batch_problems(batch.problems, resource_refs.len(), "manifest")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                resource_refs.len(),
                "manifest",
            )?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceAccountResolutionProblem(problem) => Err(BatchResourceError::AccountResolution(
            account_resolution_problem_error(problem),
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote render_manifests returned an unrecognized BatchResourceManifestsOutcome \
             variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_delete_outcome(
    outcome: cynic_api::operations::delete::ResourceDeleteOutcome,
    resource_refs: &[domain::ResourceRef],
) -> Result<BatchResourceResponse<domain::ResourceID, ResourceLookupProblem>, BatchResourceError> {
    use cynic_api::operations::delete::ResourceDeleteOutcome as O;
    match outcome {
        O::ResourceDeleteResult(batch) => {
            let successes =
                collect_batch_successes(resource_refs.len(), batch.resources, "delete", |s| {
                    Ok((s.request_index, s.resource_id))
                })?;
            let problems = collect_batch_problems(batch.problems, resource_refs.len(), "delete")?;
            validate_batch_response_indexes(&successes, &problems, resource_refs.len(), "delete")?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceAccountResolutionProblem(problem) => Err(BatchResourceError::AccountResolution(
            account_resolution_problem_error(problem),
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote delete returned an unrecognized ResourceDeleteOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
