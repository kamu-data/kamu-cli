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
    bad_account_problem_error,
    map_selector_problem_result,
    unsupported_selector_problem_error,
};
use crate::facade::graphql::cynic_api;
use crate::{
    BatchResourceError,
    BatchResourceResponse,
    DeleteResourceError,
    GetResourceError,
    ListAllResourcesError,
    ListResourcesError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceLookupProblem,
    ResourcesSummaryError,
    SearchResourceHandlesResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_summary_outcome(
    outcome: cynic_api::operations::summary::ResourcesSummaryOutcome,
) -> Result<domain::ResourcesSummary, ResourcesSummaryError> {
    use cynic_api::operations::summary::ResourcesSummaryOutcome as O;

    match outcome {
        O::ResourcesSummary(summary) => summary.try_into().map_err(ResourcesSummaryError::Internal),
        O::ResourceBadAccountProblem(problem) => Err(ResourcesSummaryError::BadAccount(
            bad_account_problem_error(problem).map_err(ResourcesSummaryError::Internal)?,
        )),
        O::Unknown => Err(ResourcesSummaryError::Internal(InternalError::new(
            "Remote summary returned an unrecognized ResourcesSummaryOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_list_outcome(
    outcome: cynic_api::operations::list::ResourceListOutcome,
) -> Result<Vec<domain::ResourceSummaryView>, ListResourcesError> {
    use cynic_api::operations::list::ResourceListOutcome as O;

    match outcome {
        O::ResourceConnection(connection) => connection
            .nodes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(ListResourcesError::Internal),
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListResourcesError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote list returned an unrecognized ResourceListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_list_all_outcome(
    outcome: cynic_api::operations::list::ResourceListAllOutcome,
) -> Result<Vec<domain::ResourceSummaryView>, ListAllResourcesError> {
    use cynic_api::operations::list::ResourceListAllOutcome as O;

    match outcome {
        O::ResourceConnection(connection) => connection
            .nodes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(ListAllResourcesError::Internal),
        O::ResourceBadAccountProblem(problem) => Err(ListAllResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListAllResourcesError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListAllResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListAllResourcesError::Internal(InternalError::new(
            "Remote list_all returned an unrecognized ResourceListAllOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_list_handles_outcome(
    outcome: cynic_api::operations::list::ResourceHandleListOutcome,
) -> Result<Vec<domain::ResourceHandle>, ListResourcesError> {
    use cynic_api::operations::list::ResourceHandleListOutcome as O;

    match outcome {
        O::ResourceHandleConnection(connection) => {
            Ok(connection.nodes.into_iter().map(Into::into).collect())
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListResourcesError::Internal)?,
        )),
        O::ResourceInvalidSearchQueryProblem(problem) => {
            drop(problem.message);
            Err(crate::InvalidResourceSearchQueryError.into())
        }
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote list_handles returned an unrecognized ResourceHandleListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_list_all_handles_outcome(
    outcome: cynic_api::operations::list::ResourceHandleListAllOutcome,
) -> Result<Vec<domain::ResourceHandle>, ListAllResourcesError> {
    use cynic_api::operations::list::ResourceHandleListAllOutcome as O;

    match outcome {
        O::ResourceHandleConnection(connection) => {
            Ok(connection.nodes.into_iter().map(Into::into).collect())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListAllResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListAllResourcesError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListAllResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListAllResourcesError::Internal(InternalError::new(
            "Remote list_all_handles returned an unrecognized ResourceHandleListAllOutcome variant",
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
        O::ResourceBadAccountProblem(problem) => Err(ListResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListResourcesError::Internal)?,
        )),
        O::ResourceInvalidSearchQueryProblem(problem) => {
            drop(problem.message);
            Err(crate::InvalidResourceSearchQueryError.into())
        }
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(ListResourcesError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote search returned an unrecognized ResourceHandleListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_get_resource_outcome(
    outcome: cynic_api::operations::get_resource::ResourceGetOutcome,
) -> Result<domain::Resource, GetResourceError> {
    use cynic_api::operations::get_resource::ResourceGetOutcome as O;
    match outcome {
        O::Resource(r) => r.try_into().map_err(GetResourceError::Internal),
        O::ResourceSelectorProblemResult(p) => Err(map_selector_problem_result(
            p,
            GetResourceError::LookupProblem,
            Into::into,
            GetResourceError::BadAccount,
        )
        .map_err(GetResourceError::Internal)?),
        O::Unknown => Err(GetResourceError::Internal(InternalError::new(
            "Remote get returned an unrecognized ResourceGetOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_get_handle_outcome(
    outcome: cynic_api::operations::handle::ResourceGetHandleOutcome,
) -> Result<domain::ResourceHandle, GetResourceError> {
    use cynic_api::operations::handle::ResourceGetHandleOutcome as O;
    match outcome {
        O::ResourceHandle(h) => Ok(h.into()),
        O::ResourceSelectorProblemResult(p) => Err(map_selector_problem_result(
            p,
            GetResourceError::LookupProblem,
            Into::into,
            GetResourceError::BadAccount,
        )
        .map_err(GetResourceError::Internal)?),
        O::Unknown => Err(GetResourceError::Internal(InternalError::new(
            "Remote get_handle returned an unrecognized ResourceGetHandleOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_delete_outcome(
    outcome: cynic_api::operations::delete::ResourceDeleteOutcome,
) -> Result<domain::ResourceID, DeleteResourceError> {
    use cynic_api::operations::delete::ResourceDeleteOutcome as O;
    match outcome {
        O::ResourceDeleteSuccess(s) => Ok(s.resource_id),
        O::ResourceSelectorProblemResult(p) => Err(map_selector_problem_result(
            p,
            DeleteResourceError::LookupProblem,
            Into::into,
            Into::into,
        )
        .map_err(DeleteResourceError::Internal)?),
        O::Unknown => Err(DeleteResourceError::Internal(InternalError::new(
            "Remote delete returned an unrecognized ResourceDeleteOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_get_resources_outcome(
    outcome: cynic_api::operations::get_resources::BatchResourcesOutcome,
    selector: &ResourceBatchSelector,
) -> Result<BatchResourceResponse<domain::Resource, ResourceLookupProblem>, BatchResourceError> {
    use cynic_api::operations::get_resources::BatchResourcesOutcome as O;
    match outcome {
        O::BatchResourcesResult(batch) => {
            let successes = collect_batch_successes(
                selector.resource_refs.len(),
                batch.resources,
                "resource",
                |s| {
                    Ok((
                        s.request_index,
                        s.resource
                            .try_into()
                            .map_err(BatchResourceError::Internal)?,
                    ))
                },
            )?;
            let problems =
                collect_batch_problems(batch.problems, selector.resource_refs.len(), "resource")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                selector.resource_refs.len(),
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
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(BatchResourceError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote get_many returned an unrecognized BatchResourcesOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_get_handles_outcome(
    outcome: cynic_api::operations::handle::BatchResourceHandlesOutcome,
    selector: &ResourceBatchSelector,
) -> Result<BatchResourceResponse<domain::ResourceHandle, ResourceLookupProblem>, BatchResourceError>
{
    use cynic_api::operations::handle::BatchResourceHandlesOutcome as O;
    match outcome {
        O::BatchResourceHandlesResult(batch) => {
            let successes = collect_batch_successes(
                selector.resource_refs.len(),
                batch.handles,
                "handle",
                |s| Ok((s.request_index, s.handle.into())),
            )?;
            let problems =
                collect_batch_problems(batch.problems, selector.resource_refs.len(), "handle")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                selector.resource_refs.len(),
                "handle",
            )?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(BatchResourceError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote get_handles returned an unrecognized BatchResourceHandlesOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_render_manifests_outcome(
    outcome: cynic_api::operations::render_manifest::BatchResourceManifestsOutcome,
    selector: &ResourceBatchSelector,
) -> Result<
    BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
    BatchResourceError,
> {
    use cynic_api::operations::render_manifest::BatchResourceManifestsOutcome as O;
    match outcome {
        O::BatchResourceManifestsResult(batch) => {
            let successes = collect_batch_successes(
                selector.resource_refs.len(),
                batch.manifests,
                "manifest",
                |s| Ok((s.request_index, s.manifest.into())),
            )?;
            let problems =
                collect_batch_problems(batch.problems, selector.resource_refs.len(), "manifest")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                selector.resource_refs.len(),
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
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(BatchResourceError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote render_manifests returned an unrecognized BatchResourceManifestsOutcome \
             variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_delete_many_outcome(
    outcome: cynic_api::operations::delete::ResourceDeleteManyOutcome,
    selector: &ResourceBatchSelector,
) -> Result<BatchResourceResponse<domain::ResourceID, ResourceLookupProblem>, BatchResourceError> {
    use cynic_api::operations::delete::ResourceDeleteManyOutcome as O;
    match outcome {
        O::ResourceDeleteManyResult(batch) => {
            let successes = collect_batch_successes(
                selector.resource_refs.len(),
                batch.resources,
                "delete",
                |s| Ok((s.request_index, s.resource_id)),
            )?;
            let problems =
                collect_batch_problems(batch.problems, selector.resource_refs.len(), "delete")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                selector.resource_refs.len(),
                "delete",
            )?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedSelectorProblem(problem) => {
            Err(unsupported_selector_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::ResourceInvalidLabelFilterProblem(problem) => {
            Err(BatchResourceError::InvalidLabelFilter(problem.into()))
        }
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote delete_many returned an unrecognized ResourceDeleteManyOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_render_manifest_outcome(
    outcome: cynic_api::operations::render_manifest::ResourceRenderManifestOutcome,
) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
    use cynic_api::operations::render_manifest::ResourceRenderManifestOutcome as O;
    match outcome {
        O::ResourceRenderManifestResult(r) => Ok(r.into()),
        O::ResourceSelectorProblemResult(p) => Err(map_selector_problem_result(
            p,
            RenderResourceManifestError::LookupProblem,
            Into::into,
            RenderResourceManifestError::BadAccount,
        )
        .map_err(RenderResourceManifestError::Internal)?),
        O::Unknown => Err(RenderResourceManifestError::Internal(InternalError::new(
            "Remote render_manifest returned an unrecognized ResourceRenderManifestOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
