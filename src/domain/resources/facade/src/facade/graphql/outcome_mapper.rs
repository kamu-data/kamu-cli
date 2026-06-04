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

use crate::facade::graphql::cynic_api;
use crate::{
    BatchResourceError,
    BatchResourceProblem,
    BatchResourceResponse,
    BatchResourceSuccess,
    DeleteResourceError,
    GetResourceError,
    ListAllResourcesError,
    ListResourcesError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceKindMismatchError,
    ResourceLookupProblem,
    ResourcesSummaryError,
    SearchResourceIdentitiesResponse,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn collect_batch_successes<S, T, F>(
    request_len: usize,
    items: Vec<S>,
    context: &str,
    map_item: F,
) -> Result<Vec<BatchResourceSuccess<T>>, BatchResourceError>
where
    F: Fn(S) -> Result<(i32, T), BatchResourceError>,
{
    let mut seen = std::collections::BTreeSet::new();
    items
        .into_iter()
        .map(|s| {
            let (raw_index, item) = map_item(s)?;
            let request_index = usize::try_from(raw_index).map_err(|_| {
                BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} success index {raw_index} cannot be converted to usize",
                )))
            })?;
            if request_index >= request_len {
                return Err(BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} success index {request_index} is out of bounds",
                ))));
            }
            if !seen.insert(request_index) {
                return Err(BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} returned duplicate success index {request_index}",
                ))));
            }
            Ok(BatchResourceSuccess {
                request_index,
                item,
            })
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn collect_batch_problems(
    problems: Vec<cynic_api::fragments::BatchResourceProblem>,
    request_len: usize,
    context: &str,
) -> Result<Vec<BatchResourceProblem<ResourceLookupProblem>>, BatchResourceError> {
    problems
        .into_iter()
        .map(|problem| {
            let request_index = usize::try_from(problem.request_index).map_err(|_| {
                BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} problem index {} cannot be converted to usize",
                    problem.request_index
                )))
            })?;
            if request_index >= request_len {
                return Err(BatchResourceError::Internal(InternalError::new(format!(
                    "Remote {context} problem index {request_index} is out of bounds",
                ))));
            }
            let error = map_batch_lookup_problem(problem.problem)?;
            Ok(BatchResourceProblem {
                request_index,
                error,
            })
        })
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn validate_batch_response_indexes<T, E>(
    successes: &[BatchResourceSuccess<T>],
    problems: &[BatchResourceProblem<E>],
    request_len: usize,
    context: &str,
) -> Result<(), BatchResourceError> {
    let mut seen: std::collections::BTreeMap<usize, &str> = std::collections::BTreeMap::new();

    for s in successes {
        if s.request_index >= request_len {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} success index {} is out of bounds",
                s.request_index,
            ))));
        }
        if seen.insert(s.request_index, "success").is_some() {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} returned duplicate result index {}",
                s.request_index,
            ))));
        }
    }

    for p in problems {
        if p.request_index >= request_len {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} problem index {} is out of bounds",
                p.request_index,
            ))));
        }
        if let Some(previous) = seen.insert(p.request_index, "problem") {
            return Err(BatchResourceError::Internal(InternalError::new(format!(
                "Remote {context} returned both {previous} and problem for index {}",
                p.request_index,
            ))));
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_lookup_problem(
    problem: cynic_api::fragments::ResourceLookupProblem,
) -> Result<ResourceLookupProblem, BatchResourceError> {
    use cynic_api::fragments::ResourceLookupProblem as P;
    match problem {
        P::ResourceUIDNotFoundProblem(p) => Ok(map_uid_not_found(p)),
        P::ResourceNameNotFoundProblem(p) => Ok(map_name_not_found(p)),
        P::ResourceApiVersionMismatchProblem(p) => Ok(map_api_version_mismatch(p)),
        P::ResourceKindMismatchProblem(p) => Ok(map_kind_mismatch(p)),
        P::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote batch problem contains unrecognized ResourceLookupProblem variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn unsupported_descriptor_problem_error(
    problem: cynic_api::fragments::ResourceUnsupportedDescriptorProblem,
) -> domain::UnsupportedResourceDescriptorError {
    use cynic_api::fragments::ResourceUnsupportedDescriptorProblemCode as C;

    match problem.code {
        C::NotFound => domain::UnsupportedResourceDescriptorError::NotFound {
            kind: problem.kind,
            api_version: problem.api_version,
        },
        C::Duplicate => domain::UnsupportedResourceDescriptorError::Duplicate {
            kind: problem.kind,
            api_version: problem.api_version,
        },
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn bad_account_problem_error(
    problem: cynic_api::fragments::ResourceBadAccountProblem,
) -> Result<crate::ResolveManifestAccountError, InternalError> {
    use cynic_api::fragments::ResourceBadAccountProblemCode as C;

    Ok(match problem.code {
        C::EmptySelector => crate::ResolveManifestAccountError::EmptySelector,
        C::AccountNotFoundById => crate::ResolveManifestAccountError::AccountNotFoundById(
            kamu_accounts::AccountNotFoundByIdError {
                account_id: problem.account_id.ok_or_else(|| {
                    InternalError::new("Malformed remote bad account problem: missing account_id")
                })?,
            },
        ),
        C::AccountNotFoundByName => crate::ResolveManifestAccountError::AccountNotFoundByName(
            kamu_accounts::AccountNotFoundByNameError {
                account_name: account_name_from_problem(problem.account_name, "account_name")?,
            },
        ),
        C::IdNameMismatch => crate::ResolveManifestAccountError::IdNameMismatch {
            account_id: problem.account_id.ok_or_else(|| {
                InternalError::new("Malformed remote bad account problem: missing account_id")
            })?,
            expected_name: account_name_from_problem(problem.expected_name, "expected_name")?,
            actual_name: account_name_from_problem(problem.actual_name, "actual_name")?,
        },
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn account_name_from_problem(
    value: Option<cynic_api::scalars::AccountName>,
    field: &str,
) -> Result<odf::AccountName, InternalError> {
    value
        .map(|name| odf::AccountName::new_unchecked(&name.0))
        .ok_or_else(|| {
            InternalError::new(format!(
                "Malformed remote bad account problem: missing {field}"
            ))
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_uid_not_found(
    p: cynic_api::fragments::ResourceUIDNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::UIDNotFound(domain::ResourceUIDNotFoundError(p.uid))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_name_not_found(
    p: cynic_api::fragments::ResourceNameNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
        kind: p.kind,
        name: p.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_api_version_mismatch(
    p: cynic_api::fragments::ResourceApiVersionMismatchProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::ApiVersionMismatch(domain::ResourceAPIVersionMismatchError {
        expected_api_version: p.expected_api_version,
        actual_api_version: p.actual_api_version,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_kind_mismatch(
    p: cynic_api::fragments::ResourceKindMismatchProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
        uid: p.uid,
        expected_kind: p.expected_kind,
        actual_kind: p.actual_kind,
    })
}

pub(super) fn map_selector_problem_result<E, FLookup, FUnsupported, FBadAccount>(
    result: cynic_api::fragments::ResourceSelectorProblemResult,
    map_lookup: FLookup,
    map_unsupported: FUnsupported,
    map_bad_account: FBadAccount,
) -> Result<E, InternalError>
where
    FLookup: FnOnce(ResourceLookupProblem) -> E,
    FUnsupported: FnOnce(domain::UnsupportedResourceDescriptorError) -> E,
    FBadAccount: FnOnce(crate::ResolveManifestAccountError) -> E,
{
    use cynic_api::fragments::ResourceSelectorProblem as P;
    match result.problem {
        P::ResourceUIDNotFoundProblem(p) => Ok(map_lookup(map_uid_not_found(p))),
        P::ResourceNameNotFoundProblem(p) => Ok(map_lookup(map_name_not_found(p))),
        P::ResourceApiVersionMismatchProblem(p) => Ok(map_lookup(map_api_version_mismatch(p))),
        P::ResourceKindMismatchProblem(p) => Ok(map_lookup(map_kind_mismatch(p))),
        P::ResourceUnsupportedDescriptorProblem(p) => {
            Ok(map_unsupported(unsupported_descriptor_problem_error(p)))
        }
        P::ResourceBadAccountProblem(p) => Ok(map_bad_account(bad_account_problem_error(p)?)),
        P::Unknown => Err(InternalError::new(
            "Remote returned an unrecognized ResourceSelectorProblem variant",
        )),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_summary_outcome(
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

pub(super) fn map_list_outcome(
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
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListResourcesError::Internal)?,
        )),
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote list returned an unrecognized ResourceListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_list_all_outcome(
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
        O::Unknown => Err(ListAllResourcesError::Internal(InternalError::new(
            "Remote list_all returned an unrecognized ResourceListAllOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_list_identities_outcome(
    outcome: cynic_api::operations::list::ResourceIdentityListOutcome,
) -> Result<Vec<domain::ResourceIdentityView>, ListResourcesError> {
    use cynic_api::operations::list::ResourceIdentityListOutcome as O;

    match outcome {
        O::ResourceIdentityConnection(connection) => {
            Ok(connection.nodes.into_iter().map(Into::into).collect())
        }
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListResourcesError::Internal)?,
        )),
        O::ResourceInvalidSearchQueryProblem(problem) => {
            drop(problem.message);
            Err(crate::InvalidResourceSearchQueryError.into())
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote list_identities returned an unrecognized ResourceIdentityListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_list_all_identities_outcome(
    outcome: cynic_api::operations::list::ResourceIdentityListAllOutcome,
) -> Result<Vec<domain::ResourceIdentityView>, ListAllResourcesError> {
    use cynic_api::operations::list::ResourceIdentityListAllOutcome as O;

    match outcome {
        O::ResourceIdentityConnection(connection) => {
            Ok(connection.nodes.into_iter().map(Into::into).collect())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListAllResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListAllResourcesError::Internal)?,
        )),
        O::Unknown => Err(ListAllResourcesError::Internal(InternalError::new(
            "Remote list_all_identities returned an unrecognized ResourceIdentityListAllOutcome \
             variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_search_identities_outcome(
    outcome: cynic_api::operations::search::ResourceIdentityListOutcome,
) -> Result<SearchResourceIdentitiesResponse, ListResourcesError> {
    use cynic_api::operations::search::ResourceIdentityListOutcome as O;

    match outcome {
        O::ResourceIdentityConnection(connection) => {
            let total_count = usize::try_from(connection.total_count).map_err(|_| {
                ListResourcesError::Internal(InternalError::new(format!(
                    "Remote search total_count {} cannot be converted to usize",
                    connection.total_count
                )))
            })?;

            Ok(SearchResourceIdentitiesResponse {
                items: connection.nodes.into_iter().map(Into::into).collect(),
                total_count,
            })
        }
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(ListResourcesError::BadAccount(
            bad_account_problem_error(problem).map_err(ListResourcesError::Internal)?,
        )),
        O::ResourceInvalidSearchQueryProblem(problem) => {
            drop(problem.message);
            Err(crate::InvalidResourceSearchQueryError.into())
        }
        O::Unknown => Err(ListResourcesError::Internal(InternalError::new(
            "Remote search returned an unrecognized ResourceIdentityListOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_get_resource_outcome(
    outcome: cynic_api::operations::get_resource::ResourceGetOutcome,
) -> Result<domain::ResourceView, GetResourceError> {
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

pub(super) fn map_get_identity_outcome(
    outcome: cynic_api::operations::identity::ResourceGetIdentityOutcome,
) -> Result<domain::ResourceIdentityView, GetResourceError> {
    use cynic_api::operations::identity::ResourceGetIdentityOutcome as O;
    match outcome {
        O::ResourceIdentity(i) => Ok(i.into()),
        O::ResourceSelectorProblemResult(p) => Err(map_selector_problem_result(
            p,
            GetResourceError::LookupProblem,
            Into::into,
            GetResourceError::BadAccount,
        )
        .map_err(GetResourceError::Internal)?),
        O::Unknown => Err(GetResourceError::Internal(InternalError::new(
            "Remote get_identity returned an unrecognized ResourceGetIdentityOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_delete_outcome(
    outcome: cynic_api::operations::delete::ResourceDeleteOutcome,
) -> Result<domain::ResourceUID, DeleteResourceError> {
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

pub(super) fn map_batch_get_resources_outcome(
    outcome: cynic_api::operations::get_resources::BatchResourcesOutcome,
    selector: &ResourceBatchSelector,
) -> Result<BatchResourceResponse<domain::ResourceView, ResourceLookupProblem>, BatchResourceError>
{
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
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote get_many returned an unrecognized BatchResourcesOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_batch_get_identities_outcome(
    outcome: cynic_api::operations::identity::BatchResourceIdentitiesOutcome,
    selector: &ResourceBatchSelector,
) -> Result<
    BatchResourceResponse<domain::ResourceIdentityView, ResourceLookupProblem>,
    BatchResourceError,
> {
    use cynic_api::operations::identity::BatchResourceIdentitiesOutcome as O;
    match outcome {
        O::BatchResourceIdentitiesResult(batch) => {
            let successes = collect_batch_successes(
                selector.resource_refs.len(),
                batch.identities,
                "identity",
                |s| Ok((s.request_index, s.identity.into())),
            )?;
            let problems =
                collect_batch_problems(batch.problems, selector.resource_refs.len(), "identity")?;
            validate_batch_response_indexes(
                &successes,
                &problems,
                selector.resource_refs.len(),
                "identity",
            )?;
            Ok(BatchResourceResponse {
                successes,
                problems,
            })
        }
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote get_identities returned an unrecognized BatchResourceIdentitiesOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_batch_render_manifests_outcome(
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
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote render_manifests returned an unrecognized BatchResourceManifestsOutcome \
             variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_batch_delete_many_outcome(
    outcome: cynic_api::operations::delete::ResourceDeleteManyOutcome,
    selector: &ResourceBatchSelector,
) -> Result<BatchResourceResponse<domain::ResourceUID, ResourceLookupProblem>, BatchResourceError> {
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
        O::ResourceUnsupportedDescriptorProblem(problem) => {
            Err(unsupported_descriptor_problem_error(problem).into())
        }
        O::ResourceBadAccountProblem(problem) => Err(BatchResourceError::BadAccount(
            bad_account_problem_error(problem).map_err(BatchResourceError::Internal)?,
        )),
        O::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote delete_many returned an unrecognized ResourceDeleteManyOutcome variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn map_render_manifest_outcome(
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

#[cfg(test)]
mod tests {
    use kamu_resources::ResourceUID;

    use super::*;
    use crate::facade::graphql::cynic_api::fragments::{
        BatchResourceProblem,
        ResourceLookupProblem as CynicResourceLookupProblem,
        ResourceUIDNotFoundProblem,
    };

    fn uid_not_found_problem(index: i32) -> BatchResourceProblem {
        let uid: ResourceUID =
            serde_json::from_str("\"00000000-0000-0000-0000-000000000000\"").unwrap();
        BatchResourceProblem {
            request_index: index,
            problem: CynicResourceLookupProblem::ResourceUIDNotFoundProblem(
                ResourceUIDNotFoundProblem { uid },
            ),
        }
    }

    #[test]
    fn collect_batch_problems_negative_index_is_error() {
        let err = collect_batch_problems(vec![uid_not_found_problem(-1)], 1, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for negative problem index"
        );
    }

    #[test]
    fn collect_batch_problems_out_of_bounds_index_is_error() {
        let err = collect_batch_problems(vec![uid_not_found_problem(5)], 1, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for out-of-bounds problem index"
        );
    }

    fn collect<T>(
        request_len: usize,
        pairs: Vec<(i32, T)>,
    ) -> Result<Vec<BatchResourceSuccess<T>>, BatchResourceError> {
        collect_batch_successes(request_len, pairs, "test", Ok)
    }

    #[test]
    fn collect_batch_successes_happy_path() {
        let result = collect(3, vec![(0, "a"), (1, "b"), (2, "c")]).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].request_index, 0);
        assert_eq!(result[1].request_index, 1);
        assert_eq!(result[2].request_index, 2);
    }

    #[test]
    fn collect_batch_successes_negative_index_is_error() {
        let err = collect(3, vec![(-1_i32, "x")]).unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for negative index"
        );
    }

    #[test]
    fn collect_batch_successes_out_of_bounds_index_is_error() {
        let err = collect(2, vec![(5_i32, "x")]).unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for out-of-bounds index"
        );
    }

    #[test]
    fn collect_batch_successes_duplicate_index_is_error() {
        let err = collect(3, vec![(1_i32, "a"), (1_i32, "b")]).unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for duplicate index"
        );
    }

    fn make_success(index: usize) -> crate::BatchResourceSuccess<()> {
        crate::BatchResourceSuccess {
            request_index: index,
            item: (),
        }
    }

    fn make_domain_problem(index: usize) -> crate::BatchResourceProblem<()> {
        crate::BatchResourceProblem {
            request_index: index,
            error: (),
        }
    }

    #[test]
    fn validate_batch_response_indexes_happy_path() {
        let successes = vec![make_success(0), make_success(2)];
        let problems = vec![make_domain_problem(1)];
        assert!(validate_batch_response_indexes(&successes, &problems, 3, "test").is_ok());
    }

    #[test]
    fn validate_batch_response_indexes_overlap_is_error() {
        let successes = vec![make_success(0)];
        let problems = vec![make_domain_problem(0)];
        let err = validate_batch_response_indexes(&successes, &problems, 2, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error when success and problem share an index"
        );
    }

    #[test]
    fn validate_batch_response_indexes_duplicate_problem_index_is_error() {
        let successes: Vec<crate::BatchResourceSuccess<()>> = vec![];
        let problems = vec![make_domain_problem(0), make_domain_problem(0)];
        let err = validate_batch_response_indexes(&successes, &problems, 2, "test").unwrap_err();
        assert!(
            matches!(err, BatchResourceError::Internal(_)),
            "expected Internal error for duplicate problem index"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
