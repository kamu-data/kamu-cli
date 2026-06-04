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
use crate::{BatchResourceError, ResourceKindMismatchError, ResourceLookupProblem};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_lookup_problem(
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

pub(crate) fn map_uid_not_found(
    p: cynic_api::fragments::ResourceUIDNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::UIDNotFound(domain::ResourceUIDNotFoundError(p.uid))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_name_not_found(
    p: cynic_api::fragments::ResourceNameNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
        kind: p.kind,
        name: p.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_api_version_mismatch(
    p: cynic_api::fragments::ResourceApiVersionMismatchProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::ApiVersionMismatch(domain::ResourceAPIVersionMismatchError {
        expected_api_version: p.expected_api_version,
        actual_api_version: p.actual_api_version,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_kind_mismatch(
    p: cynic_api::fragments::ResourceKindMismatchProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::KindMismatch(ResourceKindMismatchError {
        uid: p.uid,
        expected_kind: p.expected_kind,
        actual_kind: p.actual_kind,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_selector_problem_result<E, FLookup, FUnsupported, FBadAccount>(
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
