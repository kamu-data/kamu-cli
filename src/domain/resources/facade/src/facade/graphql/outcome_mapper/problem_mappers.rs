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
    ResourceLookupProblem,
    ResourceNameMismatchError,
    ResourceSchemaMismatchError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_batch_lookup_problem(
    problem: cynic_api::fragments::ResourceLookupProblem,
) -> Result<ResourceLookupProblem, BatchResourceError> {
    use cynic_api::fragments::ResourceLookupProblem as P;
    match problem {
        P::ResourceIDNotFoundProblem(p) => Ok(map_id_not_found(p)),
        P::ResourceNameNotFoundProblem(p) => Ok(map_name_not_found(p)),
        P::ResourceSchemaMismatchProblem(p) => Ok(map_schema_mismatch(p)),
        P::ResourceNameMismatchProblem(p) => Ok(map_name_mismatch(p)),
        P::Unknown => Err(BatchResourceError::Internal(InternalError::new(
            "Remote batch problem contains unrecognized ResourceLookupProblem variant",
        ))),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn unsupported_descriptor_problem_error(
    problem: cynic_api::fragments::ResourceUnsupportedDescriptorProblem,
) -> domain::UnsupportedResourceDescriptorError {
    domain::UnsupportedResourceDescriptorError::NotFound {
        schema: problem.schema,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn unsupported_selector_problem_error(
    problem: cynic_api::fragments::ResourceUnsupportedSelectorProblem,
) -> domain::UnsupportedResourceSelectorError {
    domain::UnsupportedResourceSelectorError::NotFound {
        raw_selector: problem.selector,
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
        C::SelectorMismatch => crate::ResolveManifestAccountError::SelectorMismatch {
            did: problem.account_id.ok_or_else(|| {
                InternalError::new("Malformed remote bad account problem: missing account_id")
            })?,
            actual_name: account_name_from_problem(problem.actual_name, "actual_name")?,
            expected_resource_id: problem.expected_resource_id,
            expected_did: problem.expected_did,
            expected_name: problem
                .expected_name
                .map(|name| odf::AccountName::new_unchecked(&name.0)),
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

pub(crate) fn map_id_not_found(
    p: cynic_api::fragments::ResourceIDNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::IDNotFound(domain::ResourceIDNotFoundError(p.id))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_name_not_found(
    p: cynic_api::fragments::ResourceNameNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::NameNotFound(domain::ResourceNameNotFoundError {
        type_name: p.type_name,
        name: p.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_schema_mismatch(
    p: cynic_api::fragments::ResourceSchemaMismatchProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::SchemaMismatch(ResourceSchemaMismatchError {
        id: p.id,
        expected_schema: p.expected_schema,
        actual_schema: p.actual_schema,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_name_mismatch(
    p: cynic_api::fragments::ResourceNameMismatchProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::NameMismatch(ResourceNameMismatchError {
        id: p.id,
        expected_name: p.expected_name,
        actual_name: p.actual_name,
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
    FUnsupported: FnOnce(domain::UnsupportedResourceSelectorError) -> E,
    FBadAccount: FnOnce(crate::ResolveManifestAccountError) -> E,
{
    use cynic_api::fragments::ResourceSelectorProblem as P;
    match result.problem {
        P::ResourceIDNotFoundProblem(p) => Ok(map_lookup(map_id_not_found(p))),
        P::ResourceNameNotFoundProblem(p) => Ok(map_lookup(map_name_not_found(p))),
        P::ResourceSchemaMismatchProblem(p) => Ok(map_lookup(map_schema_mismatch(p))),
        P::ResourceNameMismatchProblem(p) => Ok(map_lookup(map_name_mismatch(p))),
        P::ResourceUnsupportedSelectorProblem(p) => {
            Ok(map_unsupported(unsupported_selector_problem_error(p)))
        }
        P::ResourceBadAccountProblem(p) => Ok(map_bad_account(bad_account_problem_error(p)?)),
        P::Unknown => Err(InternalError::new(
            "Remote returned an unrecognized ResourceSelectorProblem variant",
        )),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
