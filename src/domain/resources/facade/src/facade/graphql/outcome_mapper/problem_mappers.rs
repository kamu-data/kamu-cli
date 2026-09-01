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
    ResourceAccountResolutionError,
    ResourceAccountResolutionProblemCode,
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
        P::ResourceAnyTypeNameNotFoundProblem(p) => Ok(map_any_type_name_not_found(p)),
        P::ResourceAmbiguousTypeProblem(p) => Ok(map_ambiguous_type(p)),
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

/// Rebuilds the account-resolution problem the remote facade reported.
///
/// Infallible: the wire type carries only a stable code and a rendered message,
/// so there is nothing to validate — which is the point of the `{code,
/// message}` shape shared with the header and label-filter problems.
pub(crate) fn account_resolution_problem_error(
    problem: cynic_api::fragments::ResourceAccountResolutionProblem,
) -> ResourceAccountResolutionError {
    use cynic_api::fragments::ResourceAccountResolutionProblemCode as C;

    let code = match problem.code {
        C::EmptySelector => ResourceAccountResolutionProblemCode::EmptySelector,
        C::AccountNotFoundById => ResourceAccountResolutionProblemCode::AccountNotFoundById,
        C::AccountNotFoundByName => ResourceAccountResolutionProblemCode::AccountNotFoundByName,
        C::SelectorMismatch => ResourceAccountResolutionProblemCode::SelectorMismatch,
    };

    ResourceAccountResolutionError {
        code,
        message: problem.message,
    }
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

pub(crate) fn map_any_type_name_not_found(
    p: cynic_api::fragments::ResourceAnyTypeNameNotFoundProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::AnyTypeNameNotFound(domain::ResourceAnyTypeNameNotFoundError {
        name: p.name,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_ambiguous_type(
    p: cynic_api::fragments::ResourceAmbiguousTypeProblem,
) -> ResourceLookupProblem {
    ResourceLookupProblem::AmbiguousType(domain::ResourceAmbiguousTypeError {
        name: p.name,
        type_names: p.type_names,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
