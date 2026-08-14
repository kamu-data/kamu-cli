// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ConcurrentModificationError;
use graphql_http::GraphqlHttpRequestError;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_resources::{
    ApplyResourceCrudDispatcherError,
    DeleteResourcesCrudDispatcherError,
    GetResourceCrudDispatcherError,
    ResourceExtensionResolutionError,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceInvalidSpecError,
    ResourceLabelFilterExprParseError,
    ResourceNameNotFoundError,
    TypeRef,
    TypeUri,
    UnsupportedResourceDescriptorError,
    UnsupportedResourceSelectorError,
};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResourceHeadersValidationProblemCode {
    EmptyName,
    NameTooLong,
    InvalidName,
    TooManyLabels,
    DuplicateLabelKey,
    TooManyAnnotations,
    DuplicateAnnotationKey,
    ResourceExtensionSchema,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ResourceInvalidHeadersError {
    pub code: ResourceHeadersValidationProblemCode,
    pub message: String,
}

impl From<ResourceHeadersValidationError> for ResourceInvalidHeadersError {
    fn from(err: ResourceHeadersValidationError) -> Self {
        use ResourceHeadersValidationError as E;
        use ResourceHeadersValidationProblemCode as C;
        let code = match &err {
            E::EmptyName => C::EmptyName,
            E::NameTooLong { .. } => C::NameTooLong,
            E::InvalidName { .. } => C::InvalidName,
            E::TooManyLabels { .. } => C::TooManyLabels,
            E::DuplicateLabelKey { .. } => C::DuplicateLabelKey,
            E::TooManyAnnotations { .. } => C::TooManyAnnotations,
            E::DuplicateAnnotationKey { .. } => C::DuplicateAnnotationKey,
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}

impl From<ResourceExtensionResolutionError> for ResourceInvalidHeadersError {
    fn from(err: ResourceExtensionResolutionError) -> Self {
        Self {
            code: ResourceHeadersValidationProblemCode::ResourceExtensionSchema,
            message: err.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResourceLabelFilterProblemCode {
    InvalidKey,
    ResourceExtensionSchema,
    NonStringValue,
    DuplicateAfterCanonicalization,
    UnsupportedExpression,
}

#[derive(Debug, Error)]
#[error("{message}")]
pub struct ResourceInvalidLabelFilterError {
    pub code: ResourceLabelFilterProblemCode,
    pub message: String,
}

impl ResourceInvalidLabelFilterError {
    pub fn invalid_key(key: &str, reason: impl std::fmt::Display) -> Self {
        Self {
            code: ResourceLabelFilterProblemCode::InvalidKey,
            message: format!("invalid label filter key '{key}': {reason}"),
        }
    }

    pub fn non_string_value(key: &TypeRef) -> Self {
        Self {
            code: ResourceLabelFilterProblemCode::NonStringValue,
            message: format!("non-string label filter values are not supported yet (key '{key}')"),
        }
    }

    pub fn unsupported_expression(reason: impl std::fmt::Display) -> Self {
        Self {
            code: ResourceLabelFilterProblemCode::UnsupportedExpression,
            message: format!("label filter expression is not supported yet: {reason}"),
        }
    }
}

impl From<ResourceExtensionResolutionError> for ResourceInvalidLabelFilterError {
    fn from(err: ResourceExtensionResolutionError) -> Self {
        let code = match &err {
            ResourceExtensionResolutionError::DuplicateAfterCanonicalization { .. } => {
                ResourceLabelFilterProblemCode::DuplicateAfterCanonicalization
            }
            _ => ResourceLabelFilterProblemCode::ResourceExtensionSchema,
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}

impl From<ResourceLabelFilterExprParseError> for ResourceInvalidLabelFilterError {
    fn from(err: ResourceLabelFilterExprParseError) -> Self {
        Self::unsupported_expression(err)
    }
}

use crate::{ResolveManifestAccountError, UnrepresentableScopeError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListSupportedResourceTypesError {
    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetResourceError {
    #[error(transparent)]
    UnsupportedSelector(#[from] UnsupportedResourceSelectorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    LookupProblem(#[from] ResourceLookupProblem),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetResourceCrudDispatcherError> for GetResourceError {
    fn from(err: GetResourceCrudDispatcherError) -> Self {
        use GetResourceCrudDispatcherError as E;
        match err {
            E::NotFound(err) => Self::LookupProblem(ResourceLookupProblem::IDNotFound(err)),
            E::TypeMismatch(err) => Self::LookupProblem(ResourceLookupProblem::SchemaMismatch(
                ResourceSchemaMismatchError {
                    id: err.id,
                    expected_schema: err.expected_schema,
                    actual_schema: err.actual_schema,
                },
            )),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourceLookupProblem {
    #[error(transparent)]
    IDNotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    NameNotFound(#[from] ResourceNameNotFoundError),

    #[error(transparent)]
    SchemaMismatch(#[from] ResourceSchemaMismatchError),

    /// A reference naming neither an id nor a name. Its own variant rather than
    /// a `NameNotFound`, which would wrongly claim a lookup was attempted.
    #[error("Resource reference must specify at least one of `id` or `name`")]
    EmptyRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum BatchResourceError {
    #[error(transparent)]
    UnsupportedSelector(#[from] UnsupportedResourceSelectorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    InvalidLabelFilter(#[from] ResourceInvalidLabelFilterError),

    #[error(transparent)]
    NonUniformBatch(#[from] NonUniformBatchError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A batch whose references disagree on the account or type they target.
///
/// Temporary: the API shape already allows both to vary per reference, but
/// account resolution is still singular below the facade. Lifting this is the
/// remaining half of the acceptance request.
#[derive(Debug, Error)]
pub enum NonUniformBatchError {
    #[error(
        "Batch operations cannot yet span resource types: got both `{first}` and `{other}`. Issue \
         one call per type."
    )]
    MixedTypes { first: String, other: String },

    #[error(
        "Batch operations cannot yet span accounts. Issue one call per account, and spell the \
         account the same way in every reference."
    )]
    MixedAccounts,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RenderResourceManifestError {
    #[error(transparent)]
    UnsupportedSelector(#[from] UnsupportedResourceSelectorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    LookupProblem(#[from] ResourceLookupProblem),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetResourceCrudDispatcherError> for RenderResourceManifestError {
    fn from(err: GetResourceCrudDispatcherError) -> Self {
        use GetResourceCrudDispatcherError as E;
        match err {
            E::NotFound(err) => Self::LookupProblem(ResourceLookupProblem::IDNotFound(err)),
            E::TypeMismatch(err) => Self::LookupProblem(ResourceLookupProblem::SchemaMismatch(
                ResourceSchemaMismatchError {
                    id: err.id,
                    expected_schema: err.expected_schema,
                    actual_schema: err.actual_schema,
                },
            )),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListResourcesError {
    #[error(transparent)]
    UnsupportedSelector(#[from] UnsupportedResourceSelectorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    InvalidLabelFilter(#[from] ResourceInvalidLabelFilterError),

    #[error(transparent)]
    UnrepresentableScope(#[from] UnrepresentableScopeError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Listing every type is just a listing with an all-types scope, so it carries
/// the same failures.
pub type ListAllResourcesError = ListResourcesError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourcesSummaryError {
    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteResourceError {
    #[error(transparent)]
    UnsupportedSelector(#[from] UnsupportedResourceSelectorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    LookupProblem(#[from] ResourceLookupProblem),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<DeleteResourcesCrudDispatcherError> for DeleteResourceError {
    fn from(err: DeleteResourcesCrudDispatcherError) -> Self {
        use DeleteResourcesCrudDispatcherError as E;
        match err {
            E::Access(err) => Self::Internal(err.int_err()),
            E::ConcurrentModification(err) => Self::Internal(err.int_err()),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<BatchResourceError> for DeleteResourceError {
    fn from(err: BatchResourceError) -> Self {
        match err {
            BatchResourceError::UnsupportedSelector(err) => Self::UnsupportedSelector(err),
            BatchResourceError::BadAccount(err) => Self::BadAccount(err),
            // `delete` resolves a single pre-selected ref, so it never carries
            // a label filter for this to surface from.
            BatchResourceError::InvalidLabelFilter(err) => Self::Internal(err.int_err()),
            // Likewise unreachable: a one-element batch is uniform by
            // construction.
            BatchResourceError::NonUniformBatch(err) => Self::Internal(err.int_err()),
            BatchResourceError::RemoteRequest(err) => Self::RemoteRequest(err),
            BatchResourceError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<DeleteResourcesCrudDispatcherError> for BatchResourceError {
    fn from(err: DeleteResourcesCrudDispatcherError) -> Self {
        use DeleteResourcesCrudDispatcherError as E;
        match err {
            E::Access(err) => Self::Internal(err.int_err()),
            E::ConcurrentModification(err) => Self::Internal(err.int_err()),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ApplyManifestError {
    #[error(transparent)]
    ParseManifest(#[from] ParseResourceManifestError),

    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    InvalidHeaders(#[from] ResourceInvalidHeadersError),

    #[error(transparent)]
    InvalidSpec(#[from] ResourceInvalidSpecError),

    #[error(transparent)]
    IDNotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] kamu_resources::ResourceTypeMismatchError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<ResourceHeadersValidationError> for ApplyManifestError {
    fn from(err: ResourceHeadersValidationError) -> Self {
        Self::InvalidHeaders(err.into())
    }
}

impl From<ResourceExtensionResolutionError> for ApplyManifestError {
    fn from(err: ResourceExtensionResolutionError) -> Self {
        Self::InvalidHeaders(err.into())
    }
}

impl From<ApplyResourceCrudDispatcherError> for ApplyManifestError {
    fn from(err: ApplyResourceCrudDispatcherError) -> Self {
        use ApplyResourceCrudDispatcherError as E;
        match err {
            E::Internal(err) => Self::Internal(err),
            E::NotFound(err) => Self::IDNotFound(err),
            E::TypeMismatch(err) => Self::TypeMismatch(err),
            E::ConcurrentModification(err) => Self::ConcurrentModification(err),
            E::InvalidSpec { schema, message } => {
                Self::InvalidSpec(ResourceInvalidSpecError { schema, message })
            }
        }
    }
}

#[derive(Debug, Error)]
#[error("Failed to parse resource manifest: {message}")]
pub struct ParseResourceManifestError {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Resource id {id} refers to schema '{actual_schema}', expected '{expected_schema}'")]
pub struct ResourceSchemaMismatchError {
    pub id: ResourceID,
    pub expected_schema: TypeUri,
    pub actual_schema: TypeUri,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
