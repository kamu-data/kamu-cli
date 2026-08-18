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
    ResourceAmbiguousTypeError,
    ResourceAnyTypeNameNotFoundError,
    ResourceExtensionResolutionError,
    ResourceHeadersValidationError,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceInvalidSpecError,
    ResourceLabelFilterExprParseError,
    ResourceName,
    ResourceNameNotFoundError,
    TypeRef,
    TypeUri,
    UnsupportedResourceDescriptorError,
    UnsupportedResourceSelectorError,
};
use thiserror::Error;

use crate::UnsupportedSelectorFieldError;

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

use crate::{ResolveManifestAccountError, ResourceAccountAccessError, UnrepresentableScopeError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ResourceAccountResolutionProblemCode {
    EmptySelector,
    AccountNotFoundById,
    AccountNotFoundByName,
    SelectorMismatch,
}

/// The account selector supplied for an operation could not be resolved to a
/// concrete account. This is an input-validation problem, not an authorization
/// outcome — a caller denied access to another account's resources is reported
/// through [`odf::AccessError`], never through this type.
///
/// Carries a stable `code` plus a rendered `message` rather than the resolver's
/// own field-carrying variants: the code is what lets the remote facade rebuild
/// the same error the local facade raises, which is what makes the
/// `contract_test!` local/remote pairs assert identical behavior on both
/// transports. Matches [`ResourceInvalidHeadersError`] and
/// [`ResourceInvalidLabelFilterError`] in shape.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct ResourceAccountResolutionError {
    pub code: ResourceAccountResolutionProblemCode,
    pub message: String,
}

/// Routes a [`ResolveManifestAccountError`] into one of the facade error enums.
///
/// The user-facing/infrastructure split is already structural — see
/// [`ResolveManifestAccountError::Resolution`] — so this just forwards each
/// case to its counterpart, keeping authorization denials out of
/// `AccountResolution`.
///
/// One macro rather than four hand-written impls, so the routing cannot drift
/// between the operations that share it.
macro_rules! impl_from_resolve_manifest_account_error {
    ($($error_type:ty),+ $(,)?) => {
        $(
            impl From<ResolveManifestAccountError> for $error_type {
                fn from(err: ResolveManifestAccountError) -> Self {
                    use ResolveManifestAccountError as E;
                    match err {
                        E::Resolution(problem) => Self::AccountResolution(problem),
                        E::AccountAccess(err) => Self::AccountAccess(err),
                        E::Internal(err) => Self::Internal(err),
                    }
                }
            }
        )+
    };
}

impl_from_resolve_manifest_account_error!(
    BatchResourceError,
    ListResourcesError,
    ResourcesSummaryError,
    ApplyManifestError,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListSupportedResourceTypesError {
    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A single ref failed to resolve.
///
/// Not produced by [`ResourceFacade`] itself: the ref-keyed operations are
/// batch-only and report per-item failures as [`ResourceLookupProblem`] inside
/// a `BatchResourceResponse`. This is the CLI-side currency for "one ref, one
/// outcome", built by callers that resolved a single name out of a batch — so
/// the only failure it can carry is that ref's lookup problem.
///
/// [`ResourceFacade`]: crate::ResourceFacade
#[derive(Debug, Error)]
pub enum GetResourceError {
    #[error(transparent)]
    LookupProblem(#[from] ResourceLookupProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourceLookupProblem {
    #[error(transparent)]
    IDNotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    NameNotFound(#[from] ResourceNameNotFoundError),

    /// A type-less ref whose name matched nothing in any registered type. Its
    /// own variant rather than a `NameNotFound`, which would have to name a
    /// single type that was never searched in isolation.
    #[error(transparent)]
    AnyTypeNameNotFound(#[from] ResourceAnyTypeNameNotFoundError),

    /// A type-less ref whose name matched in several types. A ref names exactly
    /// one resource, so this is an addressing failure, not a multi-match.
    #[error(transparent)]
    AmbiguousType(#[from] ResourceAmbiguousTypeError),

    #[error(transparent)]
    SchemaMismatch(#[from] ResourceSchemaMismatchError),

    /// A reference supplied both an id and a name, and they name different
    /// resources. Its own variant rather than a `NameNotFound`: the named
    /// resource may well exist — it simply is not the one the id addresses.
    #[error(transparent)]
    NameMismatch(#[from] ResourceNameMismatchError),

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
    AccountResolution(ResourceAccountResolutionError),

    /// The caller is not authenticated, or is denied access to the target
    /// account. Its own variant rather than an `AccountResolution`: an
    /// authorization denial must not be reported to clients as bad input.
    #[error(transparent)]
    AccountAccess(ResourceAccountAccessError),

    #[error(transparent)]
    InvalidLabelFilter(#[from] ResourceInvalidLabelFilterError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListResourcesError {
    #[error(transparent)]
    UnsupportedSelector(#[from] UnsupportedResourceSelectorError),

    #[error(transparent)]
    AccountResolution(ResourceAccountResolutionError),

    /// The caller is not authenticated, or is denied access to the target
    /// account. Its own variant rather than an `AccountResolution`: an
    /// authorization denial must not be reported to clients as bad input.
    #[error(transparent)]
    AccountAccess(ResourceAccountAccessError),

    #[error(transparent)]
    InvalidLabelFilter(#[from] ResourceInvalidLabelFilterError),

    #[error(transparent)]
    UnrepresentableScope(#[from] UnrepresentableScopeError),

    /// A selector set an ODF field the facade cannot honour. Rejected rather
    /// than ignored: a dropped narrowing constraint widens the result set
    /// without the caller being able to tell.
    #[error(transparent)]
    UnsupportedSelectorField(#[from] UnsupportedSelectorFieldError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResourcesSummaryError {
    #[error(transparent)]
    AccountResolution(ResourceAccountResolutionError),

    /// The caller is not authenticated, or is denied access to the target
    /// account. Its own variant rather than an `AccountResolution`: an
    /// authorization denial must not be reported to clients as bad input.
    #[error(transparent)]
    AccountAccess(ResourceAccountAccessError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    AccountResolution(ResourceAccountResolutionError),

    /// The caller is not authenticated, or is denied access to the target
    /// account. Its own variant rather than an `AccountResolution`: an
    /// authorization denial must not be reported to clients as bad input.
    #[error(transparent)]
    AccountAccess(ResourceAccountAccessError),

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

#[derive(Debug, Error)]
#[error("Resource id {id} is named '{actual_name}', not '{expected_name}'")]
pub struct ResourceNameMismatchError {
    pub id: ResourceID,
    pub expected_name: ResourceName,
    pub actual_name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
