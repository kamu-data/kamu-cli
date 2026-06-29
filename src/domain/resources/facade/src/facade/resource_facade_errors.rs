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
    ResourceHeadersValidationError,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceInvalidSpecError,
    ResourceNameNotFoundError,
    UnsupportedResourceDescriptorError,
};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceHeadersValidationProblemCode {
    EmptyName,
    NameTooLong,
    InvalidName,
    DescriptionTooLong,
    TooManyLabels,
    InvalidLabelKey,
    DuplicateLabelKey,
    LabelValueTooLong,
    TooManyAnnotations,
    InvalidAnnotationKey,
    DuplicateAnnotationKey,
    AnnotationValueTooLong,
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
            E::DescriptionTooLong { .. } => C::DescriptionTooLong,
            E::TooManyLabels { .. } => C::TooManyLabels,
            E::InvalidLabelKey { .. } => C::InvalidLabelKey,
            E::DuplicateLabelKey { .. } => C::DuplicateLabelKey,
            E::LabelValueTooLong { .. } => C::LabelValueTooLong,
            E::TooManyAnnotations { .. } => C::TooManyAnnotations,
            E::InvalidAnnotationKey { .. } => C::InvalidAnnotationKey,
            E::DuplicateAnnotationKey { .. } => C::DuplicateAnnotationKey,
            E::AnnotationValueTooLong { .. } => C::AnnotationValueTooLong,
        };
        Self {
            code,
            message: err.to_string(),
        }
    }
}

use crate::ResolveManifestAccountError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListSupportedResourceKindsError {
    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetResourceError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum BatchResourceError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RenderResourceManifestError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

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
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    InvalidSearchQuery(#[from] InvalidResourceSearchQueryError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Resource identity search requires exact names or a name pattern")]
pub struct InvalidResourceSearchQueryError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ListAllResourcesError {
    #[error(transparent)]
    BadAccount(#[from] ResolveManifestAccountError),

    #[error(transparent)]
    RemoteRequest(#[from] GraphqlHttpRequestError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

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
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

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
            BatchResourceError::UnsupportedDescriptor(err) => Self::UnsupportedDescriptor(err),
            BatchResourceError::BadAccount(err) => Self::BadAccount(err),
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
    UIDNotFound(#[from] ResourceIDNotFoundError),

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

impl From<ApplyResourceCrudDispatcherError> for ApplyManifestError {
    fn from(err: ApplyResourceCrudDispatcherError) -> Self {
        use ApplyResourceCrudDispatcherError as E;
        match err {
            E::Internal(err) => Self::Internal(err),
            E::NotFound(err) => Self::UIDNotFound(err),
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
    pub expected_schema: String,
    pub actual_schema: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
