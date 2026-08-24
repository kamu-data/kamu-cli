// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::{
    Resource,
    ResourceAccountResolutionProblem,
    ResourceUnsupportedDescriptorProblem,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceApplyOutcome {
    Success(ResourceApplySuccess),
    Rejection(ResourceApplyRejection),
    ParseManifest(ResourceApplyParseManifestProblem),
    UnsupportedDescriptor(ResourceUnsupportedDescriptorProblem),
    AccountResolution(ResourceAccountResolutionProblem),
    InvalidHeader(ResourceInvalidHeaderProblem),
    InvalidSpec(ResourceInvalidSpecProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The manifest text could not be parsed (malformed JSON/YAML).
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyParseManifestProblem {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A header field value (e.g. name, label key) failed validation.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceInvalidHeaderProblem {
    pub code: ResourceHeaderValidationProblemCode,
    pub message: String,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceHeaderValidationProblemCode {
    EmptyName,
    NameTooLong,
    InvalidName,
    TooManyLabels,
    DuplicateLabelKey,
    TooManyAnnotations,
    DuplicateAnnotationKey,
    ResourceExtensionSchema,
}

impl From<kamu_resources::ResourceHeadersValidationError> for ResourceInvalidHeaderProblem {
    fn from(value: kamu_resources::ResourceHeadersValidationError) -> Self {
        kamu_resources_facade::ResourceInvalidHeadersError::from(value).into()
    }
}

impl From<kamu_resources_facade::ResourceInvalidHeadersError> for ResourceInvalidHeaderProblem {
    fn from(value: kamu_resources_facade::ResourceInvalidHeadersError) -> Self {
        use kamu_resources_facade::ResourceHeadersValidationProblemCode as C;
        let code = match value.code {
            C::EmptyName => ResourceHeaderValidationProblemCode::EmptyName,
            C::NameTooLong => ResourceHeaderValidationProblemCode::NameTooLong,
            C::InvalidName => ResourceHeaderValidationProblemCode::InvalidName,
            C::TooManyLabels => ResourceHeaderValidationProblemCode::TooManyLabels,
            C::DuplicateLabelKey => ResourceHeaderValidationProblemCode::DuplicateLabelKey,
            C::TooManyAnnotations => ResourceHeaderValidationProblemCode::TooManyAnnotations,
            C::DuplicateAnnotationKey => {
                ResourceHeaderValidationProblemCode::DuplicateAnnotationKey
            }
            C::ResourceExtensionSchema => {
                ResourceHeaderValidationProblemCode::ResourceExtensionSchema
            }
        };
        Self {
            code,
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The `spec` field failed domain validation.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceInvalidSpecProblem {
    pub schema: TypeUri<'static>,
    pub message: String,
}

impl From<kamu_resources::ResourceInvalidSpecError> for ResourceInvalidSpecProblem {
    fn from(value: kamu_resources::ResourceInvalidSpecError) -> Self {
        Self {
            schema: value.schema.into(),
            message: value.message,
        }
    }
}

// Fallible rather than `From`: canonicalizing the documents can fail, and a
// failure there is an internal error, not an apply outcome to report.
impl TryFrom<kamu_resources::ApplyManifestPlanningDecision> for ResourceApplyOutcome {
    type Error = InternalError;

    fn try_from(value: kamu_resources::ApplyManifestPlanningDecision) -> Result<Self, Self::Error> {
        Ok(match value {
            kamu_resources::ApplyManifestPlanningDecision::Planned(plan) => {
                let documents = plan.documents()?;

                Self::Success(ResourceApplySuccess {
                    operation: plan.outcome.into(),
                    resource: plan.resource.into(),
                    before: documents.before,
                    after: documents.after,
                    warnings: plan.warnings.into_iter().map(Into::into).collect(),
                })
            }
            kamu_resources::ApplyManifestPlanningDecision::Rejected(rejection) => {
                Self::Rejection(rejection.into())
            }
        })
    }
}

impl TryFrom<kamu_resources::ApplyManifestApplicationDecision> for ResourceApplyOutcome {
    type Error = InternalError;

    fn try_from(
        value: kamu_resources::ApplyManifestApplicationDecision,
    ) -> Result<Self, Self::Error> {
        Ok(match value {
            kamu_resources::ApplyManifestApplicationDecision::Applied(result) => {
                let documents = result.documents()?;

                Self::Success(ResourceApplySuccess {
                    operation: result.outcome.into(),
                    resource: result.resource.into(),
                    before: documents.before,
                    after: documents.after,
                    warnings: result.warnings.into_iter().map(Into::into).collect(),
                })
            }
            kamu_resources::ApplyManifestApplicationDecision::Rejected(rejection) => {
                Self::Rejection(rejection.into())
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An accepted apply, reported as the canonical manifest on each side rather
/// than a pre-computed change list: clients decide how to diff and display it.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplySuccess {
    pub operation: ResourceApplyOperation,
    pub resource: Resource,

    /// Canonical manifest before the apply. Null **iff** the resource is being
    /// created.
    pub before: Option<serde_json::Value>,

    /// Canonical manifest the apply produced (or would produce). Always present
    /// on an accepted apply.
    pub after: serde_json::Value,

    pub warnings: Vec<ResourceApplyWarning>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources::ApplyResourceOutcome")]
pub enum ResourceApplyOperation {
    Created,
    Updated,
    Untouched,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyRejection {
    pub category: ResourceApplyRejectionCategory,
    pub message: String,
}

impl From<kamu_resources::ApplyManifestRejection> for ResourceApplyRejection {
    fn from(value: kamu_resources::ApplyManifestRejection) -> Self {
        Self {
            category: value.category.into(),
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyWarning {
    pub code: String,
    pub path: Option<String>,
    pub message: String,
}

impl From<kamu_resources::ResourceWarning> for ResourceApplyWarning {
    fn from(value: kamu_resources::ResourceWarning) -> Self {
        Self {
            code: value.code,
            path: value.path,
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources::ApplyResourceRejectionCategory")]
pub enum ResourceApplyRejectionCategory {
    ImmutableFieldChanged,
    BusinessValidationFailed,
    ReferencedObjectMissing,
    LifecycleRuleConflict,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
