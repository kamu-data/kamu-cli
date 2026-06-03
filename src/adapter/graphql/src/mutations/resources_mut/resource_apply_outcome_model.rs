// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::{Resource, ResourceBadAccountProblem, ResourceUnsupportedDescriptorProblem};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceApplyOutcome {
    Success(ResourceApplySuccess),
    Rejection(ResourceApplyRejection),
    ParseManifest(ResourceApplyParseManifestProblem),
    UnsupportedDescriptor(ResourceUnsupportedDescriptorProblem),
    BadAccount(ResourceBadAccountProblem),
    InvalidMetadata(ResourceInvalidMetadataProblem),
    InvalidSpec(ResourceInvalidSpecProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The manifest text could not be parsed (malformed JSON/YAML).
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyParseManifestProblem {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A metadata field value (e.g. name, label key) failed validation.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceInvalidMetadataProblem {
    pub code: ResourceMetadataValidationProblemCode,
    pub message: String,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceMetadataValidationProblemCode {
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

impl From<kamu_resources::ResourceMetadataValidationError> for ResourceInvalidMetadataProblem {
    fn from(value: kamu_resources::ResourceMetadataValidationError) -> Self {
        use kamu_resources::ResourceMetadataValidationError as E;
        let code = match &value {
            E::EmptyName => ResourceMetadataValidationProblemCode::EmptyName,
            E::NameTooLong { .. } => ResourceMetadataValidationProblemCode::NameTooLong,
            E::InvalidName { .. } => ResourceMetadataValidationProblemCode::InvalidName,
            E::DescriptionTooLong { .. } => {
                ResourceMetadataValidationProblemCode::DescriptionTooLong
            }
            E::TooManyLabels { .. } => ResourceMetadataValidationProblemCode::TooManyLabels,
            E::InvalidLabelKey { .. } => ResourceMetadataValidationProblemCode::InvalidLabelKey,
            E::DuplicateLabelKey { .. } => ResourceMetadataValidationProblemCode::DuplicateLabelKey,
            E::LabelValueTooLong { .. } => ResourceMetadataValidationProblemCode::LabelValueTooLong,
            E::TooManyAnnotations { .. } => {
                ResourceMetadataValidationProblemCode::TooManyAnnotations
            }
            E::InvalidAnnotationKey { .. } => {
                ResourceMetadataValidationProblemCode::InvalidAnnotationKey
            }
            E::DuplicateAnnotationKey { .. } => {
                ResourceMetadataValidationProblemCode::DuplicateAnnotationKey
            }
            E::AnnotationValueTooLong { .. } => {
                ResourceMetadataValidationProblemCode::AnnotationValueTooLong
            }
        };
        Self {
            code,
            message: value.to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The `spec` field failed domain validation.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceInvalidSpecProblem {
    pub kind: String,
    pub api_version: String,
    pub message: String,
}

impl From<kamu_resources::ResourceInvalidSpecError> for ResourceInvalidSpecProblem {
    fn from(value: kamu_resources::ResourceInvalidSpecError) -> Self {
        Self {
            kind: value.kind,
            api_version: value.api_version,
            message: value.message,
        }
    }
}

impl From<kamu_resources::ApplyManifestPlanningDecision> for ResourceApplyOutcome {
    fn from(value: kamu_resources::ApplyManifestPlanningDecision) -> Self {
        match value {
            kamu_resources::ApplyManifestPlanningDecision::Planned(plan) => {
                Self::Success(ResourceApplySuccess {
                    operation: plan.outcome.into(),
                    resource: plan.resource.into(),
                    changes: plan.changes.into_iter().map(Into::into).collect(),
                    warnings: plan.warnings.into_iter().map(Into::into).collect(),
                })
            }
            kamu_resources::ApplyManifestPlanningDecision::Rejected(rejection) => {
                Self::Rejection(rejection.into())
            }
        }
    }
}

impl From<kamu_resources::ApplyManifestApplicationDecision> for ResourceApplyOutcome {
    fn from(value: kamu_resources::ApplyManifestApplicationDecision) -> Self {
        match value {
            kamu_resources::ApplyManifestApplicationDecision::Applied(result) => {
                Self::Success(ResourceApplySuccess {
                    operation: result.outcome.into(),
                    resource: result.resource.into(),
                    changes: Vec::new(),
                    warnings: result.warnings.into_iter().map(Into::into).collect(),
                })
            }
            kamu_resources::ApplyManifestApplicationDecision::Rejected(rejection) => {
                Self::Rejection(rejection.into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplySuccess {
    pub operation: ResourceApplyOperation,
    pub resource: Resource,
    pub changes: Vec<ResourceApplyChange>,
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
pub struct ResourceApplyChange {
    pub kind: ResourceApplyChangeKind,
    pub path: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
}

impl From<kamu_resources::ApplyManifestChange> for ResourceApplyChange {
    fn from(value: kamu_resources::ApplyManifestChange) -> Self {
        Self {
            kind: value.kind.into(),
            path: value.path,
            before: value.before,
            after: value.after,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
#[graphql(remote = "kamu_resources::ApplyManifestChangeKind")]
pub enum ResourceApplyChangeKind {
    Generation,
    Metadata,
    Spec,
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
