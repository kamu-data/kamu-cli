// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::Resource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceApplyOutcome {
    Success(ResourceApplySuccess),
    Rejection(ResourceApplyRejection),
    Error(ResourceApplyError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Structured error returned when the manifest cannot be applied due to a
/// client-visible problem (malformed input, invalid spec, unknown kind, etc.).
/// Unlike `ResourceApplyRejection` (which represents a domain-level policy
/// decision), this represents input validation / parsing failures.
#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyError {
    pub code: ResourceApplyErrorCode,
    pub message: String,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceApplyErrorCode {
    /// The manifest text could not be parsed (malformed JSON/YAML).
    ParseManifest,
    /// The resource kind / apiVersion is not supported.
    UnsupportedDescriptor,
    /// The `metadata.account` field refers to an account that does not exist.
    BadAccount,
    /// A metadata field value (e.g. name) is invalid.
    InvalidMetadata,
    /// The `spec` failed domain validation (e.g. empty variables map).
    InvalidSpec,
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
