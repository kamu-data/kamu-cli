// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cynic::MutationBuilder;
use internal_error::InternalError;
use kamu_resources as domain;

use crate::ApplyManifestRequest;
use crate::facade::graphql::cynic_api::fragments::{Resource, ResourceManifestFormat};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Mutation", variables = "ApplyManifestVariables")]
pub(crate) struct ApplyManifestMutation {
    pub resources: ResourcesMut,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourcesMut", variables = "ApplyManifestVariables")]
pub(crate) struct ResourcesMut {
    #[arguments(manifest: $manifest, format: $format, dryRun: $dry_run)]
    pub apply_manifest: ResourceApplyOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
#[cynic(graphql_type = "ResourceApplyOutcome")]
pub(crate) enum ResourceApplyOutcome {
    ResourceApplySuccess(ResourceApplySuccess),
    ResourceApplyRejection(ResourceApplyRejection),
    #[cynic(fallback)]
    Unknown,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplySuccess {
    pub operation: ResourceApplyOperation,
    pub resource: Resource,
    pub changes: Vec<ResourceApplyChange>,
    pub warnings: Vec<ResourceApplyWarning>,
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceApplyOperation {
    Created,
    Updated,
    Untouched,
}

impl From<ResourceApplyOperation> for domain::ApplyResourceOutcome {
    fn from(value: ResourceApplyOperation) -> Self {
        match value {
            ResourceApplyOperation::Created => Self::Created,
            ResourceApplyOperation::Updated => Self::Updated,
            ResourceApplyOperation::Untouched => Self::Untouched,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplyChange {
    pub kind: ResourceApplyChangeKind,
    pub path: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
}

impl From<ResourceApplyChange> for domain::ApplyManifestChange {
    fn from(value: ResourceApplyChange) -> Self {
        Self {
            kind: value.kind.into(),
            path: value.path,
            before: value.before,
            after: value.after,
        }
    }
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceApplyChangeKind {
    Generation,
    Metadata,
    Spec,
}

impl From<ResourceApplyChangeKind> for domain::ApplyManifestChangeKind {
    fn from(value: ResourceApplyChangeKind) -> Self {
        match value {
            ResourceApplyChangeKind::Generation => Self::Generation,
            ResourceApplyChangeKind::Metadata => Self::Metadata,
            ResourceApplyChangeKind::Spec => Self::Spec,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplyWarning {
    pub code: String,
    pub path: Option<String>,
    pub message: String,
}

impl From<ResourceApplyWarning> for domain::ResourceWarning {
    fn from(value: ResourceApplyWarning) -> Self {
        Self {
            code: value.code,
            path: value.path,
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplyRejection {
    pub category: ResourceApplyRejectionCategory,
    pub message: String,
}

impl From<ResourceApplyRejection> for domain::ApplyManifestRejection {
    fn from(value: ResourceApplyRejection) -> Self {
        Self {
            category: value.category.into(),
            message: value.message,
        }
    }
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceApplyRejectionCategory {
    ImmutableFieldChanged,
    BusinessValidationFailed,
    ReferencedObjectMissing,
    LifecycleRuleConflict,
}

impl From<ResourceApplyRejectionCategory> for domain::ApplyResourceRejectionCategory {
    fn from(value: ResourceApplyRejectionCategory) -> Self {
        match value {
            ResourceApplyRejectionCategory::ImmutableFieldChanged => Self::ImmutableFieldChanged,
            ResourceApplyRejectionCategory::BusinessValidationFailed => {
                Self::BusinessValidationFailed
            }
            ResourceApplyRejectionCategory::ReferencedObjectMissing => {
                Self::ReferencedObjectMissing
            }
            ResourceApplyRejectionCategory::LifecycleRuleConflict => Self::LifecycleRuleConflict,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceApplyOutcome {
    pub(crate) fn into_planning_decision(
        self,
    ) -> Result<domain::ApplyManifestPlanningDecision, InternalError> {
        Ok(match self {
            Self::ResourceApplySuccess(success) => {
                let resource = success.resource.try_into()?;
                let outcome = success.operation.into();
                let changes = success.changes.into_iter().map(Into::into).collect();
                let warnings = success.warnings.into_iter().map(Into::into).collect();

                domain::ApplyManifestPlanningDecision::Planned(domain::ApplyManifestPlan {
                    resource,
                    outcome,
                    // TODO: Expose these plan-only fields in GraphQL when the CLI needs them.
                    reconciliation_required: false,
                    executable: true,
                    changes,
                    warnings,
                })
            }
            Self::ResourceApplyRejection(rejection) => {
                domain::ApplyManifestPlanningDecision::Rejected(rejection.into())
            }
            Self::Unknown => {
                return Err(InternalError::new(
                    "Remote apply returned an unrecognized ResourceApplyOutcome variant",
                ));
            }
        })
    }

    pub(crate) fn into_application_decision(
        self,
    ) -> Result<domain::ApplyManifestApplicationDecision, InternalError> {
        Ok(match self {
            Self::ResourceApplySuccess(success) => {
                let resource = success.resource.try_into()?;
                let outcome = success.operation.into();
                let warnings = success.warnings.into_iter().map(Into::into).collect();

                domain::ApplyManifestApplicationDecision::Applied(domain::ApplyManifestResult {
                    resource,
                    outcome,
                    warnings,
                })
            }
            Self::ResourceApplyRejection(rejection) => {
                domain::ApplyManifestApplicationDecision::Rejected(rejection.into())
            }
            Self::Unknown => {
                return Err(InternalError::new(
                    "Remote apply returned an unrecognized ResourceApplyOutcome variant",
                ));
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ApplyManifestVariables {
    pub manifest: String,
    pub format: ResourceManifestFormat,
    pub dry_run: Option<bool>,
}

impl From<&ApplyManifestRequest> for ApplyManifestVariables {
    fn from(value: &ApplyManifestRequest) -> Self {
        Self {
            manifest: value.manifest.clone(),
            format: value.format.into(),
            dry_run: None,
        }
    }
}

impl ApplyManifestVariables {
    pub(crate) fn new(request: &ApplyManifestRequest, dry_run: bool) -> Self {
        let mut vars: Self = request.into();
        vars.dry_run = Some(dry_run);
        vars
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ApplyManifestVariables,
) -> cynic::Operation<ApplyManifestMutation, ApplyManifestVariables> {
    ApplyManifestMutation::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
