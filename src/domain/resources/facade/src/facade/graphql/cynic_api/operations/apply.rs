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
use crate::facade::graphql::cynic_api::fragments::{
    Resource,
    ResourceBadAccountProblem,
    ResourceManifestFormat,
    ResourceUnsupportedDescriptorProblem,
};
use crate::facade::graphql::cynic_api::schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "Mutation", variables = "ApplyManifestVariables")]
pub(crate) struct ApplyManifestMutation {
    pub resources: ApplyManifestResourcesMut,
}

#[derive(cynic::QueryFragment, Debug, Clone)]
#[cynic(graphql_type = "ResourcesMut", variables = "ApplyManifestVariables")]
pub(crate) struct ApplyManifestResourcesMut {
    #[arguments(manifest: $manifest, format: $format, dryRun: $dry_run)]
    pub apply_manifest: ResourceApplyOutcome,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::InlineFragments, Debug, Clone)]
#[cynic(graphql_type = "ResourceApplyOutcome")]
pub(crate) enum ResourceApplyOutcome {
    ResourceApplySuccess(ResourceApplySuccess),
    ResourceApplyRejection(ResourceApplyRejection),
    ResourceApplyParseManifestProblem(ResourceApplyParseManifestProblem),
    ResourceUnsupportedDescriptorProblem(ResourceUnsupportedDescriptorProblem),
    ResourceBadAccountProblem(ResourceBadAccountProblem),
    ResourceInvalidHeaderProblem(ResourceInvalidHeaderProblem),
    ResourceInvalidSpecProblem(ResourceInvalidSpecProblem),
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
    Headers,
    Spec,
}

impl From<ResourceApplyChangeKind> for domain::ApplyManifestChangeKind {
    fn from(value: ResourceApplyChangeKind) -> Self {
        match value {
            ResourceApplyChangeKind::Generation => Self::Generation,
            ResourceApplyChangeKind::Headers => Self::Headers,
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

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceApplyParseManifestProblem {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceInvalidHeaderProblem {
    pub code: ResourceHeaderValidationProblemCode,
    pub message: String,
}

#[derive(cynic::Enum, Debug, Clone, Copy)]
pub(crate) enum ResourceHeaderValidationProblemCode {
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

impl From<ResourceHeaderValidationProblemCode> for crate::ResourceHeadersValidationProblemCode {
    fn from(value: ResourceHeaderValidationProblemCode) -> Self {
        use ResourceHeaderValidationProblemCode as C;
        match value {
            C::EmptyName => Self::EmptyName,
            C::NameTooLong => Self::NameTooLong,
            C::InvalidName => Self::InvalidName,
            C::DescriptionTooLong => Self::DescriptionTooLong,
            C::TooManyLabels => Self::TooManyLabels,
            C::InvalidLabelKey => Self::InvalidLabelKey,
            C::DuplicateLabelKey => Self::DuplicateLabelKey,
            C::LabelValueTooLong => Self::LabelValueTooLong,
            C::TooManyAnnotations => Self::TooManyAnnotations,
            C::InvalidAnnotationKey => Self::InvalidAnnotationKey,
            C::DuplicateAnnotationKey => Self::DuplicateAnnotationKey,
            C::AnnotationValueTooLong => Self::AnnotationValueTooLong,
        }
    }
}

impl From<ResourceInvalidHeaderProblem> for crate::ApplyManifestError {
    fn from(value: ResourceInvalidHeaderProblem) -> Self {
        Self::InvalidHeaders(crate::ResourceInvalidHeadersError {
            code: value.code.into(),
            message: value.message,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryFragment, Debug, Clone)]
pub(crate) struct ResourceInvalidSpecProblem {
    pub schema: String,
    pub message: String,
}

impl From<ResourceInvalidSpecProblem> for crate::ApplyManifestError {
    fn from(value: ResourceInvalidSpecProblem) -> Self {
        Self::InvalidSpec(kamu_resources::ResourceInvalidSpecError {
            schema: value.schema,
            message: value.message,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceApplyOutcome {
    pub(crate) fn try_into_planning_decision(
        self,
    ) -> Result<domain::ApplyManifestPlanningDecision, crate::ApplyManifestError> {
        Ok(match self {
            Self::ResourceApplySuccess(success) => {
                let resource = success
                    .resource
                    .try_into()
                    .map_err(crate::ApplyManifestError::Internal)?;
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
            problem => return Err(map_apply_problem(problem)?),
        })
    }

    pub(crate) fn try_into_application_decision(
        self,
    ) -> Result<domain::ApplyManifestApplicationDecision, crate::ApplyManifestError> {
        Ok(match self {
            Self::ResourceApplySuccess(success) => {
                let resource = success
                    .resource
                    .try_into()
                    .map_err(crate::ApplyManifestError::Internal)?;
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
            problem => return Err(map_apply_problem(problem)?),
        })
    }
}

fn map_apply_problem(
    outcome: ResourceApplyOutcome,
) -> Result<crate::ApplyManifestError, InternalError> {
    use crate::facade::graphql::outcome_mapper;
    use crate::facade::resource_facade_errors::ParseResourceManifestError;

    match outcome {
        ResourceApplyOutcome::ResourceApplyParseManifestProblem(p) => {
            Ok(crate::ApplyManifestError::ParseManifest(
                ParseResourceManifestError { message: p.message },
            ))
        }
        ResourceApplyOutcome::ResourceUnsupportedDescriptorProblem(p) => {
            Ok(crate::ApplyManifestError::UnsupportedDescriptor(
                outcome_mapper::unsupported_descriptor_problem_error(p),
            ))
        }
        ResourceApplyOutcome::ResourceBadAccountProblem(p) => Ok(
            crate::ApplyManifestError::BadAccount(outcome_mapper::bad_account_problem_error(p)?),
        ),
        ResourceApplyOutcome::ResourceInvalidHeaderProblem(p) => Ok(p.into()),
        ResourceApplyOutcome::ResourceInvalidSpecProblem(p) => Ok(p.into()),
        ResourceApplyOutcome::Unknown => Err(InternalError::new(
            "Remote apply returned an unrecognized ResourceApplyOutcome variant",
        )),
        // Success and Rejection are handled by the caller — should not reach here
        _ => Err(InternalError::new(
            "map_apply_problem called with non-problem outcome variant",
        )),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(cynic::QueryVariables, Debug, Clone)]
pub(crate) struct ApplyManifestVariables {
    pub manifest: String,
    pub format: ResourceManifestFormat,
    pub dry_run: bool,
}

impl ApplyManifestVariables {
    pub(crate) fn new(request: ApplyManifestRequest, dry_run: bool) -> Self {
        Self {
            manifest: request.manifest,
            format: request.format.into(),
            dry_run,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn build_operation(
    variables: ApplyManifestVariables,
) -> cynic::Operation<ApplyManifestMutation, ApplyManifestVariables> {
    ApplyManifestMutation::build(variables)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::facade::graphql::cynic_api::fragments::ResourceManifestFormat;

    fn test_variables() -> ApplyManifestVariables {
        ApplyManifestVariables {
            manifest: String::new(),
            format: ResourceManifestFormat::Yaml,
            dry_run: false,
        }
    }

    #[test]
    fn apply_manifest_operation_selects_required_union_arms() {
        let op = build_operation(test_variables());
        let query = &op.query;
        assert!(
            query.contains("ResourceApplySuccess"),
            "missing success arm"
        );
        assert!(
            query.contains("ResourceApplyRejection"),
            "missing rejection arm"
        );
        assert!(query.contains("resource"), "missing resource field");
        assert!(query.contains("spec"), "missing spec field");
    }

    #[test]
    fn apply_unknown_outcome_returns_internal_error_for_planning() {
        let result = ResourceApplyOutcome::Unknown.try_into_planning_decision();
        assert!(
            result.is_err(),
            "expected InternalError for Unknown outcome"
        );
    }

    #[test]
    fn apply_unknown_outcome_returns_internal_error_for_application() {
        let result = ResourceApplyOutcome::Unknown.try_into_application_decision();
        assert!(
            result.is_err(),
            "expected InternalError for Unknown outcome"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
