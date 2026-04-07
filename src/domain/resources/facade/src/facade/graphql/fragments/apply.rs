// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_resources as domain;
use serde::Deserialize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ApplyManifestMutationDataFragment {
    pub resources: ResourcesMutFragment,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourcesMutFragment {
    pub apply_manifest: ResourceApplyOutcomeFragment,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(tag = "__typename")]
pub(crate) enum ResourceApplyOutcomeFragment {
    ResourceApplySuccess(ResourceApplySuccessFragment),
    ResourceApplyRejection(ResourceApplyRejectionFragment),
}

impl ResourceApplyOutcomeFragment {
    pub(crate) fn into_planning_decision(
        self,
    ) -> Result<domain::ApplyManifestPlanningDecision, InternalError> {
        Ok(match self {
            Self::ResourceApplySuccess(success) => {
                let (outcome, resource, changes, warnings) = success.into_parts()?;

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
        })
    }

    pub(crate) fn into_application_decision(
        self,
    ) -> Result<domain::ApplyManifestApplicationDecision, InternalError> {
        Ok(match self {
            Self::ResourceApplySuccess(success) => {
                let (outcome, resource, _changes, warnings) = success.into_parts()?;

                domain::ApplyManifestApplicationDecision::Applied(domain::ApplyManifestResult {
                    resource,
                    outcome,
                    warnings,
                })
            }
            Self::ResourceApplyRejection(rejection) => {
                domain::ApplyManifestApplicationDecision::Rejected(rejection.into())
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceApplySuccessFragment {
    pub operation: ResourceApplyOperationFragment,
    pub resource: ResourceFragment,
    pub changes: Vec<ResourceApplyChangeFragment>,
    pub warnings: Vec<ResourceApplyWarningFragment>,
}

impl ResourceApplySuccessFragment {
    fn into_parts(
        self,
    ) -> Result<
        (
            domain::ApplyResourceOutcome,
            domain::ResourceView,
            Vec<domain::ApplyManifestChange>,
            Vec<domain::ResourceWarning>,
        ),
        InternalError,
    > {
        Ok((
            self.operation.into(),
            self.resource.try_into()?,
            self.changes.into_iter().map(Into::into).collect(),
            self.warnings.into_iter().map(Into::into).collect(),
        ))
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ResourceApplyOperationFragment {
    Created,
    Updated,
    Untouched,
}

impl From<ResourceApplyOperationFragment> for domain::ApplyResourceOutcome {
    fn from(value: ResourceApplyOperationFragment) -> Self {
        match value {
            ResourceApplyOperationFragment::Created => Self::Created,
            ResourceApplyOperationFragment::Updated => Self::Updated,
            ResourceApplyOperationFragment::Untouched => Self::Untouched,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceApplyChangeFragment {
    pub kind: ResourceApplyChangeKindFragment,
    pub path: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
}

impl From<ResourceApplyChangeFragment> for domain::ApplyManifestChange {
    fn from(value: ResourceApplyChangeFragment) -> Self {
        Self {
            kind: value.kind.into(),
            path: value.path,
            before: value.before,
            after: value.after,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ResourceApplyChangeKindFragment {
    Generation,
    Metadata,
    Spec,
}

impl From<ResourceApplyChangeKindFragment> for domain::ApplyManifestChangeKind {
    fn from(value: ResourceApplyChangeKindFragment) -> Self {
        match value {
            ResourceApplyChangeKindFragment::Generation => Self::Generation,
            ResourceApplyChangeKindFragment::Metadata => Self::Metadata,
            ResourceApplyChangeKindFragment::Spec => Self::Spec,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceApplyRejectionFragment {
    pub category: ResourceApplyRejectionCategoryFragment,
    pub message: String,
}

impl From<ResourceApplyRejectionFragment> for domain::ApplyManifestRejection {
    fn from(value: ResourceApplyRejectionFragment) -> Self {
        Self {
            category: value.category.into(),
            message: value.message,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum ResourceApplyRejectionCategoryFragment {
    ImmutableFieldChanged,
    BusinessValidationFailed,
    ReferencedObjectMissing,
    LifecycleRuleConflict,
}

impl From<ResourceApplyRejectionCategoryFragment> for domain::ApplyResourceRejectionCategory {
    fn from(value: ResourceApplyRejectionCategoryFragment) -> Self {
        match value {
            ResourceApplyRejectionCategoryFragment::ImmutableFieldChanged => {
                Self::ImmutableFieldChanged
            }
            ResourceApplyRejectionCategoryFragment::BusinessValidationFailed => {
                Self::BusinessValidationFailed
            }
            ResourceApplyRejectionCategoryFragment::ReferencedObjectMissing => {
                Self::ReferencedObjectMissing
            }
            ResourceApplyRejectionCategoryFragment::LifecycleRuleConflict => {
                Self::LifecycleRuleConflict
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceApplyWarningFragment {
    pub code: String,
    pub path: Option<String>,
    pub message: String,
}

impl From<ResourceApplyWarningFragment> for domain::ResourceWarning {
    fn from(value: ResourceApplyWarningFragment) -> Self {
        Self {
            code: Box::leak(value.code.into_boxed_str()),
            path: value.path,
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceFragment {
    pub api_version: String,
    pub kind: ResourceKindFragment,
    pub metadata: ResourceMetadataFragment,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
}

impl TryFrom<ResourceFragment> for domain::ResourceView {
    type Error = InternalError;

    fn try_from(value: ResourceFragment) -> Result<Self, Self::Error> {
        let labels: std::collections::BTreeMap<String, String> =
            serde_json::from_value(value.metadata.labels).int_err()?;
        let annotations: std::collections::BTreeMap<String, String> =
            serde_json::from_value(value.metadata.annotations).int_err()?;

        Ok(Self {
            kind: value.kind.value,
            api_version: value.api_version,
            account: domain::ResourceViewAccount {
                id: value.metadata.account_id,
                name: None,
            },
            metadata: domain::ResourceViewMetadata {
                uid: value.metadata.id,
                name: value.metadata.name,
                description: value.metadata.description,
                labels,
                annotations,
                generation: value.metadata.generation,
                created_at: value.metadata.created_at,
                updated_at: value.metadata.updated_at,
                deleted_at: value.metadata.deleted_at,
            },
            last_reconciled_at: value.metadata.last_reconciled_at,
            spec: value.spec,
            status: value.status,
        })
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResourceKindFragment {
    pub value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResourceMetadataFragment {
    pub id: domain::ResourceUID,
    pub account_id: odf::AccountID,
    pub name: String,
    pub description: Option<String>,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
