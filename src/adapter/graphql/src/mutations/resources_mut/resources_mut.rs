// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::{Resource, ResourceKind, ResourceManifestFormat, ResourceSelectorInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ResourcesMut;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl ResourcesMut {
    #[tracing::instrument(level = "info", name = ResourcesMut_apply_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn apply_manifest(
        &self,
        ctx: &Context<'_>,
        manifest: String,
        format: ResourceManifestFormat,
        dry_run: Option<bool>,
    ) -> Result<ResourceApplyOutcome> {
        super::helpers::apply_resource_manifest(ctx, manifest, format, dry_run.unwrap_or(false))
            .await
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_delete, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<ResourceDeleteResult> {
        super::helpers::delete_resource(ctx, selector, None).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceApplyOperation {
    Created,
    Updated,
    Unchanged,
}

impl From<kamu_resources::ApplyResourceOutcome> for ResourceApplyOperation {
    fn from(value: kamu_resources::ApplyResourceOutcome) -> Self {
        match value {
            kamu_resources::ApplyResourceOutcome::Created => Self::Created,
            kamu_resources::ApplyResourceOutcome::Updated => Self::Updated,
            kamu_resources::ApplyResourceOutcome::Untouched => Self::Unchanged,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceApplyRejectionCategory {
    ImmutableFieldChanged,
    BusinessValidationFailed,
    ReferencedObjectMissing,
    LifecycleRuleConflict,
}

impl From<kamu_resources::ApplyResourceRejectionCategory> for ResourceApplyRejectionCategory {
    fn from(value: kamu_resources::ApplyResourceRejectionCategory) -> Self {
        match value {
            kamu_resources::ApplyResourceRejectionCategory::ImmutableFieldChanged => {
                Self::ImmutableFieldChanged
            }
            kamu_resources::ApplyResourceRejectionCategory::BusinessValidationFailed => {
                Self::BusinessValidationFailed
            }
            kamu_resources::ApplyResourceRejectionCategory::ReferencedObjectMissing => {
                Self::ReferencedObjectMissing
            }
            kamu_resources::ApplyResourceRejectionCategory::LifecycleRuleConflict => {
                Self::LifecycleRuleConflict
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplySuccess {
    pub operation: ResourceApplyOperation,
    pub resource: Resource,
}

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyRejection {
    pub category: ResourceApplyRejectionCategory,
    pub message: String,
}

#[derive(Union, Debug, Clone)]
pub enum ResourceApplyOutcome {
    Success(ResourceApplySuccess),
    Rejection(ResourceApplyRejection),
}

impl From<kamu_resources::ApplyManifestPlanningDecision> for ResourceApplyOutcome {
    fn from(value: kamu_resources::ApplyManifestPlanningDecision) -> Self {
        match value {
            kamu_resources::ApplyManifestPlanningDecision::Planned(plan) => {
                Self::Success(ResourceApplySuccess {
                    operation: plan.outcome.into(),
                    resource: plan.resource.into(),
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
                })
            }
            kamu_resources::ApplyManifestApplicationDecision::Rejected(rejection) => {
                Self::Rejection(rejection.into())
            }
        }
    }
}

impl From<kamu_resources_facade::ApplyManifestRejection> for ResourceApplyRejection {
    fn from(value: kamu_resources_facade::ApplyManifestRejection) -> Self {
        Self {
            category: value.category.into(),
            message: value.message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteResult {
    pub resource_id: ResourceID,
    pub kind: Option<ResourceKind>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
