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
    ) -> Result<ResourceApplyResult> {
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

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceApplyResult {
    pub operation: ResourceApplyOperation,
    pub resource: Resource,
}

impl From<kamu_resources::ApplyManifestResult> for ResourceApplyResult {
    fn from(value: kamu_resources::ApplyManifestResult) -> Self {
        Self {
            operation: value.outcome.into(),
            resource: value.resource.into(),
        }
    }
}

impl From<kamu_resources::ApplyManifestPlan> for ResourceApplyResult {
    fn from(value: kamu_resources::ApplyManifestPlan) -> Self {
        Self {
            operation: value.outcome.into(),
            resource: value.resource.into(),
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
