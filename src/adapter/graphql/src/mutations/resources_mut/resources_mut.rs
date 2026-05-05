// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::ResourceApplyOutcome;
use crate::prelude::*;
use crate::queries::{
    ResourceAccountSelectorInput,
    ResourceKind,
    ResourceManifestFormat,
    ResourceSelectorInput,
    map_resolve_manifest_account_error,
};

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
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let request = kamu_resources_facade::ApplyManifestRequest {
            format: format.into(),
            manifest,
        };

        let result = if dry_run.unwrap_or(false) {
            resource_facade
                .plan_apply_manifest(request)
                .await
                .map(ResourceApplyOutcome::from)
        } else {
            resource_facade
                .apply_manifest(request)
                .await
                .map(ResourceApplyOutcome::from)
        }
        .map_err(map_apply_resource_error)?;

        Ok(result)
    }

    #[tracing::instrument(level = "info", name = ResourcesMut_delete, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<ResourceDeleteResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let ResourceSelectorInput {
            kind,
            api_version,
            resource_ref,
            account,
        } = selector;
        let kind = kind.into_resource_type();

        let resource_id = resource_facade
            .delete(kamu_resources_facade::ResourceSelector {
                kind: kind.clone(),
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                api_version,
                resource_ref: resource_ref.into(),
            })
            .await
            .map_err(map_delete_resource_error)?;

        Ok(ResourceDeleteResult {
            resource_id: resource_id.into(),
            kind: ResourceKind::new(kind).into(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct ResourceDeleteResult {
    pub resource_id: ResourceID,
    pub kind: Option<ResourceKind>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_apply_resource_error(error: kamu_resources_facade::ApplyManifestError) -> GqlError {
    use kamu_resources_facade::ApplyManifestError as E;

    match error {
        E::ParseManifest(error) => GqlError::gql(error.to_string()),
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::InvalidMetadata(error) => GqlError::gql(error.to_string()),
        E::InvalidSpec(error) => GqlError::gql(error.to_string()),
        E::UIDNotFound(error) => GqlError::gql(error.to_string()),
        E::TypeMismatch(error) => GqlError::gql(error.to_string()),
        E::ConcurrentModification(error) => {
            tracing::error!(error = ?error, "Resource apply_manifest concurrent modification");
            GqlError::gql("Resource was modified concurrently")
        }
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_delete_resource_error(error: kamu_resources_facade::DeleteResourceError) -> GqlError {
    use kamu_resources_facade::DeleteResourceError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::LookupProblem(problem) => GqlError::gql(problem.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
