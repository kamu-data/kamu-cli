// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Context;

use crate::mutations::{ResourceApplyOutcome, ResourceDeleteResult};
use crate::prelude::*;
use crate::queries::helpers::map_resolve_manifest_account_error;
use crate::queries::{ResourceKind, ResourceManifestFormat, ResourceSelectorInput};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn apply_resource_manifest(
    ctx: &Context<'_>,
    manifest: String,
    format: ResourceManifestFormat,
    dry_run: bool,
) -> Result<ResourceApplyOutcome> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let request = kamu_resources_facade::ApplyManifestRequest {
        format: format.into(),
        manifest,
    };

    let result = if dry_run {
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
pub(crate) fn map_apply_resource_error(
    error: kamu_resources_facade::ApplyManifestError,
) -> GqlError {
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

pub(crate) async fn delete_resource(
    ctx: &Context<'_>,
    selector: ResourceSelectorInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<ResourceDeleteResult> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let kind = selector.kind.into_resource_type();
    let resource_id = resource_facade
        .delete(kamu_resources_facade::DeleteResourceRequest {
            kind: kind.clone(),
            account,
            resource_ref: selector.resource_ref.into(),
        })
        .await
        .map_err(map_delete_resource_error)?;

    Ok(ResourceDeleteResult {
        resource_id: resource_id.into(),
        kind: ResourceKind::new(kind).into(),
    })
}

pub(crate) fn map_delete_resource_error(
    error: kamu_resources_facade::DeleteResourceError,
) -> GqlError {
    use kamu_resources_facade::DeleteResourceError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => GqlError::Internal(error.int_err()),
        E::UIDNotFound(error) => GqlError::gql(error.to_string()),
        E::NameNotFound(error) => GqlError::gql(error.to_string()),
        E::KindMismatch(error) => GqlError::gql(error.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
