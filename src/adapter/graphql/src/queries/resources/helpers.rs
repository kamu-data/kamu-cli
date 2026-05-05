// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::Context;
use database_common::PaginationOpts;

use crate::prelude::*;
use crate::queries::{
    BatchResourceIdentitiesResult,
    BatchResourceManifestsResult,
    BatchResourcesResult,
    Resource,
    ResourceBatchSelectorInput,
    ResourceConnection,
    ResourceIdentity,
    ResourceIdentityConnection,
    ResourceKindDescriptor,
    ResourceKindInput,
    ResourceManifestFormat,
    ResourceRenderManifestResult,
    ResourceSelectorInput,
    ResourceSummary,
    ResourcesSummary,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_supported_resource_kinds(
    ctx: &Context<'_>,
) -> Result<Vec<ResourceKindDescriptor>> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    // TODO: Memoize supported resource kinds on the server side. This is
    // effectively static metadata and only changes when the deployed build
    // changes its registered resource kinds.
    let items = resource_facade
        .list_supported_kinds()
        .await
        .map_err(|error| match error {
            kamu_resources_facade::ListSupportedResourceKindsError::RemoteRequest(error) => {
                GqlError::from(error.int_err())
            }
            kamu_resources_facade::ListSupportedResourceKindsError::Internal(error) => {
                GqlError::from(error)
            }
        })?;

    Ok(items.into_iter().map(Into::into).collect())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn summary(
    ctx: &Context<'_>,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<ResourcesSummary> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let summary = resource_facade
        .summary(kamu_resources_facade::ResourcesSummaryRequest { account })
        .await
        .map_err(|error| match error {
            kamu_resources_facade::ResourcesSummaryError::BadAccount(error) => {
                map_resolve_manifest_account_error(error)
            }
            kamu_resources_facade::ResourcesSummaryError::RemoteRequest(error) => {
                error.int_err().into()
            }
            kamu_resources_facade::ResourcesSummaryError::Internal(error) => error.into(),
        })?;

    Ok(ResourcesSummary::from_domain(summary))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_resource(
    ctx: &Context<'_>,
    selector: ResourceSelectorInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<Option<Resource>> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceSelectorInput {
        kind,
        api_version,
        resource_ref,
    } = selector;

    let kind = kind.into_resource_type();

    let resource = resource_facade
        .get(kamu_resources_facade::ResourceSelector {
            account,
            kind,
            api_version,
            resource_ref: resource_ref.into(),
        })
        .await;

    match resource {
        Ok(resource) => Ok(Some(resource.into())),
        Err(kamu_resources_facade::GetResourceError::LookupProblem(
            kamu_resources_facade::ResourceLookupProblem::UIDNotFound(_)
            | kamu_resources_facade::ResourceLookupProblem::NameNotFound(_),
        )) => Ok(None),
        Err(kamu_resources_facade::GetResourceError::LookupProblem(problem)) => {
            Err(GqlError::gql(problem.to_string()))
        }
        Err(kamu_resources_facade::GetResourceError::UnsupportedDescriptor(_)) => {
            Err(GqlError::gql("Unsupported resource kind"))
        }
        Err(kamu_resources_facade::GetResourceError::BadAccount(error)) => {
            Err(map_resolve_manifest_account_error(error))
        }
        Err(kamu_resources_facade::GetResourceError::RemoteRequest(error)) => {
            Err(error.int_err().into())
        }
        Err(kamu_resources_facade::GetResourceError::Internal(error)) => Err(error.into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_resources(
    ctx: &Context<'_>,
    selector: ResourceBatchSelectorInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<BatchResourcesResult> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceBatchSelectorInput {
        kind,
        api_version,
        resource_refs,
    } = selector;

    resource_facade
        .get_many(kamu_resources_facade::ResourceBatchSelector {
            account,
            kind: kind.into_resource_type(),
            api_version,
            resource_refs: resource_refs.into_iter().map(Into::into).collect(),
        })
        .await
        .map(Into::into)
        .map_err(map_batch_resource_error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_resource_identity(
    ctx: &Context<'_>,
    selector: ResourceSelectorInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<Option<ResourceIdentity>> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceSelectorInput {
        kind,
        api_version,
        resource_ref,
    } = selector;

    let kind = kind.into_resource_type();

    let identity = resource_facade
        .get_identity(kamu_resources_facade::ResourceSelector {
            account,
            kind,
            api_version,
            resource_ref: resource_ref.into(),
        })
        .await;

    match identity {
        Ok(identity) => Ok(Some(identity.into())),
        Err(kamu_resources_facade::GetResourceError::LookupProblem(
            kamu_resources_facade::ResourceLookupProblem::UIDNotFound(_)
            | kamu_resources_facade::ResourceLookupProblem::NameNotFound(_),
        )) => Ok(None),
        Err(kamu_resources_facade::GetResourceError::LookupProblem(problem)) => {
            Err(GqlError::gql(problem.to_string()))
        }
        Err(kamu_resources_facade::GetResourceError::UnsupportedDescriptor(_)) => {
            Err(GqlError::gql("Unsupported resource kind"))
        }
        Err(kamu_resources_facade::GetResourceError::BadAccount(error)) => {
            Err(map_resolve_manifest_account_error(error))
        }
        Err(kamu_resources_facade::GetResourceError::RemoteRequest(error)) => {
            Err(error.int_err().into())
        }
        Err(kamu_resources_facade::GetResourceError::Internal(error)) => Err(error.into()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn get_resource_identities(
    ctx: &Context<'_>,
    selector: ResourceBatchSelectorInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<BatchResourceIdentitiesResult> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceBatchSelectorInput {
        kind,
        api_version,
        resource_refs,
    } = selector;

    resource_facade
        .get_identities(kamu_resources_facade::ResourceBatchSelector {
            account,
            kind: kind.into_resource_type(),
            api_version,
            resource_refs: resource_refs.into_iter().map(Into::into).collect(),
        })
        .await
        .map(Into::into)
        .map_err(map_batch_resource_error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_resources_connection(
    ctx: &Context<'_>,
    kind: ResourceKindInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
    page: usize,
    per_page: usize,
) -> Result<ResourceConnection> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let items = resource_facade
        .list(kamu_resources_facade::ListResourcesRequest {
            kind: kind.into_resource_type(),
            account,
            pagination: PaginationOpts::from_page(page, per_page),
        })
        .await
        .map_err(map_list_resources_error)?;

    let total_count = items.len();
    let items = items.into_iter().map(ResourceSummary::from).collect();

    Ok(ResourceConnection::new(items, page, per_page, total_count))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_resource_identities_connection(
    ctx: &Context<'_>,
    kind: ResourceKindInput,
    account: Option<kamu_resources::ResourceManifestAccount>,
    page: usize,
    per_page: usize,
) -> Result<ResourceIdentityConnection> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let items = resource_facade
        .list_identities(kamu_resources_facade::ListResourceIdentitiesRequest {
            kind: kind.into_resource_type(),
            account,
            pagination: PaginationOpts::from_page(page, per_page),
        })
        .await
        .map_err(map_list_resources_error)?;

    let total_count = items.len();
    let items = items.into_iter().map(ResourceIdentity::from).collect();

    Ok(ResourceIdentityConnection::new(
        items,
        page,
        per_page,
        total_count,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_all_resources_connection(
    ctx: &Context<'_>,
    account: Option<kamu_resources::ResourceManifestAccount>,
    page: usize,
    per_page: usize,
) -> Result<ResourceConnection> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let items = resource_facade
        .list_all(kamu_resources_facade::ListAllResourcesRequest {
            account,
            pagination: PaginationOpts::from_page(page, per_page),
        })
        .await
        .map_err(map_list_all_resources_error)?;

    let total_count = items.len();
    let items = items.into_iter().map(ResourceSummary::from).collect();

    Ok(ResourceConnection::new(items, page, per_page, total_count))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn list_all_resource_identities_connection(
    ctx: &Context<'_>,
    account: Option<kamu_resources::ResourceManifestAccount>,
    page: usize,
    per_page: usize,
) -> Result<ResourceIdentityConnection> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let items = resource_facade
        .list_all_identities(kamu_resources_facade::ListAllResourceIdentitiesRequest {
            account,
            pagination: PaginationOpts::from_page(page, per_page),
        })
        .await
        .map_err(map_list_all_resources_error)?;

    let total_count = items.len();
    let items = items.into_iter().map(ResourceIdentity::from).collect();

    Ok(ResourceIdentityConnection::new(
        items,
        page,
        per_page,
        total_count,
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn render_resource_manifest(
    ctx: &Context<'_>,
    selector: ResourceSelectorInput,
    format: ResourceManifestFormat,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<ResourceRenderManifestResult> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceSelectorInput {
        kind,
        api_version,
        resource_ref,
    } = selector;

    let rendered = resource_facade
        .render_manifest(
            kamu_resources_facade::ResourceSelector {
                account,
                kind: kind.into_resource_type(),
                api_version,
                resource_ref: resource_ref.into(),
            },
            format.into(),
        )
        .await
        .map_err(map_render_resource_manifest_error)?;

    Ok(ResourceRenderManifestResult {
        manifest: rendered.manifest,
        format: rendered.format.into(),
    })
}

pub(crate) async fn render_resource_manifests(
    ctx: &Context<'_>,
    selector: ResourceBatchSelectorInput,
    format: ResourceManifestFormat,
    account: Option<kamu_resources::ResourceManifestAccount>,
) -> Result<BatchResourceManifestsResult> {
    let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

    let ResourceBatchSelectorInput {
        kind,
        api_version,
        resource_refs,
    } = selector;

    resource_facade
        .render_manifests(
            kamu_resources_facade::ResourceBatchSelector {
                account,
                kind: kind.into_resource_type(),
                api_version,
                resource_refs: resource_refs.into_iter().map(Into::into).collect(),
            },
            format.into(),
        )
        .await
        .map(Into::into)
        .map_err(map_batch_resource_error)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_render_resource_manifest_error(
    error: kamu_resources_facade::RenderResourceManifestError,
) -> GqlError {
    use kamu_resources_facade::RenderResourceManifestError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::LookupProblem(problem) => GqlError::gql(problem.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_resource_error(error: kamu_resources_facade::BatchResourceError) -> GqlError {
    use kamu_resources_facade::BatchResourceError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_list_resources_error(error: kamu_resources_facade::ListResourcesError) -> GqlError {
    use kamu_resources_facade::ListResourcesError as E;

    match error {
        E::UnsupportedDescriptor(_) => GqlError::gql("Unsupported resource kind"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

fn map_list_all_resources_error(error: kamu_resources_facade::ListAllResourcesError) -> GqlError {
    use kamu_resources_facade::ListAllResourcesError as E;

    match error {
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

pub(crate) fn map_resolve_manifest_account_error(
    error: kamu_resources_facade::ResolveManifestAccountError,
) -> GqlError {
    use kamu_resources_facade::ResolveManifestAccountError as E;

    match error {
        E::AnonymousSubject => {
            unreachable!("GraphQL resource resolvers should enforce logged-in access")
        }
        E::EmptySelector
        | E::IdNameMismatch { .. }
        | E::AccountNotFoundById(_)
        | E::AccountNotFoundByName(_) => {
            unreachable!("GraphQL resource selectors should be validated before facade calls")
        }
        E::Access(error) => error.into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
