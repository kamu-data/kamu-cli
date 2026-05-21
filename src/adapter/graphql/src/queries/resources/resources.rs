// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;

use crate::LoggedInGuard;
use crate::prelude::*;
use crate::queries::{
    BatchResourceIdentitiesResult,
    BatchResourceManifestsResult,
    BatchResourcesResult,
    Resource,
    ResourceAccountSelectorInput,
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
    SearchResourceIdentitiesInput,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Resources;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Resources {
    const DEFAULT_PER_PAGE: usize = 15;

    /// Returns resource kinds supported by the current server
    #[tracing::instrument(level = "info", name = Resources_supported_kinds, skip_all)]
    async fn supported_kinds(&self, ctx: &Context<'_>) -> Result<Vec<ResourceKindDescriptor>> {
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

    /// Returns a summary-oriented dashboard for the current or specified
    /// subject
    #[tracing::instrument(level = "info", name = Resources_summary, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn summary(
        &self,
        ctx: &Context<'_>,
        account: Option<ResourceAccountSelectorInput>,
    ) -> Result<ResourcesSummary> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let summary = resource_facade
            .summary(kamu_resources_facade::ResourcesSummaryRequest {
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
            })
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

        Ok(summary.into())
    }

    /// Returns a resource by selector, if found
    #[tracing::instrument(level = "info", name = Resources_resource, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
        #[graphql(default)] revealed: bool,
    ) -> Result<Option<Resource>> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        match resource_facade.get(selector.into(), spec_view_mode).await {
            Ok(resource) => Ok(Some(resource.into())),
            Err(e) => {
                map_get_resource_error(e)?;
                Ok(None)
            }
        }
    }

    /// Returns resources by selectors
    #[tracing::instrument(level = "info", name = Resources_resources, skip_all, fields(selector_count = selector.resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resources(
        &self,
        ctx: &Context<'_>,
        selector: ResourceBatchSelectorInput,
        #[graphql(default)] revealed: bool,
    ) -> Result<BatchResourcesResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        resource_facade
            .get_many(selector.into(), spec_view_mode)
            .await
            .map(Into::into)
            .map_err(map_batch_resource_error)
    }

    /// Returns resource identity by selector, if found
    #[tracing::instrument(level = "info", name = Resources_resource_identity, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource_identity(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<Option<ResourceIdentity>> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade.get_identity(selector.into()).await {
            Ok(identity) => Ok(Some(identity.into())),
            Err(e) => {
                map_get_resource_error(e)?;
                Ok(None)
            }
        }
    }

    /// Returns resource identities by selectors
    #[tracing::instrument(level = "info", name = Resources_resource_identities, skip_all, fields(selector_count = selector.resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource_identities(
        &self,
        ctx: &Context<'_>,
        selector: ResourceBatchSelectorInput,
    ) -> Result<BatchResourceIdentitiesResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        resource_facade
            .get_identities(selector.into())
            .await
            .map(Into::into)
            .map_err(map_batch_resource_error)
    }

    /// Returns resources of the specified kind
    #[tracing::instrument(level = "info", name = Resources_list_by_kind, skip_all, fields(?kind, ?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_by_kind(
        &self,
        ctx: &Context<'_>,
        kind: ResourceKindInput,
        account: Option<ResourceAccountSelectorInput>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let items = resource_facade
            .list(kamu_resources_facade::ListResourcesRequest {
                kind: kind.into_resource_type(),
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                pagination: PaginationOpts::from_page(page, per_page),
            })
            .await
            .map_err(map_list_resources_error)?;

        let total_count = items.len();
        let items = items.into_iter().map(ResourceSummary::from).collect();

        Ok(ResourceConnection::new(items, page, per_page, total_count))
    }

    /// Returns resource identities of the specified kind
    #[tracing::instrument(level = "info", name = Resources_list_identities_by_kind, skip_all, fields(?kind, ?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_identities_by_kind(
        &self,
        ctx: &Context<'_>,
        kind: ResourceKindInput,
        account: Option<ResourceAccountSelectorInput>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceIdentityConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let items = resource_facade
            .list_identities(kamu_resources_facade::ListResourceIdentitiesRequest {
                kind: kind.into_resource_type(),
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
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

    /// Searches resource identities across the specified exact kinds
    #[tracing::instrument(level = "info", name = Resources_search_identities, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn search_identities(
        &self,
        ctx: &Context<'_>,
        query: SearchResourceIdentitiesInput,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceIdentityConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let response = resource_facade
            .search_identities(query.into_facade_request(PaginationOpts::from_page(page, per_page)))
            .await
            .map_err(map_list_resources_error)?;

        let total_count = response.total_count;
        let items = response
            .items
            .into_iter()
            .map(ResourceIdentity::from)
            .collect();

        Ok(ResourceIdentityConnection::new(
            items,
            page,
            per_page,
            total_count,
        ))
    }

    /// Returns resources across all kinds
    #[tracing::instrument(level = "info", name = Resources_list_all, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_all(
        &self,
        ctx: &Context<'_>,
        account: Option<ResourceAccountSelectorInput>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let items = resource_facade
            .list_all(kamu_resources_facade::ListAllResourcesRequest {
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
                pagination: PaginationOpts::from_page(page, per_page),
            })
            .await
            .map_err(map_list_all_resources_error)?;

        let total_count = items.len();
        let items = items.into_iter().map(ResourceSummary::from).collect();

        Ok(ResourceConnection::new(items, page, per_page, total_count))
    }

    /// Returns resource identities across all kinds
    #[tracing::instrument(level = "info", name = Resources_list_all_identities, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_all_identities(
        &self,
        ctx: &Context<'_>,
        account: Option<ResourceAccountSelectorInput>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceIdentityConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let items = resource_facade
            .list_all_identities(kamu_resources_facade::ListAllResourceIdentitiesRequest {
                account: account.map(ResourceAccountSelectorInput::into_manifest_account),
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

    /// Renders a canonical manifest representation from a stored resource
    #[tracing::instrument(level = "info", name = Resources_render_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn render_manifest(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
        format: ResourceManifestFormat,
        #[graphql(default)] revealed: bool,
    ) -> Result<ResourceRenderManifestResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        let rendered = resource_facade
            .render_manifest(selector.into(), format.into(), spec_view_mode)
            .await
            .map_err(map_render_resource_manifest_error)?;

        Ok(ResourceRenderManifestResult {
            manifest: rendered.manifest,
            format: rendered.format.into(),
        })
    }

    /// Renders canonical manifest representations from stored resources
    #[tracing::instrument(level = "info", name = Resources_render_manifests, skip_all, fields(selector_count = selector.resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn render_manifests(
        &self,
        ctx: &Context<'_>,
        selector: ResourceBatchSelectorInput,
        format: ResourceManifestFormat,
        #[graphql(default)] revealed: bool,
    ) -> Result<BatchResourceManifestsResult> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        resource_facade
            .render_manifests(selector.into(), format.into(), spec_view_mode)
            .await
            .map(Into::into)
            .map_err(map_batch_resource_error)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Resources {
    fn spec_view_mode_from_revealed(revealed: bool) -> kamu_resources_facade::SpecViewMode {
        if revealed {
            kamu_resources_facade::SpecViewMode::Revealed
        } else {
            kamu_resources_facade::SpecViewMode::Encrypted
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_get_resource_error(error: kamu_resources_facade::GetResourceError) -> Result<()> {
    use kamu_resources_facade::{GetResourceError as E, ResourceLookupProblem as P};

    match error {
        E::LookupProblem(P::UIDNotFound(_) | P::NameNotFound(_)) => Ok(()),
        E::LookupProblem(problem) => Err(GqlError::gql(problem.to_string())),
        E::UnsupportedDescriptor(_) => Err(GqlError::gql("Unsupported resource kind")),
        E::BadAccount(error) => Err(map_resolve_manifest_account_error(error)),
        E::RemoteRequest(error) => Err(error.int_err().into()),
        E::Internal(error) => Err(error.into()),
    }
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_list_all_resources_error(error: kamu_resources_facade::ListAllResourcesError) -> GqlError {
    use kamu_resources_facade::ListAllResourcesError as E;

    match error {
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_resolve_manifest_account_error(
    error: kamu_resources_facade::ResolveManifestAccountError,
) -> GqlError {
    use kamu_resources_facade::ResolveManifestAccountError as E;

    match error {
        E::AnonymousSubject => GqlError::Access(odf::AccessError::Unauthenticated(
            "Anonymous subject cannot resolve a target account".into(),
        )),
        E::EmptySelector
        | E::IdNameMismatch { .. }
        | E::AccountNotFoundById(_)
        | E::AccountNotFoundByName(_) => GqlError::gql(error.to_string()),
        E::Access(error) => error.into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
