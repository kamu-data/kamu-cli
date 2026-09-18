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
    AccountRefInput,
    BatchResourceHandlesOutcome,
    BatchResourceManifestsOutcome,
    BatchResourcesOutcome,
    ResourceAccountResolutionProblem,
    ResourceConnection,
    ResourceHandle,
    ResourceHandleConnection,
    ResourceInvalidLabelFilterProblem,
    ResourceManifestFormat,
    ResourceRefInput,
    ResourceSummary,
    ResourceTypeDescriptor,
    ResourceUnsupportedSelectorProblem,
    ResourcesSummary,
    SearchResourcesInput,
    SpecViewOptsInput,
    into_resource_refs,
    map_unsupported_selector_problem,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Resources
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Resources;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl Resources {
    const DEFAULT_PER_PAGE: usize = 15;

    /// Returns resource types supported by the current server
    #[tracing::instrument(level = "info", name = Resources_supported_resource_types, skip_all)]
    async fn supported_resource_types(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Vec<ResourceTypeDescriptor>> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        // TODO: Memoize supported resource kinds on the server side. This is
        // effectively static metadata and only changes when the deployed build
        // changes its registered resource kinds.
        let items = resource_facade
            .list_supported_resource_types()
            .await
            .map_err(|error| match error {
                kamu_resources_facade::ListSupportedResourceTypesError::RemoteRequest(error) => {
                    GqlError::from(error.int_err())
                }
                kamu_resources_facade::ListSupportedResourceTypesError::Internal(error) => {
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
        account: Option<AccountRefInput>,
    ) -> Result<ResourcesSummaryOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .summary(kamu_resources_facade::ResourcesSummaryRequest {
                account: account.map(AccountRefInput::into_manifest_account),
            })
            .await
        {
            Ok(summary) => Ok(ResourcesSummaryOutcome::Success(summary.into())),
            Err(kamu_resources_facade::ResourcesSummaryError::AccountResolution(error)) => {
                Ok(ResourcesSummaryOutcome::AccountResolution(error.into()))
            }
            Err(kamu_resources_facade::ResourcesSummaryError::AccountAccess(error)) => {
                Err(map_account_access_error(error))
            }
            Err(kamu_resources_facade::ResourcesSummaryError::RemoteRequest(error)) => {
                Err(error.int_err().into())
            }
            Err(kamu_resources_facade::ResourcesSummaryError::Internal(error)) => Err(error.into()),
        }
    }

    /// Returns resources by refs
    #[tracing::instrument(level = "info", name = Resources_by_refs, skip_all, fields(selector_count = resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn by_refs(
        &self,
        ctx: &Context<'_>,
        resource_refs: Vec<ResourceRefInput>,
        #[graphql(default)] opts: SpecViewOptsInput,
    ) -> Result<BatchResourcesOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .get(
                into_resource_refs(resource_refs)?,
                opts.into_spec_view_opts(),
            )
            .await
        {
            Ok(response) => Ok(BatchResourcesOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedSelector(e)) => Ok(
                BatchResourcesOutcome::UnsupportedSelector(map_unsupported_selector_problem(e)),
            ),
            Err(kamu_resources_facade::BatchResourceError::AccountResolution(e)) => {
                Ok(BatchResourcesOutcome::AccountResolution(e.into()))
            }
            Err(e) => Err(map_batch_resource_error(e)),
        }
    }

    /// Returns resource handles by refs
    #[tracing::instrument(level = "info", name = Resources_handles_by_refs, skip_all, fields(selector_count = resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn handles_by_refs(
        &self,
        ctx: &Context<'_>,
        resource_refs: Vec<ResourceRefInput>,
    ) -> Result<BatchResourceHandlesOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .get_handles(into_resource_refs(resource_refs)?)
            .await
        {
            Ok(response) => Ok(BatchResourceHandlesOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedSelector(e)) => {
                Ok(BatchResourceHandlesOutcome::UnsupportedSelector(
                    map_unsupported_selector_problem(e),
                ))
            }
            Err(kamu_resources_facade::BatchResourceError::AccountResolution(e)) => {
                Ok(BatchResourceHandlesOutcome::AccountResolution(e.into()))
            }
            Err(e) => Err(map_batch_resource_error(e)),
        }
    }

    /// Returns resources matching the given selectors, which act as a logical
    /// OR and may span several resource types and accounts
    #[tracing::instrument(level = "info", name = Resources_by_selectors, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn by_selectors(
        &self,
        ctx: &Context<'_>,
        query: SearchResourcesInput,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceListOutcome> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .search(query.into_facade_request(PaginationOpts::from_page(page, per_page))?)
            .await
        {
            Ok(response) => {
                let total_count = response.total_count;
                let items = response
                    .items
                    .into_iter()
                    .map(ResourceSummary::from)
                    .collect();
                Ok(ResourceListOutcome::Success(ResourceConnection::new(
                    items,
                    page,
                    per_page,
                    total_count,
                )))
            }
            Err(kamu_resources_facade::ListResourcesError::UnsupportedSelector(error)) => Ok(
                ResourceListOutcome::UnsupportedSelector(map_unsupported_selector_problem(error)),
            ),
            Err(kamu_resources_facade::ListResourcesError::AccountResolution(error)) => {
                Ok(ResourceListOutcome::AccountResolution(error.into()))
            }
            Err(kamu_resources_facade::ListResourcesError::InvalidLabelFilter(error)) => {
                Ok(ResourceListOutcome::InvalidLabelFilter(error.into()))
            }
            Err(error) => Err(map_list_resources_error(error)),
        }
    }

    /// Returns resource handles matching the given selectors
    #[tracing::instrument(level = "info", name = Resources_handles_by_selectors, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn handles_by_selectors(
        &self,
        ctx: &Context<'_>,
        query: SearchResourcesInput,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceHandleListOutcome> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .search_handles(query.into_facade_request(PaginationOpts::from_page(page, per_page))?)
            .await
        {
            Ok(response) => {
                let total_count = response.total_count;
                let items = response
                    .items
                    .into_iter()
                    .map(ResourceHandle::from)
                    .collect();
                Ok(ResourceHandleListOutcome::Success(
                    ResourceHandleConnection::new(items, page, per_page, total_count),
                ))
            }
            Err(kamu_resources_facade::ListResourcesError::UnsupportedSelector(error)) => {
                Ok(ResourceHandleListOutcome::UnsupportedSelector(
                    map_unsupported_selector_problem(error),
                ))
            }
            Err(kamu_resources_facade::ListResourcesError::AccountResolution(error)) => {
                Ok(ResourceHandleListOutcome::AccountResolution(error.into()))
            }
            Err(kamu_resources_facade::ListResourcesError::InvalidLabelFilter(error)) => {
                Ok(ResourceHandleListOutcome::InvalidLabelFilter(error.into()))
            }
            Err(error) => Err(map_list_resources_error(error)),
        }
    }

    /// Renders canonical manifest representations from stored resources
    #[tracing::instrument(level = "info", name = Resources_render_manifests, skip_all, fields(selector_count = resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn render_manifests(
        &self,
        ctx: &Context<'_>,
        resource_refs: Vec<ResourceRefInput>,
        format: ResourceManifestFormat,
        #[graphql(default)] opts: SpecViewOptsInput,
    ) -> Result<BatchResourceManifestsOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade
            .render_manifests(
                into_resource_refs(resource_refs)?,
                format.into(),
                opts.into_spec_view_opts(),
            )
            .await
        {
            Ok(response) => Ok(BatchResourceManifestsOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedSelector(e)) => {
                Ok(BatchResourceManifestsOutcome::UnsupportedSelector(
                    map_unsupported_selector_problem(e),
                ))
            }
            Err(kamu_resources_facade::BatchResourceError::AccountResolution(e)) => {
                Ok(BatchResourceManifestsOutcome::AccountResolution(e.into()))
            }
            Err(e) => Err(map_batch_resource_error(e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourcesSummaryOutcome {
    Success(ResourcesSummary),
    AccountResolution(ResourceAccountResolutionProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum ResourceListOutcome {
    Success(ResourceConnection),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    AccountResolution(ResourceAccountResolutionProblem),
    InvalidLabelFilter(ResourceInvalidLabelFilterProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum ResourceHandleListOutcome {
    Success(ResourceHandleConnection),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    AccountResolution(ResourceAccountResolutionProblem),
    InvalidLabelFilter(ResourceInvalidLabelFilterProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_resource_error(error: kamu_resources_facade::BatchResourceError) -> GqlError {
    use kamu_resources_facade::BatchResourceError as E;

    match error {
        E::UnsupportedSelector(_) => GqlError::gql("Unsupported resource type selector"),
        E::AccountResolution(error) => GqlError::gql(error.to_string()),
        E::AccountAccess(error) => map_account_access_error(error),
        E::InvalidLabelFilter(error) => GqlError::gql(error.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_list_resources_error(error: kamu_resources_facade::ListResourcesError) -> GqlError {
    use kamu_resources_facade::ListResourcesError as E;

    match error {
        E::UnsupportedSelector(_) => GqlError::gql("Unsupported resource type selector"),
        E::AccountResolution(error) => GqlError::gql(error.to_string()),
        E::AccountAccess(error) => map_account_access_error(error),
        E::InvalidLabelFilter(error) => GqlError::gql(error.to_string()),
        E::UnrepresentableScope(error) => GqlError::gql(error.to_string()),
        E::UnsupportedSelectorField(error) => GqlError::gql(error.to_string()),
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Renders an account-access failure as an ordinary GraphQL error.
///
/// Authorization outcomes never become typed problems — see
/// [`ResourceAccountResolutionProblem`] for the input-validation counterpart.
pub(crate) fn map_account_access_error(
    error: kamu_resources_facade::ResourceAccountAccessError,
) -> GqlError {
    use kamu_resources_facade::ResourceAccountAccessError as E;

    match error {
        E::AnonymousSubject => GqlError::Access(odf::AccessError::Unauthenticated(
            "Anonymous subject cannot resolve a target account".into(),
        )),
        E::Access(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
