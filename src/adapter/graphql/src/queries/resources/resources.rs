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
    Resource,
    ResourceBadAccountProblem,
    ResourceConnection,
    ResourceHandle,
    ResourceHandleConnection,
    ResourceInvalidLabelFilterProblem,
    ResourceManifestFormat,
    ResourceRefInput,
    ResourceRenderManifestResult,
    ResourceSelectorProblem,
    ResourceSelectorProblemResult,
    ResourceSummary,
    ResourceTypeDescriptor,
    ResourceUnsupportedSelectorProblem,
    ResourcesSummary,
    SearchResourceHandlesInput,
    SearchResourcesInput,
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
            Err(kamu_resources_facade::ResourcesSummaryError::BadAccount(error)) => Ok(
                ResourcesSummaryOutcome::BadAccount(map_bad_account_problem(error)?),
            ),
            Err(kamu_resources_facade::ResourcesSummaryError::RemoteRequest(error)) => {
                Err(error.int_err().into())
            }
            Err(kamu_resources_facade::ResourcesSummaryError::Internal(error)) => Err(error.into()),
        }
    }

    /// Returns a resource by selector, if found
    #[tracing::instrument(level = "info", name = Resources_resource, skip_all, fields(?resource_ref))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource(
        &self,
        ctx: &Context<'_>,
        resource_ref: ResourceRefInput,
        #[graphql(default)] revealed: bool,
    ) -> Result<ResourceGetOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        match resource_facade
            .get(resource_ref.try_into()?, spec_view_mode)
            .await
        {
            Ok(resource) => Ok(ResourceGetOutcome::Success(resource.into())),
            Err(kamu_resources_facade::GetResourceError::LookupProblem(problem)) => {
                Ok(ResourceGetOutcome::Problem(problem.into()))
            }
            Err(kamu_resources_facade::GetResourceError::UnsupportedSelector(error)) => {
                Ok(ResourceGetOutcome::Problem(error.try_into()?))
            }
            Err(kamu_resources_facade::GetResourceError::BadAccount(error)) => {
                Ok(ResourceGetOutcome::Problem(ResourceSelectorProblemResult {
                    problem: ResourceSelectorProblem::BadAccount(map_bad_account_problem(error)?),
                }))
            }
            Err(error) => Err(map_get_resource_non_lookup_error(error)),
        }
    }

    /// Returns resources by selectors
    #[tracing::instrument(level = "info", name = Resources_resources, skip_all, fields(selector_count = resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resources(
        &self,
        ctx: &Context<'_>,
        resource_refs: Vec<ResourceRefInput>,
        #[graphql(default)] revealed: bool,
    ) -> Result<BatchResourcesOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        match resource_facade
            .get_many(into_resource_refs(resource_refs)?, spec_view_mode)
            .await
        {
            Ok(response) => Ok(BatchResourcesOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedSelector(e)) => Ok(
                BatchResourcesOutcome::UnsupportedSelector(map_unsupported_selector_problem(e)),
            ),
            Err(kamu_resources_facade::BatchResourceError::BadAccount(e)) => Ok(
                BatchResourcesOutcome::BadAccount(map_bad_account_problem(e)?),
            ),
            Err(e) => Err(map_batch_resource_error(e)),
        }
    }

    /// Returns resource handle by selector, if found
    #[tracing::instrument(level = "info", name = Resources_resource_handle, skip_all, fields(?resource_ref))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource_handle(
        &self,
        ctx: &Context<'_>,
        resource_ref: ResourceRefInput,
    ) -> Result<ResourceGetHandleOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        match resource_facade.get_handle(resource_ref.try_into()?).await {
            Ok(handle) => Ok(ResourceGetHandleOutcome::Success(handle.into())),
            Err(kamu_resources_facade::GetResourceError::LookupProblem(problem)) => {
                Ok(ResourceGetHandleOutcome::Problem(problem.into()))
            }
            Err(kamu_resources_facade::GetResourceError::UnsupportedSelector(error)) => {
                Ok(ResourceGetHandleOutcome::Problem(error.try_into()?))
            }
            Err(kamu_resources_facade::GetResourceError::BadAccount(error)) => Ok(
                ResourceGetHandleOutcome::Problem(ResourceSelectorProblemResult {
                    problem: ResourceSelectorProblem::BadAccount(map_bad_account_problem(error)?),
                }),
            ),
            Err(error) => Err(map_get_resource_non_lookup_error(error)),
        }
    }

    /// Returns resource handles by selectors
    #[tracing::instrument(level = "info", name = Resources_resource_handles, skip_all, fields(selector_count = resource_refs.len()))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource_handles(
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
            Err(kamu_resources_facade::BatchResourceError::BadAccount(e)) => Ok(
                BatchResourceHandlesOutcome::BadAccount(map_bad_account_problem(e)?),
            ),
            Err(e) => Err(map_batch_resource_error(e)),
        }
    }

    /// Searches resources matching the given selectors, which act as a logical
    /// OR and may span several resource types and accounts
    #[tracing::instrument(level = "info", name = Resources_search, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn search(
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
            Err(kamu_resources_facade::ListResourcesError::BadAccount(error)) => Ok(
                ResourceListOutcome::BadAccount(map_bad_account_problem(error)?),
            ),
            Err(kamu_resources_facade::ListResourcesError::InvalidLabelFilter(error)) => {
                Ok(ResourceListOutcome::InvalidLabelFilter(error.into()))
            }
            Err(error) => Err(map_list_resources_error(error)),
        }
    }

    /// Searches resource handles matching the given selectors
    #[tracing::instrument(level = "info", name = Resources_search_handles, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn search_handles(
        &self,
        ctx: &Context<'_>,
        query: SearchResourceHandlesInput,
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
            Err(kamu_resources_facade::ListResourcesError::BadAccount(error)) => Ok(
                ResourceHandleListOutcome::BadAccount(map_bad_account_problem(error)?),
            ),
            Err(kamu_resources_facade::ListResourcesError::InvalidLabelFilter(error)) => {
                Ok(ResourceHandleListOutcome::InvalidLabelFilter(error.into()))
            }
            Err(error) => Err(map_list_resources_error(error)),
        }
    }

    /// Renders a canonical manifest representation from a stored resource
    #[tracing::instrument(level = "info", name = Resources_render_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn render_manifest(
        &self,
        ctx: &Context<'_>,
        resource_ref: ResourceRefInput,
        format: ResourceManifestFormat,
        #[graphql(default)] revealed: bool,
    ) -> Result<ResourceRenderManifestOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        match resource_facade
            .render_manifest(resource_ref.try_into()?, format.into(), spec_view_mode)
            .await
        {
            Ok(rendered) => Ok(ResourceRenderManifestOutcome::Success(
                ResourceRenderManifestResult {
                    manifest: rendered.manifest,
                    format: rendered.format.into(),
                },
            )),
            Err(kamu_resources_facade::RenderResourceManifestError::LookupProblem(problem)) => {
                Ok(ResourceRenderManifestOutcome::Problem(problem.into()))
            }
            Err(kamu_resources_facade::RenderResourceManifestError::UnsupportedSelector(error)) => {
                Ok(ResourceRenderManifestOutcome::Problem(error.try_into()?))
            }
            Err(kamu_resources_facade::RenderResourceManifestError::BadAccount(error)) => Ok(
                ResourceRenderManifestOutcome::Problem(ResourceSelectorProblemResult {
                    problem: ResourceSelectorProblem::BadAccount(map_bad_account_problem(error)?),
                }),
            ),
            Err(error) => Err(map_render_resource_manifest_error(error)),
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
        #[graphql(default)] revealed: bool,
    ) -> Result<BatchResourceManifestsOutcome> {
        let resource_facade = from_catalog_n!(ctx, dyn kamu_resources_facade::ResourceFacade);

        let spec_view_mode = Self::spec_view_mode_from_revealed(revealed);

        match resource_facade
            .render_manifests(
                into_resource_refs(resource_refs)?,
                format.into(),
                spec_view_mode,
            )
            .await
        {
            Ok(response) => Ok(BatchResourceManifestsOutcome::Success(response.into())),
            Err(kamu_resources_facade::BatchResourceError::UnsupportedSelector(e)) => {
                Ok(BatchResourceManifestsOutcome::UnsupportedSelector(
                    map_unsupported_selector_problem(e),
                ))
            }
            Err(kamu_resources_facade::BatchResourceError::BadAccount(e)) => Ok(
                BatchResourceManifestsOutcome::BadAccount(map_bad_account_problem(e)?),
            ),
            Err(e) => Err(map_batch_resource_error(e)),
        }
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

fn map_get_resource_non_lookup_error(error: kamu_resources_facade::GetResourceError) -> GqlError {
    use kamu_resources_facade::GetResourceError as E;

    match error {
        E::LookupProblem(_) | E::UnsupportedSelector(_) | E::BadAccount(_) => {
            unreachable!("handled as union arm")
        }
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourcesSummaryOutcome {
    Success(ResourcesSummary),
    BadAccount(ResourceBadAccountProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum ResourceListOutcome {
    Success(ResourceConnection),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    BadAccount(ResourceBadAccountProblem),
    InvalidLabelFilter(ResourceInvalidLabelFilterProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub enum ResourceHandleListOutcome {
    Success(ResourceHandleConnection),
    UnsupportedSelector(ResourceUnsupportedSelectorProblem),
    BadAccount(ResourceBadAccountProblem),
    InvalidLabelFilter(ResourceInvalidLabelFilterProblem),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceGetOutcome {
    Success(Resource),
    Problem(ResourceSelectorProblemResult),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceGetHandleOutcome {
    Success(ResourceHandle),
    Problem(ResourceSelectorProblemResult),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_render_resource_manifest_error(
    error: kamu_resources_facade::RenderResourceManifestError,
) -> GqlError {
    use kamu_resources_facade::RenderResourceManifestError as E;

    match error {
        E::UnsupportedSelector(_) | E::BadAccount(_) | E::LookupProblem(_) => {
            unreachable!("handled as union arm")
        }
        E::RemoteRequest(error) => error.int_err().into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union, Debug, Clone)]
pub enum ResourceRenderManifestOutcome {
    Success(ResourceRenderManifestResult),
    Problem(ResourceSelectorProblemResult),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_batch_resource_error(error: kamu_resources_facade::BatchResourceError) -> GqlError {
    use kamu_resources_facade::BatchResourceError as E;

    match error {
        E::UnsupportedSelector(_) => GqlError::gql("Unsupported resource type selector"),
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
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
        E::BadAccount(error) => map_resolve_manifest_account_error(error),
        E::InvalidLabelFilter(error) => GqlError::gql(error.to_string()),
        E::UnrepresentableScope(error) => GqlError::gql(error.to_string()),
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
        | E::SelectorMismatch { .. }
        | E::AccountNotFoundById(_)
        | E::AccountNotFoundByName(_) => GqlError::gql(error.to_string()),
        E::Access(error) => error.into(),
        E::Internal(error) => error.into(),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn map_bad_account_problem(
    error: kamu_resources_facade::ResolveManifestAccountError,
) -> Result<ResourceBadAccountProblem> {
    use kamu_resources_facade::ResolveManifestAccountError as E;

    match error {
        E::EmptySelector
        | E::SelectorMismatch { .. }
        | E::AccountNotFoundById(_)
        | E::AccountNotFoundByName(_) => Ok(error.into()),
        E::AnonymousSubject | E::Access(_) | E::Internal(_) => {
            Err(map_resolve_manifest_account_error(error))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
