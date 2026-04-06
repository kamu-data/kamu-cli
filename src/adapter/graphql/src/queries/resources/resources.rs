// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::helpers as resource_helpers;
use crate::LoggedInGuard;
use crate::prelude::*;
use crate::queries::{
    Resource,
    ResourceConnection,
    ResourceKindDescriptor,
    ResourceKindInput,
    ResourceManifestFormat,
    ResourceRenderManifestResult,
    ResourceSelectorInput,
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
        resource_helpers::list_supported_resource_kinds(ctx).await
    }

    /// Returns a resource by selector, if found
    #[tracing::instrument(level = "info", name = Resources_resource, skip_all, fields(?selector))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn resource(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<Option<Resource>> {
        resource_helpers::get_resource(ctx, selector, None /* current subject */).await
    }

    /// Returns resources of the specified kind
    #[tracing::instrument(level = "info", name = Resources_list_by_kind, skip_all, fields(?kind, ?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_by_kind(
        &self,
        ctx: &Context<'_>,
        kind: ResourceKindInput,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        resource_helpers::list_resources_connection(
            ctx, kind, None, /* current subject */
            page, per_page,
        )
        .await
    }

    /// Returns resources across all kinds
    #[tracing::instrument(level = "info", name = Resources_list_all, skip_all, fields(?page, ?per_page))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn list_all(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        resource_helpers::list_all_resources_connection(
            ctx, None, /* current subject */
            page, per_page,
        )
        .await
    }

    /// Renders a canonical manifest representation from a stored resource
    #[tracing::instrument(level = "info", name = Resources_render_manifest, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn render_manifest(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
        format: ResourceManifestFormat,
    ) -> Result<ResourceRenderManifestResult> {
        resource_helpers::render_resource_manifest(
            ctx, selector, format, None, /* current subject */
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
