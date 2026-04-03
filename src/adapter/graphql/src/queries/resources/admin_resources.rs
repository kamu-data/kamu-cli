// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::helpers as resource_helpers;
use crate::prelude::*;
use crate::queries::{
    Resource,
    ResourceConnection,
    ResourceKindInput,
    ResourceManifestFormat,
    ResourceRenderManifestResult,
    ResourceSelectorInput,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AdminResources {
    of_account: kamu_accounts::Account,
}

impl AdminResources {
    const DEFAULT_PER_PAGE: usize = 15;

    pub fn from_account(of_account: kamu_accounts::Account) -> Self {
        Self { of_account }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AdminResources {
    /// Returns a resource by selector from the target account, if found
    #[tracing::instrument(level = "info", name = AdminResources_resource, skip_all)]
    async fn resource(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
    ) -> Result<Option<Resource>> {
        resource_helpers::get_resource(
            ctx,
            selector,
            Some(kamu_resources::ResourceManifestAccount {
                id: Some(self.of_account.id.clone()),
                name: None,
            }),
        )
        .await
    }

    /// Returns resources of the specified kind from the target account
    #[tracing::instrument(level = "info", name = AdminResources_list_by_kind, skip_all)]
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
            ctx,
            kind,
            Some(kamu_resources::ResourceManifestAccount {
                id: Some(self.of_account.id.clone()),
                name: None,
            }),
            page,
            per_page,
        )
        .await
    }

    /// Returns resources across all kinds from the target account
    #[tracing::instrument(level = "info", name = AdminResources_list_all, skip_all)]
    async fn list_all(
        &self,
        ctx: &Context<'_>,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<ResourceConnection> {
        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        resource_helpers::list_all_resources_connection(
            ctx,
            Some(kamu_resources::ResourceManifestAccount {
                id: Some(self.of_account.id.clone()),
                name: None,
            }),
            page,
            per_page,
        )
        .await
    }

    /// Renders a canonical manifest representation from a stored resource in
    /// the target account
    #[tracing::instrument(level = "info", name = AdminResources_render_manifest, skip_all)]
    async fn render_manifest(
        &self,
        ctx: &Context<'_>,
        selector: ResourceSelectorInput,
        format: ResourceManifestFormat,
    ) -> Result<ResourceRenderManifestResult> {
        let _ = (&self.of_account, ctx, selector, format);
        todo!("AdminResources.render_manifest is not implemented yet");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
