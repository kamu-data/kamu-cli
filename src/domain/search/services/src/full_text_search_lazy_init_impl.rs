// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Component, TypedBuilder};
use internal_error::*;
use kamu_search::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a temporary solution that initializes the search index upon the
/// first call.
pub struct FullTextSearchImplLazyInit {
    is_initialized: tokio::sync::OnceCell<()>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FullTextSearchService)]
#[dill::scope(dill::Singleton)]
impl FullTextSearchImplLazyInit {
    pub fn new() -> Self {
        Self {
            is_initialized: tokio::sync::OnceCell::new(),
        }
    }

    async fn maybe_init(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        self.is_initialized
            .get_or_try_init(async || self.init(catalog).await)
            .await?;
        Ok(())
    }

    async fn init(&self, catalog: &dill::Catalog) -> Result<(), InternalError> {
        let indexer = FullTextSearchIndexer::builder().get(catalog).int_err()?;

        use init_on_startup::InitOnStartup;
        indexer.run_initialization().await
    }

    async fn inner(
        &self,
        catalog: &dill::Catalog,
    ) -> Result<Arc<dyn FullTextSearchService>, InternalError> {
        self.maybe_init(catalog).await?;

        let inner = FullTextSearchServiceImpl::builder()
            .get(catalog)
            .int_err()?;

        Ok(inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FullTextSearchService for FullTextSearchImplLazyInit {
    async fn health(
        &self,
        ctx: FullTextSearchContext<'_>,
    ) -> Result<serde_json::Value, InternalError> {
        let inner = self.inner(ctx.catalog).await?;
        inner.health(ctx).await
    }

    async fn search(
        &self,
        ctx: FullTextSearchContext<'_>,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        let inner = self.inner(ctx.catalog).await?;
        inner.search(ctx, req).await
    }

    async fn index_bulk(
        &self,
        ctx: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        docs: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        let inner = self.inner(ctx.catalog).await?;
        inner.index_bulk(ctx, schema_name, docs).await
    }

    async fn update_bulk(
        &self,
        ctx: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        updates: Vec<(FullTextEntityId, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        let inner = self.inner(ctx.catalog).await?;
        inner.update_bulk(ctx, schema_name, updates).await
    }

    async fn delete_bulk(
        &self,
        ctx: FullTextSearchContext<'_>,
        schema_name: FullTextEntitySchemaName,
        ids: Vec<FullTextEntityId>,
    ) -> Result<(), InternalError> {
        let inner = self.inner(ctx.catalog).await?;
        inner.delete_bulk(ctx, schema_name, ids).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
