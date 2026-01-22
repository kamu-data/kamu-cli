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
use kamu_core::KamuBackgroundCatalog;
use kamu_search::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a temporary solution that initializes the search index upon the
/// first call.
pub struct SearchImplLazyInit {
    background_catalog: Arc<KamuBackgroundCatalog>,
    is_initialized: tokio::sync::OnceCell<()>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn SearchService)]
#[dill::interface(dyn SearchIndexer)]
#[dill::scope(dill::Singleton)]
impl SearchImplLazyInit {
    pub fn new(background_catalog: Arc<KamuBackgroundCatalog>) -> Self {
        Self {
            background_catalog,
            is_initialized: tokio::sync::OnceCell::new(),
        }
    }

    async fn maybe_init(&self) -> Result<(), InternalError> {
        self.is_initialized
            .get_or_try_init(async || self.init().await)
            .await?;
        Ok(())
    }

    async fn init(&self) -> Result<(), InternalError> {
        let system_user_catalog = self.background_catalog.system_user_catalog();

        let indexer = SearchIndexerImpl::builder()
            .get(&system_user_catalog)
            .int_err()?;

        use init_on_startup::InitOnStartup;
        indexer.run_initialization().await
    }

    async fn inner(&self) -> Result<Arc<dyn SearchService>, InternalError> {
        self.maybe_init().await?;

        let inner = SearchServiceImpl::builder()
            .get(self.background_catalog.base_catalog())
            .int_err()?;

        Ok(inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchService for SearchImplLazyInit {
    async fn health(&self, ctx: SearchContext<'_>) -> Result<serde_json::Value, InternalError> {
        let inner = self.inner().await?;
        inner.health(ctx).await
    }

    async fn text_search(
        &self,
        ctx: SearchContext<'_>,
        req: TextSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        let inner = self.inner().await?;
        inner.text_search(ctx, req).await
    }

    async fn vector_search(
        &self,
        ctx: SearchContext<'_>,
        req: VectorSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        let inner = self.inner().await?;
        inner.vector_search(ctx, req).await
    }

    async fn hybrid_search(
        &self,
        ctx: SearchContext<'_>,
        req: HybridSearchRequest,
    ) -> Result<SearchResponse, InternalError> {
        let inner = self.inner().await?;
        inner.hybrid_search(ctx, req).await
    }

    async fn find_document_by_id(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        id: &SearchEntityId,
    ) -> Result<Option<serde_json::Value>, InternalError> {
        let inner = self.inner().await?;
        inner.find_document_by_id(ctx, schema_name, id).await
    }

    async fn bulk_update(
        &self,
        ctx: SearchContext<'_>,
        schema_name: SearchEntitySchemaName,
        operations: Vec<SearchIndexUpdateOperation>,
    ) -> Result<(), InternalError> {
        let inner = self.inner().await?;
        inner.bulk_update(ctx, schema_name, operations).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SearchIndexer for SearchImplLazyInit {
    async fn reset_search_indices(&self) -> Result<(), InternalError> {
        let system_user_catalog = self.background_catalog.system_user_catalog();

        let indexer = SearchIndexerImpl::builder()
            .get(&system_user_catalog)
            .int_err()?;

        indexer.reset_search_indices().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
