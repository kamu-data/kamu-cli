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
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::*;
use kamu_search::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a temporary solution that initializes the search index upon the
/// first call.
pub struct FullTextSearchImplLazyInit {
    catalog: dill::Catalog,
    is_initialized: tokio::sync::OnceCell<()>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn FullTextSearchService)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.FullTextSearchImplLazyInit",
    depends_on: &[],
    requires_transaction: false,
})]
impl FullTextSearchImplLazyInit {
    pub fn new(catalog: dill::Catalog) -> Self {
        Self {
            catalog,
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
        let indexer = FullTextSearchIndexer::builder()
            .get(&self.catalog)
            .int_err()?;

        indexer.run_initialization().await
    }

    async fn inner(&self) -> Result<Arc<dyn FullTextSearchService>, InternalError> {
        self.maybe_init().await?;

        let inner = FullTextSearchServiceImpl::builder()
            .get(&self.catalog)
            .int_err()?;

        Ok(inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FullTextSearchService for FullTextSearchImplLazyInit {
    async fn health(&self) -> Result<serde_json::Value, InternalError> {
        let inner = self.inner().await?;
        inner.health().await
    }

    async fn register_entity_schema(
        &self,
        entity: FullTextSearchEntitySchema,
    ) -> Result<(), InternalError> {
        let inner = self.inner().await?;
        inner.register_entity_schema(entity).await
    }

    async fn index_bulk(
        &self,
        kind: FullTextEntityKind,
        docs: Vec<(String, serde_json::Value)>,
    ) -> Result<(), InternalError> {
        let inner = self.inner().await?;
        inner.index_bulk(kind, docs).await
    }

    async fn delete_bulk(
        &self,
        kind: FullTextEntityKind,
        ids: Vec<String>,
    ) -> Result<(), InternalError> {
        let inner = self.inner().await?;
        inner.delete_bulk(kind, ids).await
    }

    async fn search(
        &self,
        ctx: &FullTextSearchContext,
        req: FullTextSearchRequest,
    ) -> Result<FullTextSearchResponse, InternalError> {
        let inner = self.inner().await?;
        inner.search(ctx, req).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl InitOnStartup for FullTextSearchImplLazyInit {
    #[tracing::instrument(level = "debug", name = FullTextSearchImplLazyInit_run_initialization, skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Note: we only neeed this to capture the correct system catalog,
        //  and not the one that may be passed in the context of the first search call.
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
