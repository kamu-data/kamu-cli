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
use init_on_startup::InitOnStartup;
use internal_error::*;
use kamu_core::KamuBackgroundCatalog;
use kamu_search::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a temporary solution that initializes the search index upon the
/// first call.
pub struct NaturalLanguageSearchImplLazyInit {
    background_catalog: Arc<KamuBackgroundCatalog>,
    is_initialized: tokio::sync::OnceCell<()>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn NaturalLanguageSearchService)]
#[dill::scope(dill::Singleton)]
impl NaturalLanguageSearchImplLazyInit {
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

        let indexer = NaturalLanguageSearchIndexer::builder()
            .get(&system_user_catalog)
            .int_err()?;

        indexer.run_initialization().await
    }

    async fn inner(
        &self,
        ctx: SearchContext<'_>,
    ) -> Result<Arc<dyn NaturalLanguageSearchService>, InternalError> {
        self.maybe_init().await?;

        let inner = NaturalLanguageSearchServiceImpl::builder()
            .get(ctx.catalog)
            .int_err()?;

        Ok(inner)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl NaturalLanguageSearchService for NaturalLanguageSearchImplLazyInit {
    async fn search_natural_language(
        &self,
        ctx: SearchContext<'_>,
        prompt: &str,
        options: SearchNatLangOpts,
    ) -> Result<SearchNatLangResult, SearchNatLangError> {
        let inner = self.inner(ctx).await?;
        inner.search_natural_language(ctx, prompt, options).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
