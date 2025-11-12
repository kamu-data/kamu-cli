// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Component, TypedBuilder};
use init_on_startup::InitOnStartup;
use internal_error::*;
use kamu_search::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a temporary solution that initializes the search index upon the
/// first call.
pub struct NaturalLanguageSearchImplLazyInit {
    catalog: dill::Catalog,
    is_initialized: tokio::sync::OnceCell<()>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn NaturalLanguageSearchService)]
impl NaturalLanguageSearchImplLazyInit {
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
        let indexer = NaturalLanguageSearchIndexer::builder()
            .get(&self.catalog)
            .int_err()?;

        indexer.run_initialization().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl NaturalLanguageSearchService for NaturalLanguageSearchImplLazyInit {
    async fn search_natural_language(
        &self,
        prompt: &str,
        options: SearchNatLangOpts,
    ) -> Result<SearchNatLangResult, SearchNatLangError> {
        self.maybe_init().await?;

        let inner = NaturalLanguageSearchServiceImpl::builder()
            .get(&self.catalog)
            .int_err()?;

        inner.search_natural_language(prompt, options).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
