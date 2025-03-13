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
use kamu_search::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This is a temporary solution that initializes the search index upon the
/// first call.
pub struct SearchServiceLocalImplLazyInit {
    catalog: dill::Catalog,
    is_initialized: tokio::sync::OnceCell<()>,
}

#[dill::component(pub)]
#[dill::interface(dyn SearchServiceLocal)]
impl SearchServiceLocalImplLazyInit {
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
        let indexer = SearchServiceLocalIndexer::builder()
            .with_config(Arc::new(SearchServiceLocalIndexerConfig {
                clear_on_start: false,
            }))
            .get(&self.catalog)
            .int_err()?;

        indexer.run_initialization().await
    }
}

#[async_trait::async_trait]
impl SearchServiceLocal for SearchServiceLocalImplLazyInit {
    async fn search_natural_language(
        &self,
        prompt: &str,
        options: SearchNatLangOpts,
    ) -> Result<SearchLocalNatLangResult, SearchLocalNatLangError> {
        self.maybe_init().await?;

        let inner = SearchServiceLocalImpl::builder()
            .get(&self.catalog)
            .int_err()?;

        inner.search_natural_language(prompt, options).await
    }
}
