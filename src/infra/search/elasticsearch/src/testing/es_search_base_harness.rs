// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_search_services::{SearchIndexer, SearchServiceImpl};
use time_source::SystemTimeSourceStub;

use crate::testing::ElasticsearchTestContext;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchBaseHarness {
    fixed_time: DateTime<Utc>,
    es_ctx: Arc<ElasticsearchTestContext>,
    catalog: dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ElasticsearchBaseHarness {
    pub fn new(es_ctx: Arc<ElasticsearchTestContext>) -> Self {
        let mut b = dill::CatalogBuilder::new_chained(es_ctx.catalog());
        b.add::<SearchIndexer>()
            .add::<SearchServiceImpl>()
            .add::<SystemTimeSourceStub>();

        let catalog = b.build();

        let fixed_time = Utc::now();
        let time_source = catalog.get_one::<SystemTimeSourceStub>().unwrap();
        time_source.set(fixed_time);

        Self {
            fixed_time,
            es_ctx,
            catalog,
        }
    }

    pub async fn run_initial_indexing(catalog: &dill::Catalog) {
        use init_on_startup::InitOnStartup;
        let indexer = catalog.get_one::<SearchIndexer>().unwrap();
        indexer.run_initialization().await.unwrap();
    }

    #[inline]
    pub fn fixed_time(&self) -> DateTime<Utc> {
        self.fixed_time
    }

    #[inline]
    pub fn es_ctx(&self) -> &ElasticsearchTestContext {
        self.es_ctx.as_ref()
    }

    #[inline]
    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
