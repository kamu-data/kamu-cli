// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method4;
use internal_error::InternalError;
use kamu_auth_rebac::RebacService;
use kamu_datasets::{DatasetEntryService, DatasetRegistry, dataset_search_schema};
use kamu_search::*;

use crate::search::dataset_search_indexer::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::SearchEntitySchemaProvider)]
pub struct DatasetSearchSchemaProvider {
    catalog: dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetSearchSchemaProvider {
    #[transactional_method4(
        dataset_entry_service: Arc<dyn DatasetEntryService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        rebac_service: Arc<dyn RebacService>,
        embeddings_provider: Arc<dyn EmbeddingsProvider>
    )]
    async fn index_datasets(
        &self,
        search_repo: Arc<dyn SearchRepository>,
    ) -> Result<usize, InternalError> {
        index_datasets(
            dataset_entry_service.as_ref(),
            dataset_registry.as_ref(),
            search_repo.as_ref(),
            embeddings_provider.as_ref(),
            rebac_service.as_ref(),
        )
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu_search::SearchEntitySchemaProvider for DatasetSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "dev.kamu.domain.datasets.DatasetSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::SearchEntitySchema] {
        &[dataset_search_schema::SCHEMA]
    }

    async fn run_schema_initial_indexing(
        &self,
        search_repo: Arc<dyn SearchRepository>,
        schema: &SearchEntitySchema,
    ) -> Result<usize, InternalError> {
        assert!(schema.schema_name == dataset_search_schema::SCHEMA_NAME);
        self.index_datasets(search_repo).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
