// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetEntryService,
    DatasetRegistry,
    DatasetRegistryExt,
    dataset_search_schema,
};
use kamu_search::*;

use super::dataset_search_schema_helpers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::SearchEntitySchemaProvider)]
pub struct DatasetSearchSchemaProvider {
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
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
        repo: Arc<dyn SearchRepository>,
        schema: &SearchEntitySchema,
    ) -> Result<usize, InternalError> {
        assert!(schema.schema_name == dataset_search_schema::SCHEMA_NAME);

        // Process all datasets in chunks

        const CHUNK_SIZE: usize = 500;

        let mut entries_stream = self.dataset_entry_service.all_entries();

        let mut operations = Vec::new();
        let mut total_indexed = 0;

        use futures::TryStreamExt;
        while let Some(entry) = entries_stream.try_next().await? {
            // Resolve dataset
            let dataset = self
                .dataset_registry
                .get_dataset_by_id(&entry.id)
                .await
                .int_err()?;

            // Index dataset
            let dataset_document = index_dataset_from_scratch(dataset, &entry.owner_id).await?;
            let dataset_document_json = serde_json::to_value(dataset_document).int_err()?;

            tracing::debug!(
                dataset_id = %entry.id,
                dataset_name = %entry.name,
                search_document = %dataset_document_json,
                "Indexed dataset search document",
            );

            // Add dataset document to the chunk
            operations.push(SearchIndexUpdateOperation::Index {
                id: entry.id.to_string(),
                doc: dataset_document_json,
            });

            // Index in chunks to avoid memory overwhelming
            if operations.len() >= CHUNK_SIZE {
                repo.bulk_update(dataset_search_schema::SCHEMA_NAME, operations)
                    .await?;
                total_indexed += CHUNK_SIZE;
                operations = Vec::new();
            }
        }

        // Index remaining documents
        if !operations.is_empty() {
            let remaining_count = operations.len();
            repo.bulk_update(dataset_search_schema::SCHEMA_NAME, operations)
                .await?;
            total_indexed += remaining_count;
        }

        Ok(total_indexed)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
