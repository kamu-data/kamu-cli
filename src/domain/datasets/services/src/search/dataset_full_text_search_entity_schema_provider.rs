// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_datasets::DatasetEntryService;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct DatasetFullTextSearchSchemaProvider {
    dataset_entry_service: Arc<dyn DatasetEntryService>,
}

#[async_trait::async_trait]
impl kamu_search::FullTextSearchEntitySchemaProvider for DatasetFullTextSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "dev.kamu.domain.datasets.DatasetFullTextSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::FullTextSearchEntitySchema] {
        &[DATASET_FULL_TEXT_SEARCH_ENTITY_SCHEMA]
    }

    async fn run_schema_initial_indexing(
        &self,
        repo: &dyn FullTextSearchRepository,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<usize, InternalError> {
        assert!(schema.entity_kind == FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET);

        const CHUNK_SIZE: usize = 500;

        let mut entries_stream = self.dataset_entry_service.all_entries();

        let mut dataset_documents = Vec::new();
        let mut total_indexed = 0;

        use futures::TryStreamExt;
        while let Some(entry) = entries_stream.try_next().await? {
            let dataset_document = serde_json::json!({
                FIELD_DATASET_NAME: entry.name.to_string(),
                FIELD_ALIAS: entry.alias().to_string(),
                FIELD_KIND: match entry.kind {
                    odf::DatasetKind::Root => "root",
                    odf::DatasetKind::Derivative => "derivative",
                },
                FIELD_OWNER_ID: entry.owner_id.to_string(),
                FIELD_CREATED_AT: entry.created_at.to_rfc3339(),
            });
            dataset_documents.push((entry.id.to_string(), dataset_document));

            // Index in chunks to avoid memory overwhelming
            if dataset_documents.len() >= CHUNK_SIZE {
                repo.index_bulk(FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET, dataset_documents)
                    .await?;
                total_indexed += CHUNK_SIZE;
                dataset_documents = Vec::new();
            }
        }

        // Index remaining documents
        if !dataset_documents.is_empty() {
            let remaining_count = dataset_documents.len();
            repo.index_bulk(FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET, dataset_documents)
                .await?;
            total_indexed += remaining_count;
        }

        Ok(total_indexed)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET: &str = "kamu-datasets";
const FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FIELD_DATASET_NAME: &str = "dataset_name";
const FIELD_ALIAS: &str = "alias";
const FIELD_KIND: &str = "kind";
const FIELD_OWNER_ID: &str = "owner_id";
const FIELD_CREATED_AT: &str = "created_at";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_DATASET_NAME,
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: true,
        filterable: false,
    },
    FullTextSchemaField {
        path: FIELD_ALIAS,
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: true,
        filterable: false,
    },
    FullTextSchemaField {
        path: FIELD_KIND,
        kind: FullTextSchemaFieldKind::Keyword,
        searchable: false,
        sortable: false,
        filterable: true,
    },
    FullTextSchemaField {
        path: FIELD_OWNER_ID,
        kind: FullTextSchemaFieldKind::Keyword,
        searchable: false,
        sortable: false,
        filterable: true,
    },
    FullTextSchemaField {
        path: FIELD_CREATED_AT,
        kind: FullTextSchemaFieldKind::DateTime,
        searchable: false,
        sortable: true,
        filterable: true,
    },
];

const DATASET_FULL_TEXT_SEARCH_ENTITY_SCHEMA: FullTextSearchEntitySchema =
    FullTextSearchEntitySchema {
        entity_kind: FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET,
        version: FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET_VERSION,
        fields: DATASET_FIELDS,
        upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    };

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
