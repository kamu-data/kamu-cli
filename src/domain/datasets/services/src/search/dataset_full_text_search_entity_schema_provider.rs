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
use kamu_core::DatasetRegistryExt;
use kamu_datasets::{DatasetEntry, DatasetEntryService};
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct DatasetFullTextSearchSchemaProvider {
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dataset_registry: Arc<dyn kamu_core::DatasetRegistry>,
}

impl DatasetFullTextSearchSchemaProvider {
    async fn index_dataset(
        &self,
        entry: &DatasetEntry,
    ) -> Result<DatasetSearchDocument, InternalError> {
        // Resolve dataset
        let dataset = self
            .dataset_registry
            .get_dataset_by_id(&entry.id)
            .await
            .int_err()?;

        // Extract key blocks that contribute to full-text search
        let mut attachments_visitor = odf::dataset::SearchSetAttachmentsVisitor::new();
        let mut info_visitor = odf::dataset::SearchSetInfoVisitor::new();
        let mut schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();
        let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();

        let mut visitors: [&mut dyn odf::dataset::MetadataChainVisitor<Error = odf::dataset::Infallible>;
            4] = [
            &mut attachments_visitor,
            &mut info_visitor,
            &mut schema_visitor,
            &mut seed_visitor,
        ];

        use odf::dataset::MetadataChainExt as _;
        dataset
            .as_metadata_chain()
            .accept(&mut visitors)
            .await
            .int_err()?;

        // Schema: field names
        // Note: should we omit system columns (system_time, event_time, op, offset)?
        let schema_fields = schema_visitor
            .into_event()
            .map(|e| {
                e.upgrade()
                    .schema
                    .fields
                    .iter()
                    .map(|f| f.name.clone())
                    .collect()
            })
            .unwrap_or_default();

        // Info: description, keywords
        let (description, keywords) = info_visitor
            .into_event()
            .map(|e| (e.description, e.keywords))
            .unwrap_or_default();

        // Attachments: contents
        let attachments = attachments_visitor
            .into_event()
            .map(|a| match a.attachments {
                odf::metadata::Attachments::Embedded(a) => {
                    let items: Vec<String> = a.items.into_iter().map(|a| a.content).collect();
                    if items.is_empty() { None } else { Some(items) }
                }
            })
            .unwrap_or(None);

        Ok(DatasetSearchDocument {
            name: entry.name.to_string(),
            alias: entry.alias().to_string(),
            kind: match entry.kind {
                odf::DatasetKind::Root => "root".to_string(),
                odf::DatasetKind::Derivative => "derivative".to_string(),
            },
            owner_id: entry.owner_id.to_string(),
            created_at: entry.created_at.to_rfc3339(), // probably should be Seed event time?
            schema_fields,
            description,
            keywords,
            attachments,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

        // Process all datasets in chunks

        const CHUNK_SIZE: usize = 500;

        let mut entries_stream = self.dataset_entry_service.all_entries();

        let mut dataset_documents = Vec::new();
        let mut total_indexed = 0;

        use futures::TryStreamExt;
        while let Some(entry) = entries_stream.try_next().await? {
            // Index dataset
            let dataset_document = self.index_dataset(&entry).await?;
            let dataset_document_json = serde_json::to_value(dataset_document).int_err()?;

            tracing::debug!(
                dataset_id = %entry.id,
                dataset_name = %entry.name,
                search_document = %dataset_document_json,
                "Indexed dataset search document",
            );

            // Add dataset document to the chunk
            dataset_documents.push((entry.id.to_string(), dataset_document_json));

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

/// Represents a dataset document for full-text search indexing.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct DatasetSearchDocument {
    name: String,
    alias: String,
    kind: String,
    owner_id: String,
    created_at: String,
    schema_fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachments: Option<Vec<String>>,
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
const FIELD_SCHEMA_FIELDS: &str = "schema_fields";
const FIELD_DESCRIPTION: &str = "description";
const FIELD_KEYWORDS: &str = "keywords";
const FIELD_ATTACHMENTS: &str = "attachments";

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
    FullTextSchemaField {
        path: FIELD_SCHEMA_FIELDS,
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: false,
        filterable: false,
    },
    FullTextSchemaField {
        path: FIELD_DESCRIPTION,
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: false,
        filterable: false,
    },
    FullTextSchemaField {
        path: FIELD_KEYWORDS,
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: false,
        filterable: false,
    },
    FullTextSchemaField {
        path: FIELD_ATTACHMENTS,
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: false,
        filterable: false,
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
