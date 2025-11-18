// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::ResolvedDataset;
use kamu_datasets::DatasetEntry;
use kamu_search::{
    FullTextSchemaField,
    FullTextSchemaFieldRole,
    FullTextSearchEntitySchema,
    FullTextSearchEntitySchemaUpgradeMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_dataset(
    dataset: ResolvedDataset,
    entry: &DatasetEntry,
) -> Result<DatasetSearchDocument, InternalError> {
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
        dataset_name: entry.name.to_string(),
        alias: entry.alias().to_string(),
        owner_name: entry.owner_name.to_string(),
        owner_id: entry.owner_id.to_string(),
        kind: match entry.kind {
            odf::DatasetKind::Root => "root".to_string(),
            odf::DatasetKind::Derivative => "derivative".to_string(),
        },
        created_at: entry.created_at.to_rfc3339(), // probably should be Seed event time?
        updated_at: entry.created_at.to_rfc3339(), // take time of Head
        schema_fields,
        description,
        keywords,
        attachments,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a dataset document for full-text search indexing.
#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
pub(crate) struct DatasetSearchDocument {
    dataset_name: String,
    alias: String,
    owner_name: String,
    owner_id: String,
    kind: String,
    created_at: String,
    updated_at: String,
    schema_fields: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    keywords: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attachments: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET: &str = "kamu-datasets";
pub(crate) const FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FIELD_DATASET_NAME: &str = "dataset_name";
const FIELD_ALIAS: &str = "alias";
const FIELD_OWNER_NAME: &str = "owner_name";
const FIELD_OWNER_ID: &str = "owner_id";
const FIELD_KIND: &str = "kind";
const FIELD_CREATED_AT: &str = "created_at";
const FIELD_UPDATED_AT: &str = "updated_at";
const FIELD_SCHEMA_FIELDS: &str = "schema_fields";
const FIELD_DESCRIPTION: &str = "description";
const FIELD_KEYWORDS: &str = "keywords";
const FIELD_ATTACHMENTS: &str = "attachments";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: FIELD_DATASET_NAME,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_ALIAS,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: false,
            enable_inner_ngrams: false,
        },
    },
    FullTextSchemaField {
        path: FIELD_OWNER_NAME,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: true,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_OWNER_ID,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_KIND,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_CREATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_UPDATED_AT,
        role: FullTextSchemaFieldRole::DateTime,
    },
    FullTextSchemaField {
        path: FIELD_SCHEMA_FIELDS,
        role: FullTextSchemaFieldRole::Identifier {
            hierarchical: false,
            enable_edge_ngrams: true,
            enable_inner_ngrams: true,
        },
    },
    FullTextSchemaField {
        path: FIELD_DESCRIPTION,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: false, // short prose
        },
    },
    FullTextSchemaField {
        path: FIELD_KEYWORDS,
        role: FullTextSchemaFieldRole::Keyword,
    },
    FullTextSchemaField {
        path: FIELD_ATTACHMENTS,
        role: FullTextSchemaFieldRole::Prose {
            enable_positions: true, // long prose
        },
    },
];

pub(crate) const DATASET_FULL_TEXT_SEARCH_ENTITY_SCHEMA: FullTextSearchEntitySchema =
    FullTextSearchEntitySchema {
        entity_kind: FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET,
        version: FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET_VERSION,
        fields: DATASET_FIELDS,
        upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    };

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
