// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_core::ResolvedDataset;
use kamu_search::{
    FullTextSchemaField,
    FullTextSchemaFieldRole,
    FullTextSearchEntitySchema,
    FullTextSearchEntitySchemaUpgradeMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_dataset_from_scratch(
    dataset: ResolvedDataset,
    owner_id: &odf::AccountID,
) -> Result<serde_json::Value, InternalError> {
    // Find key blocks that contribute to full-text search
    let mut attachments_visitor = odf::dataset::SearchSetAttachmentsVisitor::new();
    let mut info_visitor = odf::dataset::SearchSetInfoVisitor::new();
    let mut schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();

    // Also need the seed event for created_at
    let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();

    use odf::dataset::*;
    let mut visitors: [&mut dyn MetadataChainVisitor<Error = Infallible>; 4] = [
        &mut attachments_visitor,
        &mut info_visitor,
        &mut schema_visitor,
        &mut seed_visitor,
    ];

    // Visit entire metadata chain
    dataset
        .as_metadata_chain()
        .accept(&mut visitors)
        .await
        .int_err()?;

    // Extract interested parts from visitors
    let schema_fields = extract_schema_field_names(schema_visitor);
    let (description, keywords) = extract_description_and_keywords(info_visitor);
    let attachments = extract_attachment_contents(attachments_visitor);

    // Seed event: created_at
    let seed_event_time = seed_visitor.into_block().unwrap().system_time;

    // Head event: updated_at
    let head_event_time = dataset
        .as_metadata_chain()
        .get_block_by_ref(&odf::BlockRef::Head)
        .await
        .int_err()?
        .system_time;

    // Prepare full text search document
    let alias = dataset.get_alias();
    Ok(serde_json::json!({
        FIELD_DATASET_NAME: alias.dataset_name.to_string(),
        FIELD_ALIAS: alias.to_string(),
        FIELD_OWNER_NAME: alias
            .account_name
            .as_ref()
            .map(std::string::ToString::to_string)
            .unwrap_or_else(|| DEFAULT_ACCOUNT_NAME_STR.to_string()),
        FIELD_OWNER_ID: owner_id.to_string(),
        FIELD_KIND: match dataset.get_kind() {
            odf::DatasetKind::Root => "root".to_string(),
            odf::DatasetKind::Derivative => "derivative".to_string(),
        },
        FIELD_CREATED_AT: seed_event_time.to_rfc3339(),
        FIELD_UPDATED_AT: head_event_time.to_rfc3339(),
        FIELD_SCHEMA_FIELDS: schema_fields,
        FIELD_DESCRIPTION: description,
        FIELD_KEYWORDS: keywords,
        FIELD_ATTACHMENTS: attachments,
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn partial_update_for_new_interval(
    dataset: ResolvedDataset,
    owner_id: &odf::AccountID,
    new_head: &odf::Multihash,
    maybe_prev_head: Option<&odf::Multihash>,
) -> Result<serde_json::Value, InternalError> {
    // Extract key blocks that contribute to full-text search
    let mut attachments_visitor = odf::dataset::SearchSetAttachmentsVisitor::new();
    let mut info_visitor = odf::dataset::SearchSetInfoVisitor::new();
    let mut schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();

    use odf::dataset::*;
    let mut visitors: [&mut dyn MetadataChainVisitor<Error = Infallible>; 3] = [
        &mut attachments_visitor,
        &mut info_visitor,
        &mut schema_visitor,
    ];

    // Only need to visit blocks between maybe_prev_head and new_head
    let result = dataset
        .as_metadata_chain()
        .accept_by_interval_ext(
            &mut visitors,
            Some(new_head),
            maybe_prev_head,
            AcceptByIntervalOptions {
                ignore_missing_tail: false, // Detect divergence errors
                ..Default::default()
            },
        )
        .await;

    // In case divergence is detected, fall back to full indexing
    match result {
        Ok(_) => {}
        Err(AcceptVisitorError::Traversal(IterBlocksError::InvalidInterval(_))) => {
            return index_dataset_from_scratch(dataset, owner_id).await;
        }
        Err(e) => return Err(e.int_err()),
    }

    // Extract interested parts from visitors
    let schema_fields = extract_schema_field_names(schema_visitor);
    let (description, keywords) = extract_description_and_keywords(info_visitor);
    let attachments = extract_attachment_contents(attachments_visitor);

    // New head event: updated_at
    // Note: ES should receive an update, even if other parts were not touched,
    //  otherwise ordering of datasets by updated_at would be broken
    let new_head_event_time = dataset
        .as_metadata_chain()
        .get_block(new_head)
        .await
        .int_err()?
        .system_time;

    // Prepare partial update to full text search document
    Ok(serde_json::json!({
        FIELD_SCHEMA_FIELDS: schema_fields,
        FIELD_DESCRIPTION: description,
        FIELD_KEYWORDS: keywords,
        FIELD_ATTACHMENTS: attachments,
        FIELD_UPDATED_AT: new_head_event_time.to_rfc3339(),
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_for_rename(
    new_alias: &odf::DatasetAlias,
    updated_at: DateTime<Utc>,
) -> serde_json::Value {
    serde_json::json!({
        FIELD_OWNER_NAME: new_alias.account_name.as_ref().map(ToString::to_string)
            .unwrap_or_else(|| DEFAULT_ACCOUNT_NAME_STR.to_string()),
        FIELD_DATASET_NAME: new_alias.dataset_name.to_string(),
        FIELD_ALIAS: new_alias.to_string(),
        FIELD_UPDATED_AT: updated_at.to_rfc3339(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_schema_field_names(
    schema_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetDataSchema>,
) -> Vec<String> {
    schema_visitor
        .into_event()
        .map(|e| {
            e.upgrade()
                .schema
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_description_and_keywords(
    info_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetInfo>,
) -> (Option<String>, Option<Vec<String>>) {
    info_visitor
        .into_event()
        .map(|e| (e.description, e.keywords))
        .unwrap_or_default()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_attachment_contents(
    attachments_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetAttachments>,
) -> Option<Vec<String>> {
    attachments_visitor
        .into_event()
        .map(|e| match e.attachments {
            odf::metadata::Attachments::Embedded(a) => {
                let items: Vec<String> = a.items.into_iter().map(|a| a.content).collect();
                if items.is_empty() { None } else { Some(items) }
            }
        })
        .unwrap_or(None)
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
