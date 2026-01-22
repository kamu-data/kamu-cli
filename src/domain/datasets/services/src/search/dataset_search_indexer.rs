// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_datasets::{DatasetEntryService, DatasetRegistry, ResolvedDataset, dataset_search_schema};
use kamu_search::*;
use odf::dataset::MetadataChainExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all, fields(
    dataset_id = %dataset.get_id(),
    head = %head,
))]
pub(crate) async fn index_dataset_from_scratch(
    indexer_config: &SearchIndexerConfig,
    dataset: ResolvedDataset,
    owner_id: &odf::AccountID,
    head: &odf::Multihash,
) -> Result<Option<serde_json::Value>, InternalError> {
    // Find key blocks that contribute to search
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

    use dataset_search_schema::*;

    // Visit entire metadata chain
    match dataset
        .as_metadata_chain()
        .accept_by_hash(&mut visitors, head)
        .await
    {
        Ok(_) => {
            // Extract interested parts from visitors
            let schema_fields = extract_schema_field_names(schema_visitor);
            let (description, keywords) = extract_description_and_keywords(info_visitor);
            let attachments = extract_attachment_contents(attachments_visitor);

            // Apply indexing configuration filters
            if indexer_config.skip_datasets_with_no_data && !schema_fields.is_present() {
                return Ok(None);
            }
            if indexer_config.skip_datasets_with_no_description
                && !description.is_present()
                && !attachments.is_present()
            {
                return Ok(None);
            }

            // Seed event: created_at
            let seed_event_time = seed_visitor.into_block().unwrap().system_time;

            // Head event: ref_changed_at
            let head_event_time = dataset
                .as_metadata_chain()
                .get_block_by_ref(&odf::BlockRef::Head)
                .await
                .int_err()?
                .system_time;

            // Prepare full text search document with mandatory fields
            let alias = dataset.get_alias();
            let mut index_doc: serde_json::Value = serde_json::json!({
                fields::DATASET_NAME: alias.dataset_name.to_string(),
                fields::ALIAS: alias.to_string(),
                fields::OWNER_NAME: alias
                    .account_name
                    .as_ref()
                    .map(std::string::ToString::to_string)
                    .unwrap_or_else(|| DEFAULT_ACCOUNT_NAME_STR.to_string()),
                fields::OWNER_ID: owner_id.to_string(),
                fields::KIND: match dataset.get_kind() {
                    odf::DatasetKind::Root => fields::values::KIND_ROOT.to_string(),
                    odf::DatasetKind::Derivative => fields::values::KIND_DERIVATIVE.to_string(),
                },
                fields::CREATED_AT: seed_event_time.to_rfc3339(),
                fields::REF_CHANGED_AT: head_event_time.to_rfc3339(),
            });

            // Add optional fields only if present
            let index_doc_mut = index_doc.as_object_mut().unwrap();
            if let SearchFieldUpdate::Present(v) = schema_fields {
                index_doc_mut.insert(fields::SCHEMA_FIELDS.to_string(), serde_json::json!(v));
            }
            if let SearchFieldUpdate::Present(v) = description {
                index_doc_mut.insert(fields::DESCRIPTION.to_string(), serde_json::json!(v));
            }
            if let SearchFieldUpdate::Present(v) = keywords {
                index_doc_mut.insert(fields::KEYWORDS.to_string(), serde_json::json!(v));
            }
            if let SearchFieldUpdate::Present(v) = attachments {
                index_doc_mut.insert(fields::ATTACHMENTS.to_string(), serde_json::json!(v));
            }

            Ok(Some(index_doc))
        }

        Err(e) => Err(e.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tracing::instrument(level = "debug", skip_all, fields(
    dataset_id = %dataset.get_id(),
    new_head = %new_head,
    maybe_prev_head = ?maybe_prev_head,
))]
pub(crate) async fn partial_update_for_new_interval(
    indexer_config: &SearchIndexerConfig,
    dataset: ResolvedDataset,
    owner_id: &odf::AccountID,
    new_head: &odf::Multihash,
    maybe_prev_head: Option<&odf::Multihash>,
) -> Result<Option<serde_json::Value>, InternalError> {
    // Extract key blocks that contribute to search
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
            return index_dataset_from_scratch(indexer_config, dataset, owner_id, new_head).await;
        }
        Err(e) => return Err(e.int_err()),
    }

    // Extract interested parts from visitors
    let schema_fields = extract_schema_field_names(schema_visitor);
    let (description, keywords) = extract_description_and_keywords(info_visitor);
    let attachments = extract_attachment_contents(attachments_visitor);

    // New head event: ref_changed_at
    // Note: ES should receive an update, even if other parts were not touched,
    //  otherwise ordering of datasets by ref_changed_at would be broken
    let new_head_event_time = dataset
        .as_metadata_chain()
        .get_block(new_head)
        .await
        .int_err()?
        .system_time;

    use dataset_search_schema::*;

    // Prepare partial update to full text search document
    // Only include fields that were actually touched in the interval
    let mut update = serde_json::Map::from_iter([(
        fields::REF_CHANGED_AT.to_string(),
        serde_json::json!(new_head_event_time.to_rfc3339()),
    )]);

    insert_search_incremental_update_field(&mut update, fields::SCHEMA_FIELDS, schema_fields);
    insert_search_incremental_update_field(&mut update, fields::DESCRIPTION, description);
    insert_search_incremental_update_field(&mut update, fields::KEYWORDS, keywords);
    insert_search_incremental_update_field(&mut update, fields::ATTACHMENTS, attachments);

    Ok(Some(serde_json::Value::Object(update)))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_for_rename(new_alias: &odf::DatasetAlias) -> serde_json::Value {
    use dataset_search_schema::*;
    serde_json::json!({
        fields::OWNER_NAME: new_alias.account_name.as_ref().map(ToString::to_string)
            .unwrap_or_else(|| DEFAULT_ACCOUNT_NAME_STR.to_string()),
        fields::DATASET_NAME: new_alias.dataset_name.to_string(),
        fields::ALIAS: new_alias.to_string(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_schema_field_names(
    schema_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetDataSchema>,
) -> SearchFieldUpdate<Vec<String>> {
    schema_visitor
        .into_event()
        .map(|e| {
            let schema_field_names: Vec<String> = e
                .upgrade()
                .schema
                .fields
                .iter()
                .filter(|f| {
                    // Only include fields that are not default vocabulary fields
                    ![
                        odf::metadata::DatasetVocabulary::DEFAULT_OFFSET_COLUMN_NAME,
                        odf::metadata::DatasetVocabulary::DEFAULT_OPERATION_TYPE_COLUMN_NAME,
                        odf::metadata::DatasetVocabulary::DEFAULT_SYSTEM_TIME_COLUMN_NAME,
                        odf::metadata::DatasetVocabulary::DEFAULT_EVENT_TIME_COLUMN_NAME,
                    ]
                    .contains(&f.name.as_str())
                })
                .map(|f| f.name.clone())
                .collect();
            if schema_field_names.is_empty() {
                SearchFieldUpdate::Empty
            } else {
                SearchFieldUpdate::Present(schema_field_names)
            }
        })
        .unwrap_or(SearchFieldUpdate::Absent)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_description_and_keywords(
    info_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetInfo>,
) -> (SearchFieldUpdate<String>, SearchFieldUpdate<Vec<String>>) {
    info_visitor
        .into_event()
        .map(|e| {
            let description_update = match e.description {
                None => SearchFieldUpdate::Empty,
                Some(s) if s.is_empty() => SearchFieldUpdate::Empty,
                Some(s) => SearchFieldUpdate::Present(s),
            };
            let keywords_update = match e.keywords {
                None => SearchFieldUpdate::Empty,
                Some(v) if v.is_empty() => SearchFieldUpdate::Empty,
                Some(v) => SearchFieldUpdate::Present(v),
            };
            (description_update, keywords_update)
        })
        .unwrap_or((SearchFieldUpdate::Absent, SearchFieldUpdate::Absent))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_attachment_contents(
    attachments_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetAttachments>,
) -> SearchFieldUpdate<Vec<String>> {
    attachments_visitor
        .into_event()
        .map(|e| match e.attachments {
            odf::metadata::Attachments::Embedded(a) => {
                let items: Vec<String> = a.items.into_iter().map(|a| a.content).collect();
                if items.is_empty() {
                    SearchFieldUpdate::Empty
                } else {
                    SearchFieldUpdate::Present(items)
                }
            }
        })
        .unwrap_or(SearchFieldUpdate::Absent)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_datasets(
    dataset_entry_service: &dyn DatasetEntryService,
    dataset_registry: &dyn DatasetRegistry,
    search_repo: &dyn SearchRepository,
    indexer_config: &SearchIndexerConfig,
) -> Result<usize, InternalError> {
    const BULK_SIZE: usize = 500;

    let mut entries_stream = dataset_entry_service.all_entries();

    let mut operations = Vec::new();
    let mut total_indexed = 0;

    use futures::TryStreamExt;
    use kamu_datasets::DatasetRegistryExt;
    while let Some(entry) = entries_stream.try_next().await? {
        // Resolve dataset
        let dataset = dataset_registry
            .get_dataset_by_id(&entry.id)
            .await
            .int_err()?;

        // Resolve HEAD
        let maybe_head = dataset
            .as_metadata_chain()
            .try_get_ref(&odf::BlockRef::Head)
            .await
            .int_err()?;
        let Some(head) = maybe_head else {
            tracing::warn!(
                dataset_id = %entry.id,
                dataset_name = %entry.name,
                "Dataset has no HEAD, skipping indexing",
            );
            continue;
        };

        // Index dataset
        let maybe_dataset_document =
            index_dataset_from_scratch(indexer_config, dataset, &entry.owner_id, &head).await?;
        if let Some(dataset_document) = maybe_dataset_document {
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
        }

        // Index in chunks to avoid memory overwhelming
        if operations.len() >= BULK_SIZE {
            search_repo
                .bulk_update(dataset_search_schema::SCHEMA_NAME, operations)
                .await?;
            total_indexed += BULK_SIZE;
            operations = Vec::new();
        }
    }

    // Index remaining documents
    if !operations.is_empty() {
        let remaining_count = operations.len();
        search_repo
            .bulk_update(dataset_search_schema::SCHEMA_NAME, operations)
            .await?;
        total_indexed += remaining_count;
    }

    Ok(total_indexed)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
