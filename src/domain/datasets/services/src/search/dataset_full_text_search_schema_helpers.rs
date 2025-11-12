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
use kamu_core::ResolvedDataset;
use kamu_datasets::dataset_full_text_search_schema as dataset_schema;
use kamu_search::*;

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

    // Head event: ref_changed_at
    let head_event_time = dataset
        .as_metadata_chain()
        .get_block_by_ref(&odf::BlockRef::Head)
        .await
        .int_err()?
        .system_time;

    // Convert FieldUpdate to Option for JSON serialization
    // For full indexing, Absent means the chain has no such event (use null)
    let schema_fields_value = match schema_fields {
        FullTextSearchFieldUpdate::Absent | FullTextSearchFieldUpdate::Empty => None,
        FullTextSearchFieldUpdate::Present(v) => Some(v),
    };
    let description_value = match description {
        FullTextSearchFieldUpdate::Absent | FullTextSearchFieldUpdate::Empty => None,
        FullTextSearchFieldUpdate::Present(v) => Some(v),
    };
    let keywords_value = match keywords {
        FullTextSearchFieldUpdate::Absent | FullTextSearchFieldUpdate::Empty => None,
        FullTextSearchFieldUpdate::Present(v) => Some(v),
    };
    let attachments_value = match attachments {
        FullTextSearchFieldUpdate::Absent | FullTextSearchFieldUpdate::Empty => None,
        FullTextSearchFieldUpdate::Present(v) => Some(v),
    };

    use dataset_schema::*;

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
            odf::DatasetKind::Root => FIELD_VALUE_KIND_ROOT.to_string(),
            odf::DatasetKind::Derivative => FIELD_VALUE_KIND_DERIVATIVE.to_string(),
        },
        FIELD_CREATED_AT: seed_event_time.to_rfc3339(),
        FIELD_REF_CHANGED_AT: head_event_time.to_rfc3339(),
        FIELD_SCHEMA_FIELDS: schema_fields_value,
        FIELD_DESCRIPTION: description_value,
        FIELD_KEYWORDS: keywords_value,
        FIELD_ATTACHMENTS: attachments_value,
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

    // New head event: ref_changed_at
    // Note: ES should receive an update, even if other parts were not touched,
    //  otherwise ordering of datasets by ref_changed_at would be broken
    let new_head_event_time = dataset
        .as_metadata_chain()
        .get_block(new_head)
        .await
        .int_err()?
        .system_time;

    use dataset_schema::*;

    // Prepare partial update to full text search document
    // Only include fields that were actually touched in the interval
    let mut update = serde_json::Map::from_iter([(
        FIELD_REF_CHANGED_AT.to_string(),
        serde_json::json!(new_head_event_time.to_rfc3339()),
    )]);

    insert_full_text_incremental_update_field(&mut update, FIELD_SCHEMA_FIELDS, schema_fields);
    insert_full_text_incremental_update_field(&mut update, FIELD_DESCRIPTION, description);
    insert_full_text_incremental_update_field(&mut update, FIELD_KEYWORDS, keywords);
    insert_full_text_incremental_update_field(&mut update, FIELD_ATTACHMENTS, attachments);

    Ok(serde_json::Value::Object(update))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn partial_update_for_rename(new_alias: &odf::DatasetAlias) -> serde_json::Value {
    use dataset_schema::*;
    serde_json::json!({
        FIELD_OWNER_NAME: new_alias.account_name.as_ref().map(ToString::to_string)
            .unwrap_or_else(|| DEFAULT_ACCOUNT_NAME_STR.to_string()),
        FIELD_DATASET_NAME: new_alias.dataset_name.to_string(),
        FIELD_ALIAS: new_alias.to_string(),
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_schema_field_names(
    schema_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetDataSchema>,
) -> FullTextSearchFieldUpdate<Vec<String>> {
    schema_visitor
        .into_event()
        .map(|e| {
            let schema_field_names: Vec<String> = e
                .upgrade()
                .schema
                .fields
                .iter()
                .map(|f| f.name.clone())
                .collect();
            if schema_field_names.is_empty() {
                FullTextSearchFieldUpdate::Empty
            } else {
                FullTextSearchFieldUpdate::Present(schema_field_names)
            }
        })
        .unwrap_or(FullTextSearchFieldUpdate::Absent)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_description_and_keywords(
    info_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetInfo>,
) -> (
    FullTextSearchFieldUpdate<String>,
    FullTextSearchFieldUpdate<Vec<String>>,
) {
    info_visitor
        .into_event()
        .map(|e| {
            let description_update = match e.description {
                None => FullTextSearchFieldUpdate::Empty,
                Some(s) if s.is_empty() => FullTextSearchFieldUpdate::Empty,
                Some(s) => FullTextSearchFieldUpdate::Present(s),
            };
            let keywords_update = match e.keywords {
                None => FullTextSearchFieldUpdate::Empty,
                Some(v) if v.is_empty() => FullTextSearchFieldUpdate::Empty,
                Some(v) => FullTextSearchFieldUpdate::Present(v),
            };
            (description_update, keywords_update)
        })
        .unwrap_or((
            FullTextSearchFieldUpdate::Absent,
            FullTextSearchFieldUpdate::Absent,
        ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn extract_attachment_contents(
    attachments_visitor: odf::dataset::SearchSingleTypedBlockVisitor<odf::metadata::SetAttachments>,
) -> FullTextSearchFieldUpdate<Vec<String>> {
    attachments_visitor
        .into_event()
        .map(|e| match e.attachments {
            odf::metadata::Attachments::Embedded(a) => {
                let items: Vec<String> = a.items.into_iter().map(|a| a.content).collect();
                if items.is_empty() {
                    FullTextSearchFieldUpdate::Empty
                } else {
                    FullTextSearchFieldUpdate::Present(items)
                }
            }
        })
        .unwrap_or(FullTextSearchFieldUpdate::Absent)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
