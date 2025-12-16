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
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::{
    MoleculeDataRoomEntry,
    MoleculeViewDataRoomEntriesMode,
    MoleculeViewDataRoomEntriesUseCase,
    MoleculeViewProjectsUseCase,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
};
use kamu_search::{FullTextSearchRepository, FullTextUpdateOperation};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BULK_SIZE: usize = 500;
const PARALLEL_PROJECTS: usize = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn index_data_room_entry_from_entity(
    ipnft_uid: &str,
    entry: &MoleculeDataRoomEntry,
) -> serde_json::Value {
    serde_json::json!({
        data_room_entry_schema::FIELD_EVENT_TIME: entry.event_time,
        data_room_entry_schema::FIELD_SYSTEM_TIME: entry.system_time,
        data_room_entry_schema::FIELD_IPNFT_UID: ipnft_uid,
        data_room_entry_schema::FIELD_REF: entry.reference,
        data_room_entry_schema::FIELD_PATH: entry.path,
        data_room_entry_schema::FIELD_DEPTH: entry.path.depth(),
        data_room_entry_schema::FIELD_VERSION: entry.denormalized_latest_file_info.version,
        data_room_entry_schema::FIELD_CONTENT_TYPE: entry.denormalized_latest_file_info.content_type,
        data_room_entry_schema::FIELD_CONTENT_HASH: entry.denormalized_latest_file_info.content_hash,
        data_room_entry_schema::FIELD_CONTENT_LENGTH: entry.denormalized_latest_file_info.content_length,
        data_room_entry_schema::FIELD_ACCESS_LEVEL: entry.denormalized_latest_file_info.access_level,
        data_room_entry_schema::FIELD_CHANGE_BY: entry.denormalized_latest_file_info.change_by,
        data_room_entry_schema::FIELD_DESCRIPTION: entry.denormalized_latest_file_info.description,
        data_room_entry_schema::FIELD_CATEGORIES: entry.denormalized_latest_file_info.categories,
        data_room_entry_schema::FIELD_TAGS: entry.denormalized_latest_file_info.tags,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Indexing function
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_data_room_entries(
    organization_account: &LoggedAccount,
    catalog: &dill::Catalog,
    repo: &dyn FullTextSearchRepository,
) -> Result<usize, InternalError> {
    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        "Indexing data room entries for Molecule organization account",
    );

    let mut total_documents_count = 0;
    let mut operations = Vec::new();

    let molecule_view_projects_uc = catalog
        .get_one::<dyn MoleculeViewProjectsUseCase>()
        .unwrap();
    let molecule_view_data_room_entries_uc = catalog
        .get_one::<dyn MoleculeViewDataRoomEntriesUseCase>()
        .unwrap();

    // Load all projects for the organization account
    let projects_listing = molecule_view_projects_uc
        .execute(organization_account, None)
        .await
        .int_err()?;

    // Process projects in parallel batches
    use futures::stream::{FuturesUnordered, StreamExt};

    let project_chunks = projects_listing.list.chunks(PARALLEL_PROJECTS);
    for project_chunk in project_chunks {
        // Load data room entries for multiple projects in parallel
        let mut futures = FuturesUnordered::new();

        for project in project_chunk {
            let project_clone = project.clone();
            let view_uc = Arc::clone(&molecule_view_data_room_entries_uc);

            futures.push(async move {
                let result = view_uc
                    .execute(
                        &project_clone,
                        MoleculeViewDataRoomEntriesMode::LatestFromCollection,
                        None, /* all prefixes */
                        None, /* any depth */
                        None, /* no filters */
                        None, /* no pagination */
                    )
                    .await
                    .map_err(|e| {
                        tracing::warn!(
                            project_ipnft_uid = project_clone.ipnft_uid.as_str(),
                            error = ?e,
                            "Failed to load data room entries for project",
                        );
                        e
                    })
                    .int_err();

                (project_clone, result)
            });
        }

        // Process results as they complete
        while let Some((project, data_room_entries_result)) = futures.next().await {
            let data_room_entries = data_room_entries_result?;

            // Skip empty rooms
            if data_room_entries.list.is_empty() {
                continue;
            }

            // Prepare documents for indexing
            for entry in data_room_entries.list {
                let document = index_data_room_entry_from_entity(&project.ipnft_uid, &entry);

                operations.push(FullTextUpdateOperation::Index {
                    id: data_room_entry_schema::unique_id_for_data_room_entry(
                        &project.ipnft_uid,
                        &entry.path,
                    ),
                    doc: document,
                });

                // Bulk index when we reach BULK_SIZE
                if operations.len() >= BULK_SIZE {
                    tracing::debug!(
                        documents_count = operations.len(),
                        "Bulk indexing data room entries batch",
                    );
                    repo.bulk_update(data_room_entry_schema::SCHEMA_NAME, operations)
                        .await?;
                    total_documents_count += BULK_SIZE;
                    operations = Vec::new();
                }
            }
        }
    }

    // Index remaining documents
    if !operations.is_empty() {
        let remaining_count = operations.len();
        tracing::debug!(
            documents_count = remaining_count,
            "Bulk indexing final data room entries batch",
        );
        repo.bulk_update(data_room_entry_schema::SCHEMA_NAME, operations)
            .await?;
        total_documents_count += remaining_count;
    }

    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        indexed_documents_count = total_documents_count,
        "Indexed data room entries for Molecule organization account",
    );

    Ok(total_documents_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
