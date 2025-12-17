// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::LoggedAccount;
use kamu_molecule_domain::{
    MoleculeDataRoomEntriesListing,
    MoleculeDataRoomEntry,
    MoleculeProject,
    MoleculeReadVersionedFileEntryUseCase,
    MoleculeVersionedFileEntry,
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
    content_text: Option<&String>,
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
        data_room_entry_schema::FIELD_CONTENT_TEXT: content_text,
        data_room_entry_schema::FIELD_ACCESS_LEVEL: entry.denormalized_latest_file_info.access_level,
        data_room_entry_schema::FIELD_CHANGE_BY: entry.denormalized_latest_file_info.change_by,
        data_room_entry_schema::FIELD_DESCRIPTION: entry.denormalized_latest_file_info.description,
        data_room_entry_schema::FIELD_CATEGORIES: entry.denormalized_latest_file_info.categories,
        data_room_entry_schema::FIELD_TAGS: entry.denormalized_latest_file_info.tags,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Main indexing function
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

    let dependencies = IndexingDependencies::from_catalog(catalog);

    // Load all projects for the organization account
    let projects_listing = dependencies
        .molecule_view_projects_uc
        .execute(organization_account, None)
        .await
        .int_err()?;

    let total_documents_count =
        process_projects_in_batches(&projects_listing.list, &dependencies, repo).await?;

    tracing::info!(
        organization_account_name = organization_account.account_name.as_str(),
        organization_account_id = organization_account.account_id.to_string(),
        indexed_documents_count = total_documents_count,
        "Indexed data room entries for Molecule organization account",
    );

    Ok(total_documents_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct IndexingDependencies {
    molecule_view_projects_uc: Arc<dyn MoleculeViewProjectsUseCase>,
    molecule_view_data_room_entries_uc: Arc<dyn MoleculeViewDataRoomEntriesUseCase>,
    molecule_read_versioned_file_entry_uc: Arc<dyn MoleculeReadVersionedFileEntryUseCase>,
}

impl IndexingDependencies {
    fn from_catalog(catalog: &dill::Catalog) -> Self {
        Self {
            molecule_view_projects_uc: catalog
                .get_one::<dyn MoleculeViewProjectsUseCase>()
                .unwrap(),
            molecule_view_data_room_entries_uc: catalog
                .get_one::<dyn MoleculeViewDataRoomEntriesUseCase>()
                .unwrap(),
            molecule_read_versioned_file_entry_uc: catalog
                .get_one::<dyn MoleculeReadVersionedFileEntryUseCase>()
                .unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ProjectIndexingData {
    project: MoleculeProject,
    entries_result: Result<MoleculeDataRoomEntriesListing, InternalError>,
    versioned_files_map: HashMap<odf::DatasetID, MoleculeVersionedFileEntry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn process_projects_in_batches(
    projects: &[MoleculeProject],
    dependencies: &IndexingDependencies,
    repo: &dyn FullTextSearchRepository,
) -> Result<usize, InternalError> {
    use futures::stream::{FuturesUnordered, StreamExt};

    let mut total_documents_count = 0;
    let mut operations = Vec::new();

    // Load data room entries for multiple projects in parallel
    let project_chunks = projects.chunks(PARALLEL_PROJECTS);
    for project_chunk in project_chunks {
        let mut futures = FuturesUnordered::new();
        for project in project_chunk {
            futures.push(load_project_indexing_data(
                project.clone(),
                dependencies.clone(),
            ));
        }

        // Process results as they complete
        while let Some(project_data) = futures.next().await {
            index_project_data(project_data, &mut operations)?;

            // Bulk index when we reach or exceed BULK_SIZE
            if operations.len() >= BULK_SIZE {
                let batch_size = operations.len();
                tracing::debug!(
                    documents_count = batch_size,
                    "Bulk indexing data room entries batch",
                );
                repo.bulk_update(
                    data_room_entry_schema::SCHEMA_NAME,
                    std::mem::take(&mut operations),
                )
                .await?;
                total_documents_count += batch_size;
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

    Ok(total_documents_count)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_project_indexing_data(
    project: MoleculeProject,
    dependencies: IndexingDependencies,
) -> ProjectIndexingData {
    // Load current data room entries for the project
    let result = dependencies
        .molecule_view_data_room_entries_uc
        .execute(
            &project,
            MoleculeViewDataRoomEntriesMode::LatestSource,
            None, /* all prefixes */
            None, /* any depth */
            None, /* no filters */
            None, /* no pagination */
        )
        .await
        .map_err(|e| {
            tracing::warn!(
                project_ipnft_uid = project.ipnft_uid.as_str(),
                error = ?e,
                "Failed to load data room entries for project",
            );
            e
        })
        .int_err();

    let versioned_files_map = if let Ok(ref entries_listing) = result {
        load_versioned_files_for_entries(entries_listing, &project.ipnft_uid, &dependencies).await
    } else {
        HashMap::new()
    };

    ProjectIndexingData {
        project,
        entries_result: result,
        versioned_files_map,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn load_versioned_files_for_entries(
    entries_listing: &MoleculeDataRoomEntriesListing,
    project_ipnft_uid: &str,
    dependencies: &IndexingDependencies,
) -> HashMap<odf::DatasetID, MoleculeVersionedFileEntry> {
    use futures::stream::{FuturesUnordered, StreamExt};

    // Write down file+version requests we need to make
    let file_requests: Vec<_> = entries_listing
        .list
        .iter()
        .map(|entry| {
            (
                entry.reference.clone(),
                entry.denormalized_latest_file_info.version,
            )
        })
        .collect();

    // Load versioned file entries in parallel
    let mut file_futures = FuturesUnordered::new();
    for (reference, version) in file_requests {
        let molecule_read_versioned_file_entry_uc =
            Arc::clone(&dependencies.molecule_read_versioned_file_entry_uc);
        let ipnft_uid_for_trace = project_ipnft_uid.to_string();

        file_futures.push(async move {
            let versioned_file = molecule_read_versioned_file_entry_uc
                .execute(&reference, Some(version), None)
                .await
                .map_err(|e| {
                    tracing::error!(
                        project_ipnft_uid = %ipnft_uid_for_trace,
                        reference = %reference,
                        version = version,
                        error = ?e,
                        "Failed to load versioned file entry matching the data room entry",
                    );
                    e
                })
                .ok()
                .flatten();
            (reference, versioned_file)
        });
    }

    // Collect results into a map
    let mut versioned_files_map = HashMap::new();
    while let Some((reference, versioned_file)) = file_futures.next().await {
        if let Some(file) = versioned_file {
            versioned_files_map.insert(reference, file);
        }
    }
    versioned_files_map
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn index_project_data(
    project_data: ProjectIndexingData,
    operations: &mut Vec<FullTextUpdateOperation>,
) -> Result<(), InternalError> {
    let data_room_entries = project_data.entries_result?;

    // Skip empty rooms
    if data_room_entries.list.is_empty() {
        return Ok(());
    }

    // Prepare documents for indexing
    for entry in data_room_entries.list {
        let content_text = project_data
            .versioned_files_map
            .get(&entry.reference)
            .and_then(|versioned_file| versioned_file.detailed_info.content_text.as_ref());

        let document = index_data_room_entry_from_entity(
            &project_data.project.ipnft_uid,
            &entry,
            content_text,
        );

        operations.push(FullTextUpdateOperation::Index {
            id: data_room_entry_schema::unique_id_for_data_room_entry(
                &project_data.project.ipnft_uid,
                &entry.path,
            ),
            doc: document,
        });
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
