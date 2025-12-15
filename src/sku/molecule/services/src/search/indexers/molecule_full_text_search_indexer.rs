// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_molecule_domain::{
    MoleculeGlobalActivity,
    MoleculeViewDataRoomEntriesUseCase,
    MoleculeViewGlobalActivitiesUseCase,
    MoleculeViewGlobalAnnouncementsUseCase,
    MoleculeViewProjectsUseCase,
    molecule_activity_full_text_search_schema as activity_schema,
    molecule_announcement_full_text_search_schema as announcement_schema,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    molecule_project_full_text_search_schema as project_schema,
};
use kamu_search::{FullTextSearchRepository, FullTextUpdateOperation};

use super::super::molecule_full_text_search_schema_helpers as helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub(crate) struct MoleculeFullTextSearchIndexer {
    current_account: CurrentAccountSubject,
    molecule_view_projects_uc: Arc<dyn MoleculeViewProjectsUseCase>,
    molecule_view_data_room_entries_uc: Arc<dyn MoleculeViewDataRoomEntriesUseCase>,
    molecule_view_global_announcements_uc: Arc<dyn MoleculeViewGlobalAnnouncementsUseCase>,
    molecule_view_global_activities_uc: Arc<dyn MoleculeViewGlobalActivitiesUseCase>,
}

impl MoleculeFullTextSearchIndexer {
    const BULK_SIZE: usize = 500;

    fn organization_account(&self) -> &LoggedAccount {
        match self.current_account {
            CurrentAccountSubject::Logged(ref account) => account,
            _ => panic!("MoleculeFullTextSearchIndexer requires a logged account"),
        }
    }

    pub(crate) async fn index_projects(
        &self,
        repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        let organization_account = self.organization_account();

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            "Indexing projects for Molecule organization account",
        );

        let mut total_documents_count = 0;
        let mut operations = Vec::new();

        // Load all projects for the organization account
        let projects_listing = self
            .molecule_view_projects_uc
            .execute(organization_account, None)
            .await
            .int_err()?;

        // Index each project
        for project in projects_listing.list {
            // Serialize project into search document
            let document = helpers::index_project_from_entity(&project);

            // Note: for now, use IPNFT UID as document ID
            // This should be revised after implementing non-tokenized projects
            operations.push(FullTextUpdateOperation::Index {
                id: project.ipnft_uid.clone(),
                doc: document,
            });

            // Bulk index when we reach BULK_SIZE
            if operations.len() >= Self::BULK_SIZE {
                tracing::debug!(
                    documents_count = operations.len(),
                    "Bulk indexing projects batch",
                );
                repo.bulk_update(project_schema::SCHEMA_NAME, operations)
                    .await?;
                total_documents_count += Self::BULK_SIZE;
                operations = Vec::new();
            }
        }

        // Index remaining documents
        if !operations.is_empty() {
            let remaining_count = operations.len();
            tracing::debug!(
                documents_count = remaining_count,
                "Bulk indexing final projects batch",
            );
            repo.bulk_update(project_schema::SCHEMA_NAME, operations)
                .await?;
            total_documents_count += remaining_count;
        }

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            indexed_documents_count = total_documents_count,
            "Indexed projects for Molecule organization account",
        );

        Ok(total_documents_count)
    }

    pub(crate) async fn index_data_room_entries(
        &self,
        repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        let organization_account = self.organization_account();

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            "Indexing data room entries for Molecule organization account",
        );

        const PARALLEL_PROJECTS: usize = 10;
        let mut total_documents_count = 0;
        let mut operations = Vec::new();

        // Load all projects for the organization account
        let projects_listing = self
            .molecule_view_projects_uc
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
                let view_uc = Arc::clone(&self.molecule_view_data_room_entries_uc);

                futures.push(async move {
                    let result = view_uc
                        .execute(
                            &project_clone,
                            None, /* latest */
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
                    let document =
                        helpers::index_data_room_entry_from_entity(&project.ipnft_uid, &entry);

                    operations.push(FullTextUpdateOperation::Index {
                        id: data_room_entry_schema::unique_id_for_data_room_entry(
                            &project.ipnft_uid,
                            &entry.path,
                        ),
                        doc: document,
                    });

                    // Bulk index when we reach BULK_SIZE
                    if operations.len() >= Self::BULK_SIZE {
                        tracing::debug!(
                            documents_count = operations.len(),
                            "Bulk indexing data room entries batch",
                        );
                        repo.bulk_update(data_room_entry_schema::SCHEMA_NAME, operations)
                            .await?;
                        total_documents_count += Self::BULK_SIZE;
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

    pub(crate) async fn index_announcements(
        &self,
        repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        let organization_account = self.organization_account();

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            "Indexing global announcements for Molecule organization account",
        );

        let mut total_documents_count = 0;
        let mut operations = Vec::new();
        let mut offset = 0;

        loop {
            // Load announcements in pages aligned with bulk size
            let announcements_listing = self
                .molecule_view_global_announcements_uc
                .execute(
                    organization_account,
                    None, /* no filters */
                    Some(PaginationOpts {
                        limit: Self::BULK_SIZE,
                        offset,
                    }),
                )
                .await
                .int_err()?;

            // Break if no more announcements
            if announcements_listing.list.is_empty() {
                break;
            }

            // Index each announcement
            for global_announcement in announcements_listing.list {
                // Serialize announcement into search document
                let document = helpers::index_announcement_from_global_entity(&global_announcement);

                operations.push(FullTextUpdateOperation::Index {
                    id: global_announcement.announcement.announcement_id.to_string(),
                    doc: document,
                });
            }

            // Bulk index the current page
            if !operations.is_empty() {
                let batch_count = operations.len();
                tracing::debug!(
                    documents_count = batch_count,
                    "Bulk indexing announcements batch",
                );
                repo.bulk_update(announcement_schema::SCHEMA_NAME, operations)
                    .await?;
                total_documents_count += batch_count;
                operations = Vec::new();
            }

            // Move to next page
            offset += Self::BULK_SIZE;
        }

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            indexed_documents_count = total_documents_count,
            "Indexed global announcements for Molecule organization account",
        );

        Ok(total_documents_count)
    }

    pub(crate) async fn index_activities(
        &self,
        repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        let organization_account = self.organization_account();

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            "Indexing global activities for Molecule organization account",
        );

        let mut total_documents_count = 0;
        let mut operations = Vec::new();
        let mut offset = 0;

        loop {
            // Load activities in pages aligned with bulk size
            let activities_listing = self
                .molecule_view_global_activities_uc
                .execute(
                    organization_account,
                    None, /* no filters */
                    Some(PaginationOpts {
                        limit: Self::BULK_SIZE,
                        offset,
                    }),
                )
                .await
                .int_err()?;

            // Break if no more activities
            if activities_listing.list.is_empty() {
                break;
            }

            // Index each activity
            for activity in activities_listing.list {
                // Skip announcement activities
                let data_room_activity = match activity {
                    MoleculeGlobalActivity::DataRoomActivity(dr_activity) => dr_activity,
                    MoleculeGlobalActivity::Announcement(_) => continue,
                };

                // Serialize activity into search document
                let document = helpers::index_data_room_activity(&data_room_activity);

                // Generate unique ID using molecule_account_id and the activity's offset
                let id = activity_schema::unique_id_for_data_room_activity(
                    &organization_account.account_id,
                    data_room_activity.offset,
                );

                operations.push(FullTextUpdateOperation::Index { id, doc: document });
            }

            // Bulk index the current page
            if !operations.is_empty() {
                let batch_count = operations.len();
                tracing::debug!(
                    documents_count = batch_count,
                    "Bulk indexing activities batch",
                );
                repo.bulk_update(activity_schema::SCHEMA_NAME, operations)
                    .await?;
                total_documents_count += batch_count;
                operations = Vec::new();
            }

            // Move to next page
            offset += Self::BULK_SIZE;
        }

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            indexed_documents_count = total_documents_count,
            "Indexed global activities for Molecule organization account",
        );

        Ok(total_documents_count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
