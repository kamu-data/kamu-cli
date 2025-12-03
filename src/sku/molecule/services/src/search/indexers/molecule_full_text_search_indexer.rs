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
use kamu_accounts::{CurrentAccountSubject, LoggedAccount};
use kamu_search::FullTextSearchRepository;

use super::super::molecule_full_text_search_schema_helpers as helpers;
use crate::domain::{
    MoleculeViewDataRoomEntriesUseCase,
    MoleculeViewProjectsUseCase,
    // molecule_activity_full_text_search_schema as activity_schema,
    // molecule_announcement_full_text_search_schema as announcement_schema,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    molecule_project_full_text_search_schema as project_schema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub(crate) struct MoleculeFullTextSearchIndexer {
    current_account: CurrentAccountSubject,
    molecule_view_projects_uc: Arc<dyn MoleculeViewProjectsUseCase>,
    molecule_view_data_room_entries_uc: Arc<dyn MoleculeViewDataRoomEntriesUseCase>,
}

impl MoleculeFullTextSearchIndexer {
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

        // Load all projects for the organization account
        let projects_listing = self
            .molecule_view_projects_uc
            .execute(organization_account, None)
            .await
            .int_err()?;

        // Index each project
        let mut documents_by_id = Vec::new();
        for project in projects_listing.list {
            // Serialize project into search document
            let document = helpers::index_project_from_entity(&project);

            // Note: for now, use IPNFT UID as document ID
            // This should be revised after implementing non-tokenized projects
            documents_by_id.push((project.ipnft_uid.clone(), document));
        }

        // Write documents to the index
        let documents_count = documents_by_id.len();
        repo.index_bulk(project_schema::SCHEMA_NAME, documents_by_id)
            .await?;

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            indexed_documents_count = documents_count,
            "Indexed projects for Molecule organization account",
        );

        Ok(documents_count)
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

        let mut total_documents_count = 0;

        // Load all projects for the organization account
        let projects_listing = self
            .molecule_view_projects_uc
            .execute(organization_account, None)
            .await
            .int_err()?;

        for project in projects_listing.list {
            // Load data room entries for the project
            let data_room_entries = self
                .molecule_view_data_room_entries_uc
                .execute(
                    &project, None, /* latest */
                    None, /* all prefixes */
                    None, /* any depth */
                    None, /* no pagination */
                )
                .await
                .map_err(|e| {
                    println!(
                        "Failed to load data room entries for project {}: {:?}",
                        project.ipnft_uid, e
                    );
                    e
                })
                .int_err()?;

            // Skip empty rooms
            if data_room_entries.list.is_empty() {
                continue;
            }

            // Prepare documents for indexing
            let mut documents_by_id = Vec::new();
            for entry in data_room_entries.list {
                let document = helpers::index_data_room_entry_from_entity(&project, &entry);

                // Synthesize document ID as "<ipnft_uid>:<entry_path>"
                let entry_document_id = format!("{}:{}", project.ipnft_uid, entry.path);
                documents_by_id.push((entry_document_id, document));
            }

            // Write documents of this project to the index
            let documents_count = documents_by_id.len();

            tracing::debug!(
                project_ipnft_uid = project.ipnft_uid.as_str(),
                indexed_documents_count = documents_count,
                "Indexing data room entries for Molecule project",
            );
            repo.index_bulk(data_room_entry_schema::SCHEMA_NAME, documents_by_id)
                .await?;

            total_documents_count += documents_count;
        }

        tracing::info!(
            organization_account_name = organization_account.account_name.as_str(),
            organization_account_id = organization_account.account_id.to_string(),
            indexed_documents_count = total_documents_count,
            "Indexed data room entries for Molecule organization account",
        );

        Ok(total_documents_count)
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn index_announcements(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO
        Ok(0)
    }

    #[allow(clippy::unused_async)]
    pub(crate) async fn index_activities(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO
        Ok(0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
