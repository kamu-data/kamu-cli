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
use kamu_accounts::{AccountService, LoggedAccount};
use kamu_search::*;

use super::molecule_full_text_search_schema_helpers as helpers;
use crate::domain::{
    MoleculeViewProjectsUseCase,
    molecule_activity_full_text_search_schema as activity_schema,
    molecule_announcement_full_text_search_schema as announcement_schema,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    molecule_project_full_text_search_schema as project_schema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct MoleculeFullTextSearchSchemaProvider {
    account_service: Arc<dyn AccountService>,
    molecule_view_projects: Arc<dyn MoleculeViewProjectsUseCase>,
}

#[common_macros::method_names_consts]
impl MoleculeFullTextSearchSchemaProvider {
    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_projects,
        skip_all,
    )]
    async fn index_projects(
        &self,
        repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // Index projects from all possible molecule organization accounts,
        // and return total count of indexed projects
        let mut total_indexed = 0;
        for account_name in crate::domain::MOLECULE_ORG_ACCOUNTS {
            total_indexed += self
                .index_projects_from_org_account(account_name, repo)
                .await?;
        }
        Ok(total_indexed)
    }

    async fn index_projects_from_org_account(
        &self,
        org_account_name: &str,
        repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // Resolve organization account, if exists
        let maybe_org_account = self
            .account_service
            .account_by_name(&odf::AccountName::try_from(org_account_name).unwrap())
            .await?;

        let Some(org_account) = maybe_org_account else {
            tracing::warn!(
                organization_account_name = org_account_name,
                "Organization account not found when indexing projects",
            );
            return Ok(0);
        };

        tracing::info!(
            organization_account_name = org_account.account_name.as_str(),
            organization_account_id = org_account.id.to_string(),
            "Indexing projects for Molecule organization account",
        );

        // Load all projects for the organization account
        let projects_listing = self
            .molecule_view_projects
            .execute(
                &LoggedAccount {
                    account_id: org_account.id.clone(),
                    account_name: org_account.account_name.clone(),
                },
                None,
            )
            .await
            .int_err()?;

        // Index each project
        let mut documents_by_id = Vec::new();
        for project in projects_listing.projects {
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
            organization_account_name = org_account.account_name.as_str(),
            organization_account_id = org_account.id.to_string(),
            indexed_documents_count = documents_count,
            "Indexed projects for Molecule organization account",
        );

        Ok(documents_count)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_data_room_entries,
        skip_all,
    )]
    async fn index_data_room_entries(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO: Implement indexing logic for molecule data room entries
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(0)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_announcements,
        skip_all,
    )]
    async fn index_announcements(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO: Implement indexing logic for molecule announcements
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(0)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_activities,
        skip_all,
    )]
    async fn index_activities(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO: Implement indexing logic for molecule activities
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu_search::FullTextSearchEntitySchemaProvider for MoleculeFullTextSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "xyz.molecule.kamu.MoleculeFullTextSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::FullTextSearchEntitySchema] {
        &[
            activity_schema::SCHEMA,
            announcement_schema::SCHEMA,
            data_room_entry_schema::SCHEMA,
            project_schema::SCHEMA,
        ]
    }

    async fn run_schema_initial_indexing(
        &self,
        repo: &dyn FullTextSearchRepository,
        schema: &FullTextSearchEntitySchema,
    ) -> Result<usize, InternalError> {
        let indexed_documents_count = match schema.schema_name {
            activity_schema::SCHEMA_NAME => self.index_activities(repo).await?,
            announcement_schema::SCHEMA_NAME => self.index_announcements(repo).await?,
            data_room_entry_schema::SCHEMA_NAME => self.index_data_room_entries(repo).await?,
            project_schema::SCHEMA_NAME => self.index_projects(repo).await?,

            _ => {
                return Err(InternalError::new(format!(
                    "Unsupported schema: {}",
                    schema.schema_name
                )));
            }
        };

        Ok(indexed_documents_count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
