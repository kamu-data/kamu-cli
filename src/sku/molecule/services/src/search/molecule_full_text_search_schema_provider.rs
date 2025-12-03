// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use internal_error::InternalError;
use kamu_accounts::{AccountService, CurrentAccountSubject};
use kamu_core::KamuBackgroundCatalog;
use kamu_search::*;

use crate::MoleculeFullTextSearchIndexer;
use crate::domain::{
    molecule_activity_full_text_search_schema as activity_schema,
    molecule_announcement_full_text_search_schema as announcement_schema,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    molecule_project_full_text_search_schema as project_schema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct MoleculeFullTextSearchSchemaProvider {
    background_catalog: Arc<KamuBackgroundCatalog>,
    account_service: Arc<dyn AccountService>,
}

#[common_macros::method_names_consts]
impl MoleculeFullTextSearchSchemaProvider {
    /// Common template for indexing entities across all molecule organization
    /// accounts. Takes a callback that performs the actual indexing for a
    /// specific entity type.
    async fn index_across_organizations<F, Fut>(
        &self,
        repo: Arc<dyn FullTextSearchRepository>,
        index_fn: F,
    ) -> Result<usize, InternalError>
    where
        F: Fn(Arc<MoleculeFullTextSearchIndexer>, Arc<dyn FullTextSearchRepository>) -> Fut,
        Fut: std::future::Future<Output = Result<usize, InternalError>>,
    {
        let mut total_indexed = 0;
        for org_account_name in crate::domain::MOLECULE_ORG_ACCOUNTS {
            // Resolve organization account, if exists
            let maybe_org_account = self
                .account_service
                .account_by_name(&odf::AccountName::try_from(org_account_name).unwrap())
                .await?;

            // Skip if organization account not found
            let Some(org_account) = maybe_org_account else {
                tracing::warn!(
                    organization_account_name = org_account_name,
                    "Organization account not found when indexing",
                );
                continue;
            };

            // Create catalog scoped to organization account
            let org_account_catalog =
                dill::CatalogBuilder::new_chained(self.background_catalog.catalog())
                    .add_value(CurrentAccountSubject::logged(
                        org_account.id,
                        org_account.account_name,
                    ))
                    .add::<MoleculeFullTextSearchIndexer>()
                    .build();

            // Run indexing on behalf of organization account under a dedicated transaction
            let count = DatabaseTransactionRunner::new(org_account_catalog)
                .transactional(|transaction_catalog| {
                    let index_fn = &index_fn;
                    let repo = repo.clone();
                    async move {
                        let indexer = transaction_catalog
                            .get_one::<MoleculeFullTextSearchIndexer>()
                            .unwrap();
                        index_fn(indexer, repo).await
                    }
                })
                .await?;

            total_indexed += count;
        }

        Ok(total_indexed)
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_projects,
        skip_all,
    )]
    async fn index_projects(
        &self,
        repo: Arc<dyn FullTextSearchRepository>,
    ) -> Result<usize, InternalError> {
        self.index_across_organizations(repo, |indexer, repo| async move {
            indexer.index_projects(&*repo).await
        })
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_data_room_entries,
        skip_all,
    )]
    async fn index_data_room_entries(
        &self,
        repo: Arc<dyn FullTextSearchRepository>,
    ) -> Result<usize, InternalError> {
        self.index_across_organizations(repo, |indexer, repo| async move {
            indexer.index_data_room_entries(&*repo).await
        })
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_announcements,
        skip_all,
    )]
    async fn index_announcements(
        &self,
        repo: Arc<dyn FullTextSearchRepository>,
    ) -> Result<usize, InternalError> {
        self.index_across_organizations(repo, |indexer, repo| async move {
            indexer.index_announcements(&*repo).await
        })
        .await
    }

    #[tracing::instrument(
        level = "debug",
        name = MoleculeFullTextSearchSchemaProvider_index_activities,
        skip_all,
    )]
    async fn index_activities(
        &self,
        repo: Arc<dyn FullTextSearchRepository>,
    ) -> Result<usize, InternalError> {
        self.index_across_organizations(repo, |indexer, repo| async move {
            indexer.index_activities(&*repo).await
        })
        .await
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
        repo: Arc<dyn FullTextSearchRepository>,
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
