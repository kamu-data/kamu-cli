// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_search::*;

use crate::{
    molecule_activity_full_text_search_schema as activity_schema,
    molecule_announcement_full_text_search_schema as announcement_schema,
    molecule_data_room_entry_full_text_search_schema as data_room_entry_schema,
    molecule_project_full_text_search_schema as project_schema,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct MoleculeFullTextSearchSchemaProvider {
    // ...
}

impl MoleculeFullTextSearchSchemaProvider {
    async fn index_molecule_projects(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO: Implement indexing logic for molecule projects
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(0)
    }

    async fn index_molecule_data_room_entries(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO: Implement indexing logic for molecule data room entries
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(0)
    }

    async fn index_molecule_announcements(
        &self,
        _repo: &dyn FullTextSearchRepository,
    ) -> Result<usize, InternalError> {
        // TODO: Implement indexing logic for molecule announcements
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        Ok(0)
    }

    async fn index_molecule_activities(
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
            activity_schema::SCHEMA_NAME => self.index_molecule_activities(repo).await?,
            announcement_schema::SCHEMA_NAME => self.index_molecule_announcements(repo).await?,
            data_room_entry_schema::SCHEMA_NAME => {
                self.index_molecule_data_room_entries(repo).await?
            }
            project_schema::SCHEMA_NAME => self.index_molecule_projects(repo).await?,

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
