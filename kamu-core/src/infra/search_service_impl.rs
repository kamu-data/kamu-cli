// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use opendatafabric::{DatasetRefBuf, RepositoryID};
use slog::{info, Logger};

use crate::domain::{
    DomainError, MetadataRepository, SearchError, SearchOptions, SearchResult, SearchService,
};

use super::RepositoryFactory;

pub struct SearchServiceImpl {
    metadata_repo: Arc<dyn MetadataRepository>,
    repo_factory: Arc<RepositoryFactory>,
    logger: Logger,
}

#[component(pub)]
impl SearchServiceImpl {
    pub fn new(
        metadata_repo: Arc<dyn MetadataRepository>,
        repo_factory: Arc<RepositoryFactory>,
        logger: Logger,
    ) -> Self {
        Self {
            metadata_repo,
            repo_factory,
            logger,
        }
    }

    fn search_in_repo(
        &self,
        query: Option<&str>,
        repo_id: &RepositoryID,
    ) -> Result<SearchResult, SearchError> {
        let repo = self
            .metadata_repo
            .get_repository(&repo_id)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SearchError::RepositoryDoesNotExist {
                    repo_id: repo_id.to_owned(),
                },
                e @ _ => SearchError::InternalError(e.into()),
            })?;

        info!(self.logger, "Searching remote repository"; 
                "repo_id" => repo_id.as_str(), "repo_url" => ?repo.url, "query" => ?query);

        let repo_client = self
            .repo_factory
            .get_repository_client(&repo)
            .map_err(|e| SearchError::InternalError(e.into()))?;

        let resp = repo_client.lock().unwrap().search(query)?;

        // Prefix all IDs with the local name of a repo
        Ok(SearchResult {
            datasets: resp
                .datasets
                .into_iter()
                .map(|id| DatasetRefBuf::new(Some(repo_id), id.username(), id.local_id()))
                .collect(),
        })
    }
}

impl SearchService for SearchServiceImpl {
    fn search(
        &self,
        query: Option<&str>,
        options: SearchOptions,
    ) -> Result<SearchResult, SearchError> {
        let repo_ids = if !options.repository_ids.is_empty() {
            options.repository_ids
        } else {
            self.metadata_repo.get_all_repositories().collect()
        };

        itertools::process_results(
            repo_ids
                .iter()
                .map(|repo_id| self.search_in_repo(query, repo_id)),
            |it| it.fold(SearchResult::default(), |a, b| a.merge(b)),
        )
    }
}
