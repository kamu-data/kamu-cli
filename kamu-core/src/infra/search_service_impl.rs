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
use opendatafabric::*;
use tracing::info;

use super::RepositoryFactory;
use crate::domain::*;

pub struct SearchServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    repo_factory: Arc<RepositoryFactory>,
}

#[component(pub)]
impl SearchServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        repo_factory: Arc<RepositoryFactory>,
    ) -> Self {
        Self {
            remote_repo_reg,
            repo_factory,
        }
    }

    fn search_in_repo(
        &self,
        query: Option<&str>,
        repo_name: &RepositoryName,
    ) -> Result<SearchResult, SearchError> {
        let repo = self
            .remote_repo_reg
            .get_repository(&repo_name)
            .map_err(|e| match e {
                DomainError::DoesNotExist { .. } => SearchError::RepositoryDoesNotExist {
                    repo_name: repo_name.clone(),
                },
                e @ _ => SearchError::InternalError(e.into()),
            })?;

        info!(repo_id = repo_name.as_str(), repo_url = ?repo.url, query = ?query, "Searching remote repository");

        let repo_client = self
            .repo_factory
            .get_repository_client(&repo)
            .map_err(|e| SearchError::InternalError(e.into()))?;

        let resp = repo_client.lock().unwrap().search(query)?;

        // TODO: Avoid rewriting remote name to prefix with the local name of a repo
        Ok(SearchResult {
            datasets: resp
                .datasets
                .into_iter()
                .map(|remote_name| {
                    RemoteDatasetName::new(
                        repo_name,
                        remote_name.account().as_ref(),
                        &remote_name.dataset(),
                    )
                })
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
        let repo_names = if !options.repository_names.is_empty() {
            options.repository_names
        } else {
            self.remote_repo_reg.get_all_repositories().collect()
        };

        itertools::process_results(
            repo_names
                .iter()
                .map(|repo| self.search_in_repo(query, repo)),
            |it| it.fold(SearchResult::default(), |a, b| a.merge(b)),
        )
    }
}
