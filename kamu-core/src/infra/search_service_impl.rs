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

    async fn search_in_repo(
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

        let resp = repo_client.search(query).await?;

        Ok(SearchResult {
            datasets: resp
                .datasets
                .into_iter()
                .map(|name| name.as_remote_name(repo_name))
                .collect(),
        })
    }
}

#[async_trait::async_trait(?Send)]
impl SearchService for SearchServiceImpl {
    async fn search(
        &self,
        query: Option<&str>,
        options: SearchOptions,
    ) -> Result<SearchResult, SearchError> {
        let repo_names = if !options.repository_names.is_empty() {
            options.repository_names
        } else {
            self.remote_repo_reg.get_all_repositories().collect()
        };

        let mut result = SearchResult::default();
        for repo in &repo_names {
            let repo_result = self.search_in_repo(query, repo).await?;
            result = result.merge(repo_result);
        }

        Ok(result)
    }
}
