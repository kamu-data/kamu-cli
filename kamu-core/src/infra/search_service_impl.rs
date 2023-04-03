// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use opendatafabric::*;
use tracing::info;

use crate::{domain::*, infra::utils::s3_context::S3Context};

use std::sync::Arc;
use url::Url;

pub struct SearchServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
}

#[component(pub)]
impl SearchServiceImpl {
    pub fn new(remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>) -> Self {
        Self { remote_repo_reg }
    }

    // TODO: This is crude temporary implementation until ODF specifies registry interface
    async fn search_in_resource(
        &self,
        url: &Url,
        query: Option<&str>,
    ) -> Result<Vec<DatasetNameWithOwner>, SearchError> {
        let mut datasets = Vec::new();

        match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| "Invalid path URL")
                    .int_err()?;
                let query = query.unwrap_or_default();
                for entry in std::fs::read_dir(&path).int_err()? {
                    if let Some(file_name) = entry.int_err()?.file_name().to_str() {
                        if query.is_empty() || file_name.contains(query) {
                            datasets.push(DatasetNameWithOwner::new(
                                None,
                                DatasetName::try_from(file_name).unwrap(),
                            ));
                        }
                    }
                }
            }
            "s3" | "s3+http" | "s3+https" => {
                let s3_context = S3Context::from_url(&url).await;
                let folders_common_prefixes = s3_context.bucket_list_folders().await?;

                let query = query.unwrap_or_default();

                for prefix in folders_common_prefixes {
                    let mut prefix = prefix.prefix.unwrap();
                    while prefix.ends_with('/') {
                        prefix.pop();
                    }

                    let name = DatasetName::try_from(prefix).int_err()?;

                    if query.is_empty() || name.contains(query) {
                        datasets.push(DatasetNameWithOwner::new(None, name));
                    }
                }
            }
            _ => {
                return Err(UnsupportedProtocolError {
                    message: None,
                    url: url.clone(),
                }
                .into())
            }
        }

        Ok(datasets)
    }

    async fn search_in_repo(
        &self,
        query: Option<&str>,
        repo_name: &RepositoryName,
    ) -> Result<SearchResult, SearchError> {
        let repo = self.remote_repo_reg.get_repository(&repo_name)?;

        info!(repo_id = repo_name.as_str(), repo_url = ?repo.url, query = ?query, "Searching remote repository");

        let datasets = self.search_in_resource(&repo.url, query).await?;

        Ok(SearchResult {
            datasets: datasets
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
