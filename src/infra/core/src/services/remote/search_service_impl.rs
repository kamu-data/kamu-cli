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
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use odf::dataset::DatasetStorageUnitFactory;
use serde_json::json;
use url::Url;

pub struct SearchServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    dataset_storage_unit_factory: Arc<dyn DatasetStorageUnitFactory>,
}

#[component(pub)]
#[interface(dyn SearchService)]
impl SearchServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        dataset_storage_unit_factory: Arc<dyn DatasetStorageUnitFactory>,
    ) -> Self {
        Self {
            remote_repo_reg,
            dataset_storage_unit_factory,
        }
    }

    async fn search_in_repo_storage_unit(
        &self,
        url: &Url,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<Vec<SearchResultDataset>, SearchError> {
        let mut datasets = Vec::new();

        let storage_unit = self
            .dataset_storage_unit_factory
            .get_storage_unit(url, false)
            .await
            .map_err(|e| match e {
                odf::dataset::BuildDatasetStorageUnitError::UnsupportedProtocol(e) => {
                    SearchError::UnsupportedProtocol(e)
                }
                odf::dataset::BuildDatasetStorageUnitError::Internal(e) => SearchError::Internal(e),
            })?;

        let query = query.unwrap_or_default();

        use futures::TryStreamExt;
        let mut stored_handles_stream = storage_unit.stored_dataset_handles();
        while let Some(stored_hdl) = stored_handles_stream.try_next().await.int_err()? {
            if query.is_empty() || stored_hdl.alias.dataset_name.contains(query) {
                datasets.push(SearchResultDataset {
                    id: Some(stored_hdl.id),
                    alias: odf::DatasetAliasRemote::new(
                        repo_name.clone(),
                        stored_hdl.alias.account_name,
                        stored_hdl.alias.dataset_name,
                    ),
                    kind: None,
                    num_blocks: None,
                    num_records: None,
                    estimated_size: None,
                });
            }
        }

        Ok(datasets)
    }

    // TODO: This is a quick and dirty implementation that will soon be replaced
    async fn search_in_repo_odf(
        &self,
        url: &Url,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<Vec<SearchResultDataset>, SearchError> {
        let gql_query = r#"
            {
              search {
                query(query: "{query}", perPage: 100) {
                  nodes {
                    ... on Dataset {
                      id
                      name
                      owner {
                        accountName
                      }
                      kind
                      metadata {
                        chain {
                          blocks(page: 0, perPage: 1) {
                            totalCount
                          }
                        }
                      }
                      data {
                        numRecordsTotal
                        estimatedSize
                      }
                    }
                  }
                }
              }
            }
            "#
        .replace("{query}", query.unwrap_or_default());

        let mut gql_url = Url::parse(url.as_str().strip_prefix("odf+").unwrap()).unwrap();
        gql_url.path_segments_mut().unwrap().push("graphql");

        // TODO: Include auth token if we have one in store
        let cl = reqwest::Client::new();
        let response = cl
            .post(gql_url)
            .json(&json!({"query": gql_query}))
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?;

        let gql_response: serde_json::Value = response.json().await.int_err()?;

        let invalid_response = || {
            SearchError::Internal(
                format!("GQL endpoint returned invalid response:\n{gql_response}").int_err(),
            )
        };

        let Some(nodes) = gql_response["data"]["search"]["query"]["nodes"].as_array() else {
            return Err(invalid_response());
        };

        let mut datasets = Vec::new();

        for node in nodes {
            let ds: GqlDataset = serde_json::from_value(node.clone()).int_err()?;
            datasets.push(SearchResultDataset {
                id: Some(ds.id),
                alias: odf::DatasetAliasRemote::new(
                    repo_name.clone(),
                    ds.owner.account_name,
                    ds.name,
                ),
                kind: Some(ds.kind),
                num_blocks: Some(ds.metadata.chain.blocks.total_count),
                num_records: Some(ds.data.num_records_total),
                estimated_size: Some(ds.data.estimated_size),
            });
        }

        Ok(datasets)
    }

    // TODO: This is crude temporary implementation until ODF specifies registry
    // interface
    async fn search_in_resource(
        &self,
        url: &Url,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<Vec<SearchResultDataset>, SearchError> {
        match url.scheme() {
            "file" | "s3" | "s3+http" | "s3+https" => {
                self.search_in_repo_storage_unit(url, query, repo_name)
                    .await
            }
            "odf+http" | "odf+https" => self.search_in_repo_odf(url, query, repo_name).await,
            _ => Err(odf::dataset::UnsupportedProtocolError {
                message: None,
                entity_kind: "dataset",
                url: Box::new(url.clone()),
            }
            .into()),
        }
    }

    async fn search_in_repo(
        &self,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<SearchResult, SearchError> {
        let repo = self.remote_repo_reg.get_repository(repo_name)?;

        tracing::info!(repo_id = repo_name.as_str(), repo_url = ?repo.url, query = ?query, "Searching remote repository");

        let datasets = self.search_in_resource(&repo.url, query, repo_name).await?;

        Ok(SearchResult { datasets })
    }
}

#[async_trait::async_trait]
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
            let mut repo_result = self.search_in_repo(query, repo).await?;
            result.datasets.append(&mut repo_result.datasets);
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// GQL deserializers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(::serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GqlDataset {
    id: odf::DatasetID,
    name: odf::DatasetName,
    owner: GqlAccount,
    #[serde(with = "DatasetKindDef")]
    kind: odf::DatasetKind,
    metadata: GqlMetadata,
    data: GqlData,
}

#[derive(::serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GqlAccount {
    account_name: Option<odf::AccountName>,
}

#[derive(::serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GqlMetadata {
    chain: GqlMetadataChain,
}

#[derive(::serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GqlMetadataChain {
    blocks: GqlMetadataChainBlocks,
}

#[derive(::serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GqlMetadataChainBlocks {
    total_count: u64,
}

#[derive(::serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct GqlData {
    num_records_total: u64,
    estimated_size: u64,
}

#[derive(::serde::Deserialize)]
#[serde(remote = "odf::DatasetKind", rename_all = "SCREAMING_SNAKE_CASE")]
enum DatasetKindDef {
    Root,
    Derivative,
}
