// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use s3_utils::{S3Context, S3Metrics};
use serde_json::json;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SearchServiceRemote)]
pub struct SearchServiceRemoteImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    odf_server_access_token_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,

    #[dill::component(explicit)]
    maybe_s3_metrics: Option<Arc<S3Metrics>>,
}

impl SearchServiceRemoteImpl {
    fn search_in_repo_localfs(
        &self,
        url: &Url,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<Vec<SearchRemoteResultDataset>, SearchRemoteError> {
        let mut datasets = Vec::new();

        let path = url
            .to_file_path()
            .map_err(|_| "Invalid path URL")
            .int_err()?;
        let query = query.unwrap_or_default();
        for entry in std::fs::read_dir(path).int_err()? {
            if let Some(file_name) = entry.int_err()?.file_name().to_str()
                && (query.is_empty() || file_name.contains(query))
            {
                datasets.push(SearchRemoteResultDataset {
                    id: None,
                    alias: odf::DatasetAliasRemote::new(
                        repo_name.clone(),
                        None,
                        odf::DatasetName::try_from(file_name).int_err()?,
                    ),
                    kind: None,
                    num_blocks: None,
                    num_records: None,
                    estimated_size_bytes: None,
                });
            }
        }

        Ok(datasets)
    }

    async fn search_in_repo_s3(
        &self,
        url: &Url,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<Vec<SearchRemoteResultDataset>, SearchRemoteError> {
        let mut datasets = Vec::new();

        let mut s3_context = S3Context::from_url(url).await;
        if let Some(metrics) = &self.maybe_s3_metrics {
            s3_context = s3_context.with_metrics(metrics.clone());
        }

        let folders_common_prefixes = s3_context.bucket_list_folders().await?;

        let query = query.unwrap_or_default();

        for prefix in folders_common_prefixes {
            let mut prefix = prefix.prefix.unwrap();
            while prefix.ends_with('/') {
                prefix.pop();
            }

            let name = odf::DatasetName::try_from(prefix).int_err()?;

            if query.is_empty() || name.contains(query) {
                datasets.push(SearchRemoteResultDataset {
                    id: None,
                    alias: odf::DatasetAliasRemote::new(repo_name.clone(), None, name),
                    kind: None,
                    num_blocks: None,
                    num_records: None,
                    estimated_size_bytes: None,
                });
            }
        }

        Ok(datasets)
    }

    // TODO: This is a quick and dirty implementation that will soon be replaced
    async fn search_in_repo_odf(
        &self,
        odf_url: &Url,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<Vec<SearchRemoteResultDataset>, SearchRemoteError> {
        let gql_query = indoc::indoc!(
            r#"
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
                        estimatedSizeBytes
                      }
                    }
                  }
                }
              }
            }
            "#
        )
        .replace("{query}", query.unwrap_or_default());

        let response = {
            let url = Url::parse(odf_url.as_str().strip_prefix("odf+").unwrap()).unwrap();

            let maybe_access_token = self
                .odf_server_access_token_resolver
                .resolve_odf_dataset_access_token(&url);

            let mut gql_url = url;
            gql_url.path_segments_mut().unwrap().push("graphql");

            let client = reqwest::Client::new();
            let mut request_builder = client.post(gql_url);

            // TODO: Signaling of an expired token
            if let Some(access_token) = maybe_access_token {
                request_builder = request_builder.bearer_auth(access_token);
            }

            request_builder
                .json(&json!({"query": gql_query}))
                .send()
                .await
                .int_err()?
                .error_for_status()
                .int_err()?
        };

        let gql_response: serde_json::Value = response.json().await.int_err()?;

        let invalid_response = || {
            SearchRemoteError::Internal(
                format!("GQL endpoint returned invalid response:\n{gql_response}").int_err(),
            )
        };

        let Some(nodes) = gql_response["data"]["search"]["query"]["nodes"].as_array() else {
            return Err(invalid_response());
        };

        let mut datasets = Vec::new();

        for node in nodes {
            let ds: GqlDataset = serde_json::from_value(node.clone()).int_err()?;
            datasets.push(SearchRemoteResultDataset {
                id: Some(ds.id),
                alias: odf::DatasetAliasRemote::new(
                    repo_name.clone(),
                    ds.owner.account_name,
                    ds.name,
                ),
                kind: Some(ds.kind),
                num_blocks: Some(ds.metadata.chain.blocks.total_count),
                num_records: Some(ds.data.num_records_total),
                estimated_size_bytes: Some(ds.data.estimated_size_bytes),
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
    ) -> Result<Vec<SearchRemoteResultDataset>, SearchRemoteError> {
        match url.scheme() {
            "file" => self.search_in_repo_localfs(url, query, repo_name),
            "s3" | "s3+http" | "s3+https" => self.search_in_repo_s3(url, query, repo_name).await,
            "odf+http" | "odf+https" => self.search_in_repo_odf(url, query, repo_name).await,
            _ => Err(odf::dataset::UnsupportedProtocolError {
                message: None,
                url: url.clone(),
            }
            .into()),
        }
    }

    async fn search_in_repo(
        &self,
        query: Option<&str>,
        repo_name: &odf::RepoName,
    ) -> Result<SearchRemoteResult, SearchRemoteError> {
        let repo = self.remote_repo_reg.get_repository(repo_name)?;

        tracing::info!(repo_id = repo_name.as_str(), repo_url = ?repo.url, query = ?query, "Searching remote repository");

        let datasets = self.search_in_resource(&repo.url, query, repo_name).await?;

        Ok(SearchRemoteResult { datasets })
    }
}

#[async_trait::async_trait]
impl SearchServiceRemote for SearchServiceRemoteImpl {
    async fn search(
        &self,
        query: Option<&str>,
        options: SearchRemoteOpts,
    ) -> Result<SearchRemoteResult, SearchRemoteError> {
        let repo_names = if !options.repository_names.is_empty() {
            options.repository_names
        } else {
            self.remote_repo_reg.get_all_repositories().collect()
        };

        let mut result = SearchRemoteResult::default();
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
    estimated_size_bytes: u64,
}

#[derive(::serde::Deserialize)]
#[serde(remote = "odf::DatasetKind", rename_all = "SCREAMING_SNAKE_CASE")]
enum DatasetKindDef {
    Root,
    Derivative,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
