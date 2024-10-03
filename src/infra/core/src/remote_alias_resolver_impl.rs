// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use auth::OdfServerAccessTokenResolver;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use opendatafabric::{self as odf, DatasetRefRemote};
use url::Url;

use crate::UrlExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteAliasResolverImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
}

#[component(pub)]
#[interface(dyn RemoteAliasResolver)]
impl RemoteAliasResolverImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ) -> Self {
        Self {
            remote_repo_reg,
            access_token_resolver,
            dataset_repo,
            remote_alias_reg,
        }
    }

    async fn fetch_remote_alias(
        &self,
        local_handle: &odf::DatasetHandle,
        remote_alias_kind: RemoteAliasKind,
    ) -> Result<Option<odf::DatasetRefRemote>, ResolveAliasError> {
        let remote_aliases = self
            .remote_alias_reg
            .get_remote_aliases(&local_handle.as_local_ref())
            .await
            .int_err()?;

        let push_aliases: Vec<_> = remote_aliases.get_by_kind(remote_alias_kind).collect();

        match push_aliases.len() {
            0 => Ok(None),
            1 => Ok(Some(push_aliases[0].clone())),
            _ => Err(ResolveAliasError::AmbiguousAlias),
        }
    }

    fn combine_remote_alias(
        &self,
        repo_url: &Url,
        account_name_maybe: Option<odf::AccountName>,
        dataset_name: &odf::DatasetName,
    ) -> Result<odf::DatasetRefRemote, InternalError> {
        let mut res_url = repo_url.clone().as_odf_protocol().int_err()?;
        {
            let mut path_segments = res_url.path_segments_mut().unwrap();
            if let Some(account_name) = account_name_maybe {
                path_segments.push(&account_name);
            }
            path_segments.push(dataset_name);
        }
        Ok(res_url.into())
    }

    async fn resolve_remote_dataset_name(
        &self,
        dataset_handle: &odf::DatasetHandle,
        remote_repo_url: &Url,
    ) -> Result<odf::DatasetName, ResolveAliasError> {
        let result = if let Some(remote_dataset_name) =
            RemoteAliasResolverApiHelper::fetch_remote_dataset_name(
                remote_repo_url,
                &dataset_handle.id,
            )
            .await?
        {
            remote_dataset_name
        } else {
            dataset_handle.alias.dataset_name.clone()
        };
        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RemoteAliasResolver for RemoteAliasResolverImpl {
    async fn resolve_remote_alias(
        &self,
        local_dataset_handle: &odf::DatasetHandle,
        transfer_dataset_ref_maybe: Option<odf::TransferDatasetRef>,
        remote_alias_kind: RemoteAliasKind,
    ) -> Result<odf::DatasetRefRemote, ResolveAliasError> {
        let repo_name: odf::RepoName;
        let mut account_name = None;
        let mut dataset_name = None;

        if let Some(transfer_dataset_ref) = &transfer_dataset_ref_maybe {
            match transfer_dataset_ref {
                odf::TransferDatasetRef::RemoteRef(DatasetRefRemote::Alias(
                    dataset_alias_remote,
                )) => {
                    repo_name = dataset_alias_remote.repo_name.clone();
                    account_name.clone_from(&dataset_alias_remote.account_name);
                    dataset_name = Some(dataset_alias_remote.dataset_name.clone());
                }
                odf::TransferDatasetRef::RemoteRef(dataset_ref_remote) => {
                    return Ok(dataset_ref_remote.clone());
                }
                odf::TransferDatasetRef::Repository(repository_name) => {
                    repo_name = repository_name.clone();
                }
            }
        } else {
            if let Some(remote_alias) = self
                .fetch_remote_alias(local_dataset_handle, remote_alias_kind)
                .await?
            {
                return Ok(remote_alias);
            }
            let remote_repo_names: Vec<_> = self.remote_repo_reg.get_all_repositories().collect();
            if remote_repo_names.len() > 1 {
                return Err(ResolveAliasError::AmbiguousRepository);
            }
            if let Some(repository_name) = remote_repo_names.first() {
                repo_name = repository_name.clone();
            } else {
                return Err(ResolveAliasError::EmptyRepositoryList);
            }
        }
        let remote_repo = self.remote_repo_reg.get_repository(&repo_name).int_err()?;

        if account_name.is_none() {
            let access_token_maybe = self
                .access_token_resolver
                .resolve_odf_dataset_access_token(&remote_repo.url);
            account_name = RemoteAliasResolverApiHelper::resolve_remote_account_name(
                &remote_repo.url,
                access_token_maybe,
            )
            .await
            .int_err()?;
        }
        let push_dataset_name = dataset_name.unwrap_or(
            self.resolve_remote_dataset_name(local_dataset_handle, &remote_repo.url)
                .await?,
        );

        let remote_alias =
            self.combine_remote_alias(&remote_repo.url, account_name, &push_dataset_name)?;

        return Ok(remote_alias);
    }

    // TODO: avoid traversing all datasets for every alias
    async fn inverse_lookup_dataset_by_alias(
        &self,
        transfer_ref: &odf::TransferDatasetRef,
        remote_alias_kind: RemoteAliasKind,
    ) -> Result<odf::DatasetHandle, ResolveAliasError> {
        // Do a quick check when remote and local names match
        if let odf::TransferDatasetRef::RemoteRef(remote_ref) = transfer_ref {
            if let Some(remote_name) = remote_ref.dataset_name()
                && let Some(local_handle) = self
                    .dataset_repo
                    .try_resolve_dataset_ref(
                        &odf::DatasetAlias::new(None, remote_name.clone()).as_local_ref(),
                    )
                    .await?
                && self
                    .remote_alias_reg
                    .get_remote_aliases(&local_handle.as_local_ref())
                    .await
                    .int_err()?
                    .contains(remote_ref, remote_alias_kind)
            {
                return Ok(local_handle);
            }

            // No luck - now have to search through aliases
            use tokio_stream::StreamExt;
            let mut datasets = self.dataset_repo.get_all_datasets();
            while let Some(dataset_handle) = datasets.next().await {
                let dataset_handle = dataset_handle?;

                if self
                    .remote_alias_reg
                    .get_remote_aliases(&dataset_handle.as_local_ref())
                    .await
                    .int_err()?
                    .contains(remote_ref, RemoteAliasKind::Push)
                {
                    return Ok(dataset_handle);
                }
            }
        }
        Err(ResolveAliasError::EmptyRepositoryList)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteAliasResolverApiHelper {}

impl RemoteAliasResolverApiHelper {
    // Return account name if remote workspace is in multi tenant mode
    pub async fn resolve_remote_account_name(
        server_backend_url: &Url,
        access_token_maybe: Option<String>,
    ) -> Result<Option<odf::AccountName>, GetRemoteAccountError> {
        let client = reqwest::Client::new();
        let mut header_map = http::HeaderMap::new();
        if let Some(access_token) = access_token_maybe {
            header_map.append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
            );
        };

        let workspace_info_response = client
            .get(server_backend_url.join("workspace/info").unwrap())
            .headers(header_map.clone())
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?;
        let json_workspace_info_response: serde_json::Value =
            workspace_info_response.json().await.int_err()?;

        if let Some(is_multi_tenant) = json_workspace_info_response["isMultiTenant"].as_bool()
            && !is_multi_tenant
        {
            return Ok(None);
        }

        let account_response = client
            .get(server_backend_url.join("me").unwrap())
            .headers(header_map)
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?;
        let json_account_response: serde_json::Value = account_response.json().await.int_err()?;

        if let Some(api_account_name) = json_account_response["accountName"].as_str() {
            let account_name = odf::AccountName::from_str(api_account_name).map_err(|_| {
                GetRemoteAccountError::InvalidResponse(InvalidApiResponseError {
                    response: json_account_response.to_string(),
                })
            })?;
            return Ok(Some(account_name));
        }
        Err(GetRemoteAccountError::InvalidResponse(
            InvalidApiResponseError {
                response: json_account_response.to_string(),
            },
        ))
    }

    pub async fn fetch_remote_dataset_name(
        server_backend_url: &Url,
        dataset_id: &odf::DatasetID,
    ) -> Result<Option<odf::DatasetName>, ResolveAliasError> {
        let client = reqwest::Client::new();
        let response = client
            .get(
                server_backend_url
                    .join(&format!("datasets/{dataset_id}"))
                    .unwrap(),
            )
            .send()
            .await
            .int_err()?;
        if response.status() == http::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let json_response: serde_json::Value = response.json().await.int_err()?;

        if let Some(res_dataset_name) = json_response["datasetName"].as_str() {
            let dataset_name = odf::DatasetName::try_from(res_dataset_name).int_err()?;
            return Ok(Some(dataset_name));
        }
        Ok(None)
    }
}
