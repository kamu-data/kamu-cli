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

use dill::*;
use internal_error::ResultIntoInternal;
use kamu_core::auth::OdfServerAccessTokenResolver;
use kamu_core::*;
use opendatafabric as odf;
use serde_json::json;
use url::Url;

use crate::UrlExt;

pub struct RemoteAliasResolverImpl {
    access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    dataset_repo: Arc<dyn DatasetRepository>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
}

#[component(pub)]
#[interface(dyn RemoteAliasResolver)]
impl RemoteAliasResolverImpl {
    pub fn new(
        access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        dataset_repo: Arc<dyn DatasetRepository>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ) -> Self {
        Self {
            access_token_resolver,
            remote_repo_reg,
            dataset_repo,
            remote_alias_reg,
        }
    }

    // Return account name if remote workspace is in multitenant mode
    pub async fn get_remote_account_name_by_access_token(
        &self,
        server_backend_url: &Url,
        access_token: &str,
    ) -> Result<Option<odf::AccountName>, GetRemoteAccountError> {
        let client = reqwest::Client::new();
        let gql_url = server_backend_url.join("graphql").unwrap();

        let gql_query = r#"
            {
                accounts {
                    byAccessToken(
                        accessToken: "{access_token}"
                    ) {
                        isMultiTenant
                        account {
                            id
                            accountName
                            displayName
                            accountType
                            avatarUrl
                            isAdmin
                        }
                    }
                }
            }
            "#
        .replace("{access_token}", access_token);

        let response = client
            .post(gql_url)
            .json(&json!({"query": gql_query}))
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?;

        let gql_response: serde_json::Value = response.json().await.int_err()?;
        let is_multi_tenant_maybe =
            gql_response["data"]["accounts"]["byAccessToken"]["isMultiTenant"].as_bool();

        if let Some(is_multi_tenant) = is_multi_tenant_maybe
            && is_multi_tenant
        {
            if let Some(gql_account_name) =
                gql_response["data"]["accounts"]["byAccessToken"]["account"]["accountName"].as_str()
            {
                let account_name = odf::AccountName::from_str(gql_account_name).map_err(|_| {
                    GetRemoteAccountError::InvalidResponse(InvalidGQLResponseError {
                        response: gql_response.to_string(),
                    })
                })?;
                return Ok(Some(account_name));
            }
            return Err(GetRemoteAccountError::InvalidResponse(
                InvalidGQLResponseError {
                    response: gql_response.to_string(),
                },
            ));
        }
        Ok(None)
    }

    // TODO: avoid traversing all datasets for every alias
    async fn inverse_lookup_dataset_by_push_alias(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<odf::DatasetHandle, PushError> {
        // Do a quick check when remote and local names match
        if let Some(remote_name) = remote_ref.dataset_name() {
            if let Some(local_handle) = self
                .dataset_repo
                .try_resolve_dataset_ref(
                    &odf::DatasetAlias::new(None, remote_name.clone()).as_local_ref(),
                )
                .await?
            {
                if self
                    .remote_alias_reg
                    .get_remote_aliases(&local_handle.as_local_ref())
                    .await
                    .int_err()?
                    .contains(remote_ref, RemoteAliasKind::Push)
                {
                    return Ok(local_handle);
                }
            }
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
        Err(PushError::NoTarget)
    }
}

#[async_trait::async_trait]
impl RemoteAliasResolver for RemoteAliasResolverImpl {
    async fn resolve_remote_alias(
        &self,
        dataset_ref_maybe: Option<&odf::DatasetRef>,
        transfer_dataset_ref_maybe: Option<odf::TransferDatasetRef>,
        remote_alias_kind: &RemoteAliasKind,
    ) -> Result<odf::DatasetRefRemote, ResolveAliasError> {
        let mut repo_name: &odf::RepoName;
        let mut account_name = None;
        let mut dataset_name = None;

        if let Some(transfer_dataset_ref) = &transfer_dataset_ref_maybe {
            match transfer_dataset_ref {
                odf::TransferDatasetRef::RemoteRef(dataset_ref_remote) => {
                    return Ok(dataset_ref_remote.clone());
                }
                odf::TransferDatasetRef::RepoRef(repo_ref) => {
                    repo_name = &repo_ref.repo_name;
                    account_name = repo_ref.account_name.as_ref();
                    dataset_name = repo_ref.dataset_name.as_ref();
                }
            }
        } else {
            let remote_repo_names: Vec<_> = self.remote_repo_reg.get_all_repositories().collect();
            if remote_repo_names.len() > 1 {
                return Err(ResolveAliasError::AmbiguousRepository(
                    AmbiguousRepositoryError {},
                ));
            }
            if let Some(repository_name) = remote_repo_names.first() {
                repo_name = repository_name;
            }
            return Err(ResolveAliasError::EmptyRepositoryList(
                EmptyRepositoryListError {},
            ));
        }
        let remote_repo = self.remote_repo_reg.get_repository(repo_name).int_err()?;

        let odf_repo_url = remote_repo.url.as_odf_protocol().int_err()?;
        if let Some(access_token) = self
            .access_token_resolver
            .resolve_odf_dataset_access_token(&odf_repo_url)
        {
            account_name = self
                .get_remote_account_name_by_access_token(&remote_repo.url, access_token.as_str())
                .await
                .int_err()?
                .as_ref();
        }

        unimplemented!()
    }
}
