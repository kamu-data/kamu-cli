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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use url::Url;

use crate::UrlExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteAliasResolverImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    access_token_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
    remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
}

#[component(pub)]
#[interface(dyn RemoteAliasResolver)]
impl RemoteAliasResolverImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        access_token_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
        remote_alias_reg: Arc<dyn RemoteAliasesRegistry>,
    ) -> Self {
        Self {
            remote_repo_reg,
            access_token_resolver,
            remote_alias_reg,
        }
    }

    async fn fetch_remote_url(
        &self,
        dataset_handle: &odf::DatasetHandle,
        remote_alias_kind: RemoteAliasKind,
    ) -> Result<Option<Url>, ResolveAliasError> {
        let remote_aliases = self
            .remote_alias_reg
            .get_remote_aliases(dataset_handle)
            .await
            .int_err()?;

        let aliases: Vec<_> = remote_aliases.get_by_kind(remote_alias_kind).collect();

        match aliases.len() {
            0 => Ok(None),
            1 => {
                if let odf::DatasetRefRemote::Url(remote_url) = aliases[0].clone() {
                    return Ok(Some(remote_url.as_ref().clone()));
                }
                Ok(None)
            }
            _ => Err(ResolveAliasError::AmbiguousAlias),
        }
    }

    fn combine_remote_url(
        &self,
        repo_url: &Url,
        account_name_maybe: Option<&odf::AccountName>,
        dataset_name: &odf::DatasetName,
    ) -> Result<Url, InternalError> {
        let mut res_url = repo_url.clone().as_odf_protocol().int_err()?;

        if let Some(account_name) = account_name_maybe {
            res_url = res_url.join(format!("{account_name}/").as_str()).unwrap();
        }
        res_url = res_url.join(dataset_name).unwrap();

        Ok(res_url)
    }

    async fn resolve_remote_dataset_name(
        &self,
        dataset_handle: &odf::DatasetHandle,
        remote_repo_url: &Url,
        access_token_maybe: Option<&String>,
    ) -> Result<odf::DatasetName, ResolveAliasError> {
        let result = if let Some(remote_dataset_name) =
            RemoteAliasResolverApiHelper::fetch_remote_dataset_name(
                remote_repo_url,
                &dataset_handle.id,
                access_token_maybe,
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
    #[tracing::instrument(level = "debug", skip_all, fields(dataset_handle, ?dataset_push_target_maybe))]
    async fn resolve_push_target(
        &self,
        dataset_handle: &odf::DatasetHandle,
        dataset_push_target_maybe: Option<odf::DatasetPushTarget>,
    ) -> Result<RemoteTarget, ResolveAliasError> {
        let (repo_name, mut account_name, dataset_name) = if let Some(dataset_push_target) =
            &dataset_push_target_maybe
        {
            match dataset_push_target {
                odf::DatasetPushTarget::Alias(dataset_alias_remote) => (
                    dataset_alias_remote.repo_name.clone(),
                    dataset_alias_remote.account_name.clone(),
                    Some(dataset_alias_remote.dataset_name.clone()),
                ),
                odf::DatasetPushTarget::Url(url_ref) => {
                    return Ok(RemoteTarget::new(url_ref.clone(), None, None, None));
                }
                odf::DatasetPushTarget::Repository(repository_name) => {
                    (repository_name.clone(), None, None)
                }
            }
        } else {
            if let Some(remote_url) = self
                .fetch_remote_url(dataset_handle, RemoteAliasKind::Push)
                .await?
            {
                return Ok(RemoteTarget::new(remote_url, None, None, None));
            }
            let remote_repo_names: Vec<_> = self.remote_repo_reg.get_all_repositories().collect();
            if remote_repo_names.len() > 1 {
                return Err(ResolveAliasError::AmbiguousRepository);
            }
            if let Some(repository_name) = remote_repo_names.first() {
                (repository_name.clone(), None, None)
            } else {
                return Err(ResolveAliasError::EmptyRepositoryList);
            }
        };

        let remote_repo = self.remote_repo_reg.get_repository(&repo_name)?;
        let transfer_url = if remote_repo.url.is_odf_protocol() {
            remote_repo.url.odf_to_transport_protocol()?
        } else {
            remote_repo.url
        };

        let access_token_maybe = self
            .access_token_resolver
            .resolve_odf_dataset_access_token(&transfer_url);
        if account_name.is_none() {
            account_name = RemoteAliasResolverApiHelper::resolve_remote_account_name(
                &transfer_url,
                access_token_maybe.as_ref(),
            )
            .await
            .int_err()?;
        }
        let transfer_dataset_name = if let Some(dn) = dataset_name.clone() {
            dn
        } else {
            self.resolve_remote_dataset_name(
                dataset_handle,
                &transfer_url,
                access_token_maybe.as_ref(),
            )
            .await?
        };

        let remote_url =
            self.combine_remote_url(&transfer_url, account_name.as_ref(), &transfer_dataset_name)?;

        return Ok(RemoteTarget::new(
            remote_url,
            Some(repo_name),
            dataset_name,
            account_name,
        ));
    }

    async fn resolve_pull_url(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<url::Url, ResolveAliasError> {
        match remote_ref {
            odf::DatasetRefRemote::ID(_, _) => Err(ResolveAliasError::Internal(
                "Syncing remote dataset by ID is not yet supported".int_err(),
            )),
            odf::DatasetRefRemote::Url(url) => {
                let mut dataset_url = url.as_ref().clone();
                dataset_url.ensure_trailing_slash();
                Ok(dataset_url)
            }
            odf::DatasetRefRemote::Alias(alias)
            | odf::DatasetRefRemote::Handle(odf::metadata::DatasetHandleRemote { alias, .. }) => {
                let remote_repo = self.remote_repo_reg.get_repository(&alias.repo_name)?;
                if !remote_repo.url.is_odf_protocol() {
                    return Ok(remote_repo
                        .url
                        .join(&format!("{}/", alias.local_alias()))
                        .unwrap());
                }

                let transfer_url = remote_repo.url.clone().odf_to_transport_protocol()?;

                let account_name = if alias.account_name.is_some() {
                    alias.account_name.clone()
                } else {
                    let access_token_maybe = self
                        .access_token_resolver
                        .resolve_odf_dataset_access_token(&transfer_url);
                    RemoteAliasResolverApiHelper::resolve_remote_account_name(
                        &transfer_url,
                        access_token_maybe.as_ref(),
                    )
                    .await
                    .int_err()?
                };

                let mut remote_url = self.combine_remote_url(
                    &transfer_url,
                    account_name.as_ref(),
                    &alias.dataset_name,
                )?;
                remote_url.ensure_trailing_slash();

                Ok(remote_url)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteAliasResolverApiHelper {}

impl RemoteAliasResolverApiHelper {
    fn build_headers_map(access_token_maybe: Option<&String>) -> http::HeaderMap {
        let mut header_map = http::HeaderMap::new();
        if let Some(access_token) = access_token_maybe {
            header_map.append(
                http::header::AUTHORIZATION,
                http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
            );
        };
        header_map
    }

    // Return account name if remote workspace is in multi tenant mode
    pub async fn resolve_remote_account_name(
        server_backend_url: &Url,
        access_token_maybe: Option<&String>,
    ) -> Result<Option<odf::AccountName>, GetRemoteAccountError> {
        if !(server_backend_url.scheme() == "http" || server_backend_url.scheme() == "https") {
            return Ok(None);
        }

        let client = reqwest::Client::new();
        let header_map = Self::build_headers_map(access_token_maybe);

        let workspace_info_response = client
            .get(server_backend_url.join("info").unwrap())
            .headers(header_map.clone())
            .send()
            .await
            .int_err()?;
        if workspace_info_response.status().is_client_error() {
            return Ok(None);
        }

        let json_workspace_info_response: serde_json::Value =
            workspace_info_response.json().await.int_err()?;

        if let Some(is_multi_tenant) = json_workspace_info_response["isMultiTenant"].as_bool()
            && !is_multi_tenant
        {
            return Ok(None);
        }

        let account_response = client
            .get(server_backend_url.join("accounts/me").unwrap())
            .headers(header_map)
            .send()
            .await
            .int_err()?;

        if account_response.status().is_client_error() {
            return Ok(None);
        }

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
        access_token_maybe: Option<&String>,
    ) -> Result<Option<odf::DatasetName>, ResolveAliasError> {
        if !(server_backend_url.scheme() == "http" || server_backend_url.scheme() == "https") {
            return Ok(None);
        }

        let client = reqwest::Client::new();
        let header_map = Self::build_headers_map(access_token_maybe);

        let response = client
            .get(
                server_backend_url
                    .join(&format!("datasets/{dataset_id}"))
                    .unwrap(),
            )
            .headers(header_map)
            .send()
            .await
            .int_err()?;

        if response.status().is_client_error() {
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
