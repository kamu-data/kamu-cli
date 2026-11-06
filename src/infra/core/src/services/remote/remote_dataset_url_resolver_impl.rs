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
use internal_error::ResultIntoInternal;
use kamu_core::*;
use url::Url;

use crate::RemoteAliasResolverApiHelper;
use crate::utils::url_ext::UrlExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn RemoteDatasetUrlResolver)]
pub struct RemoteDatasetUrlResolverImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    access_token_resolver: Arc<dyn odf::dataset::OdfServerAccessTokenResolver>,
}

impl RemoteDatasetUrlResolverImpl {
    async fn resolve_url_from_odf_transfer_url(
        &self,
        alias: &odf::DatasetAliasRemote,
        odf_transfer_url: &Url,
    ) -> Result<Url, ResolveRemoteSourceError> {
        let account_name = if alias.account_name.is_some() {
            alias.account_name.clone()
        } else {
            let access_token_maybe = self
                .access_token_resolver
                .resolve_odf_dataset_access_token(odf_transfer_url);
            RemoteAliasResolverApiHelper::resolve_remote_account_name(
                odf_transfer_url,
                access_token_maybe.as_ref(),
            )
            .await
            .int_err()?
        };

        let mut remote_url = RemoteAliasResolverApiHelper::combine_remote_url(
            odf_transfer_url,
            account_name.as_ref(),
            &alias.dataset_name,
        )?;
        remote_url.ensure_trailing_slash();

        Ok(remote_url)
    }
}

#[async_trait::async_trait]
impl RemoteDatasetUrlResolver for RemoteDatasetUrlResolverImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(%remote_ref))]
    async fn resolve_dataset_url(
        &self,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<Url, ResolveRemoteSourceError> {
        match remote_ref {
            // TODO: the reference can be resolved if using ODF protocol.
            // However, it is still difficult with simple folder repositories without index.
            odf::DatasetRefRemote::ID(_, _) => Err(RemoteDatasetByIdUnsupportedError {
                remote_ref: remote_ref.clone(),
            }
            .into()),
            odf::DatasetRefRemote::Url(url) => {
                let mut dataset_url = url.as_ref().clone();
                dataset_url.ensure_trailing_slash();
                Ok(dataset_url)
            }
            odf::DatasetRefRemote::Alias(alias)
            | odf::DatasetRefRemote::Handle(odf::metadata::DatasetHandleRemote { alias, .. }) => {
                let remote_repo = self.remote_repo_reg.get_repository(&alias.repo_name)?;
                if remote_repo.url.is_odf_protocol() {
                    let transfer_url = remote_repo.url.clone().odf_to_transport_protocol()?;
                    self.resolve_url_from_odf_transfer_url(alias, &transfer_url)
                        .await
                } else {
                    Ok(remote_repo
                        .url
                        .join(&format!("{}/", alias.local_alias()))
                        .unwrap())
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
