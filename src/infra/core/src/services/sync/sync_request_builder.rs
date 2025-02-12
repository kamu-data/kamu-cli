// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::{
    DatasetAnyRefUnresolvedError,
    DatasetRegistry,
    DatasetRegistryExt,
    RemoteAliasResolver,
    RemoteRepositoryRegistry,
    SyncError,
    SyncRef,
    SyncRefRemote,
    SyncRequest,
};
use url::Url;

use crate::UrlExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct SyncRequestBuilder {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
}

impl SyncRequestBuilder {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
        remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
        remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_factory,
            remote_repo_registry,
            remote_alias_resolver,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?src_ref, ?dst_ref, %create_dst_if_not_exists))]
    pub async fn build_sync_request(
        &self,
        src_ref: odf::DatasetRefAny,
        dst_ref: odf::DatasetRefAny,
        create_dst_if_not_exists: bool,
    ) -> Result<SyncRequest, SyncError> {
        let src_sync_ref = self.resolve_source_sync_ref(&src_ref).await?;

        let dst_sync_ref = self
            .resolve_dest_sync_ref(&dst_ref, create_dst_if_not_exists)
            .await?;

        let sync_request = SyncRequest {
            src: src_sync_ref,
            dst: dst_sync_ref,
        };

        Ok(sync_request)
    }

    async fn resolve_source_sync_ref(
        &self,
        any_ref: &odf::DatasetRefAny,
    ) -> Result<SyncRef, SyncError> {
        match any_ref.as_local_ref(|repo| self.remote_repo_registry.get_repository(repo).is_ok()) {
            Ok(local_ref) => {
                let resolved_dataset = self.dataset_registry.get_dataset_by_ref(&local_ref).await?;
                self.ensure_dataset_head_present(local_ref.as_any_ref(), resolved_dataset.as_ref())
                    .await?;
                Ok(SyncRef::Local(resolved_dataset))
            }
            Err(remote_ref) => {
                let remote_dataset_url = Arc::new(
                    self.remote_alias_resolver
                        .resolve_pull_url(&remote_ref)
                        .await
                        .int_err()?,
                );
                let dataset = self
                    .dataset_factory
                    .get_dataset(remote_dataset_url.as_ref(), false)
                    .await?;
                self.ensure_dataset_head_present(
                    odf::DatasetRefAny::Url(remote_dataset_url.clone()),
                    dataset.as_ref(),
                )
                .await?;
                Ok(SyncRef::Remote(SyncRefRemote {
                    url: remote_dataset_url,
                    dataset,
                    original_remote_ref: remote_ref,
                }))
            }
        }
    }

    async fn resolve_dest_sync_ref(
        &self,
        any_ref: &odf::DatasetRefAny,
        create_if_not_exists: bool,
    ) -> Result<SyncRef, SyncError> {
        match any_ref.as_local_ref(|repo| self.remote_repo_registry.get_repository(repo).is_ok()) {
            Ok(local_ref) => match self.dataset_registry.get_dataset_by_ref(&local_ref).await {
                Ok(resolved_dataset) => Ok(SyncRef::Local(resolved_dataset)),
                Err(odf::dataset::GetDatasetError::NotFound(_)) if create_if_not_exists => {
                    if let Some(alias) = local_ref.alias() {
                        Ok(SyncRef::LocalNew(alias.clone()))
                    } else {
                        Err(DatasetAnyRefUnresolvedError::new(local_ref.as_any_ref()).into())
                    }
                }
                Err(err) => Err(err.into()),
            },
            Err(remote_ref) => {
                let remote_dataset_url = Arc::new(
                    self.remote_alias_resolver
                        .resolve_pull_url(&remote_ref)
                        .await
                        .int_err()?,
                );
                let dataset = self
                    .dataset_factory
                    .get_dataset(remote_dataset_url.as_ref(), create_if_not_exists)
                    .await?;

                if !create_if_not_exists {
                    self.ensure_dataset_head_present(remote_ref.as_any_ref(), dataset.as_ref())
                        .await?;
                }

                Ok(SyncRef::Remote(SyncRefRemote {
                    url: remote_dataset_url,
                    dataset,
                    original_remote_ref: remote_ref,
                }))
            }
        }
    }

    async fn ensure_dataset_head_present(
        &self,
        dataset_ref: odf::DatasetRefAny,
        dataset: &dyn odf::Dataset,
    ) -> Result<(), SyncError> {
        match dataset
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
            .await
        {
            Ok(_) => Ok(()),
            Err(odf::storage::GetRefError::NotFound(_)) => {
                Err(DatasetAnyRefUnresolvedError { dataset_ref }.into())
            }
            Err(odf::storage::GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn resolve_remote_dataset_url(
    remote_repo_registry: &dyn RemoteRepositoryRegistry,
    remote_ref: &odf::DatasetRefRemote,
) -> Result<Url, SyncError> {
    // TODO: REMOTE ID
    match remote_ref {
        odf::DatasetRefRemote::ID(_, _) => Err(SyncError::Internal(
            "Syncing remote dataset by ID is not yet supported".int_err(),
        )),
        odf::DatasetRefRemote::Alias(alias)
        | odf::DatasetRefRemote::Handle(odf::metadata::DatasetHandleRemote { alias, .. }) => {
            let mut repo = remote_repo_registry.get_repository(&alias.repo_name)?;

            repo.url.ensure_trailing_slash();
            Ok(repo.url.join(&format!("{}/", alias.local_alias())).unwrap())
        }
        odf::DatasetRefRemote::Url(url) => {
            let mut dataset_url = url.as_ref().clone();
            dataset_url.ensure_trailing_slash();
            Ok(dataset_url)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
