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
    LocalSyncRequest,
    LocalSyncTarget,
    RemoteAliasResolver,
    RemoteRepositoryRegistry,
    SyncError,
    SyncRef,
    SyncRefRemote,
    SyncRequest,
};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use url::Url;

use crate::UrlExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub struct SyncRequestBuilder {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    remote_alias_resolver: Arc<dyn RemoteAliasResolver>,
}

impl SyncRequestBuilder {
    /// Builds a sync into a destination that is not a dataset of this node
    #[tracing::instrument(level = "debug", skip_all, fields(?src_ref, ?dst_ref, %create_dst_if_not_exists))]
    pub async fn build_sync_request(
        &self,
        src_ref: odf::DatasetRefAny,
        dst_ref: odf::DatasetRefAny,
        create_dst_if_not_exists: bool,
    ) -> Result<SyncRequest, SyncError> {
        let src = self.resolve_source_sync_ref(&src_ref).await?;

        let remote_ref = match dst_ref
            .as_local_ref(|repo| self.remote_repo_registry.get_repository(repo).is_ok())
        {
            Ok(local_ref) => {
                return Err(format!(
                    "Destination {local_ref} is a local dataset, pull into it instead"
                )
                .int_err()
                .into());
            }
            Err(remote_ref) => remote_ref,
        };

        let dst = self
            .resolve_dest_sync_ref_remote(remote_ref, create_dst_if_not_exists)
            .await?;

        Ok(SyncRequest { src, dst })
    }

    /// Builds a pull into a dataset of this node. Reads the destination's
    /// `HEAD`, which the commit expects, so call it inside a transaction
    #[tracing::instrument(level = "debug", skip_all, fields(?src_ref, ?dst_ref, %create_dst_if_not_exists))]
    pub async fn build_local_sync_request(
        &self,
        src_ref: odf::DatasetRefAny,
        dst_ref: odf::DatasetRef,
        create_dst_if_not_exists: bool,
    ) -> Result<LocalSyncRequest, SyncError> {
        let src = self.resolve_source_sync_ref(&src_ref).await?;

        let dst = match self.dataset_registry.get_dataset_by_ref(&dst_ref).await {
            Ok(dataset) => LocalSyncTarget::existing(dataset).await?,
            Err(odf::DatasetRefUnresolvedError::NotFound(_)) if create_dst_if_not_exists => {
                match dst_ref.alias() {
                    Some(alias) => LocalSyncTarget::New(alias.clone()),
                    None => return Err(DatasetAnyRefUnresolvedError::new(dst_ref).into()),
                }
            }
            Err(err) => return Err(err.into()),
        };

        Ok(LocalSyncRequest { src, dst })
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

    async fn resolve_dest_sync_ref_remote(
        &self,
        remote_ref: odf::DatasetRefRemote,
        create_if_not_exists: bool,
    ) -> Result<SyncRefRemote, SyncError> {
        let remote_dataset_url = self
            .remote_alias_resolver
            .resolve_pull_url(&remote_ref)
            .await
            .int_err()?;

        let dataset = self
            .dataset_factory
            .get_dataset(&remote_dataset_url, create_if_not_exists)
            .await?;

        if !create_if_not_exists {
            self.ensure_dataset_head_present(remote_ref.as_any_ref(), dataset.as_ref())
                .await?;
        }

        Ok(SyncRefRemote {
            url: Arc::new(remote_dataset_url),
            dataset,
            original_remote_ref: remote_ref,
        })
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
            Err(odf::GetRefError::NotFound(_)) => {
                Err(DatasetAnyRefUnresolvedError { dataset_ref }.into())
            }
            Err(odf::GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
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
