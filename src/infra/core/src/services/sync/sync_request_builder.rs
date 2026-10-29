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
use kamu_core::{
    DatasetAnyRefUnresolvedError,
    InvalidSyncDestinationError,
    RemoteDatasetUrlResolver,
    RemoteRepositoryRegistry,
    SyncError,
    SyncRef,
    SyncRefRemote,
    SyncRequest,
    try_resolve_head,
};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub struct SyncRequestBuilder {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    remote_repo_registry: Arc<dyn RemoteRepositoryRegistry>,
    remote_dataset_url_resolver: Arc<dyn RemoteDatasetUrlResolver>,
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
                return Err(InvalidSyncDestinationError {
                    dst_ref: local_ref.as_any_ref(),
                    reason: "it is a local dataset, pull into it instead".to_owned(),
                }
                .into());
            }
            Err(remote_ref) => remote_ref,
        };

        let dst = self
            .resolve_dest_sync_ref_remote(remote_ref, create_dst_if_not_exists)
            .await?;

        Ok(SyncRequest { src, dst })
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
                let url = self
                    .remote_dataset_url_resolver
                    .resolve_dataset_url(&remote_ref)
                    .await?;
                self.remote_source_sync_ref(remote_ref, url).await
            }
        }
    }

    async fn remote_source_sync_ref(
        &self,
        remote_ref: odf::DatasetRefRemote,
        url: Url,
    ) -> Result<SyncRef, SyncError> {
        let url = Arc::new(url);
        let dataset = self
            .dataset_factory
            .get_dataset(url.as_ref(), false)
            .await?;
        self.ensure_dataset_head_present(odf::DatasetRefAny::Url(url.clone()), dataset.as_ref())
            .await?;
        Ok(SyncRef::Remote(SyncRefRemote {
            url,
            dataset,
            original_remote_ref: remote_ref,
        }))
    }

    async fn resolve_dest_sync_ref_remote(
        &self,
        remote_ref: odf::DatasetRefRemote,
        create_if_not_exists: bool,
    ) -> Result<SyncRefRemote, SyncError> {
        let remote_dataset_url = self
            .remote_dataset_url_resolver
            .resolve_dataset_url(&remote_ref)
            .await?;

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
        try_resolve_head(dataset.as_metadata_chain())
            .await?
            .ok_or_else(|| DatasetAnyRefUnresolvedError { dataset_ref }.into())
            .map(|_| ())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
