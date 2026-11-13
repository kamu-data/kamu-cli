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
    RemoteDatasetUrlResolver,
    SyncError,
    SyncRefRemote,
    SyncToRequest,
    try_resolve_head,
};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
pub struct SyncRequestBuilder {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    remote_dataset_url_resolver: Arc<dyn RemoteDatasetUrlResolver>,
}

#[common_macros::method_names_consts]
impl SyncRequestBuilder {
    /// Builds a push of a local dataset to a remote destination
    #[tracing::instrument(name = SyncRequestBuilder_build_sync_request, level = "debug", skip_all, fields(%src_ref, %dst_ref, %create_dst_if_not_exists))]
    pub async fn build_sync_request(
        &self,
        src_ref: odf::DatasetRef,
        dst_ref: odf::DatasetRefRemote,
        create_dst_if_not_exists: bool,
    ) -> Result<SyncToRequest, SyncError> {
        let src = self.dataset_registry.get_dataset_by_ref(&src_ref).await?;
        self.ensure_dataset_head_present(src_ref.as_any_ref(), src.as_ref())
            .await?;

        let dst = self
            .resolve_dest_sync_ref_remote(dst_ref, create_dst_if_not_exists)
            .await?;

        Ok(SyncToRequest { src, dst })
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
