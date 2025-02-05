// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use url::Url;

use super::DatabaseBackedMetadataChain;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedDataset {
    storage_dataset: Arc<dyn odf::Dataset>,
    db_backed_metadata_chain: Arc<dyn odf::MetadataChain>,
}

impl DatabaseBackedDataset {
    pub fn new(storage_dataset: Arc<dyn odf::Dataset>) -> Self {
        let db_backed_metadata_chain = Arc::new(DatabaseBackedMetadataChain::new(
            storage_dataset.as_metadata_chain_ptr(),
        ));

        Self {
            storage_dataset,
            db_backed_metadata_chain,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::Dataset for DatabaseBackedDataset {
    /// Helper function to append a generic event to metadata chain.
    ///
    /// Warning: Don't use when synchronizing blocks from another dataset.
    async fn commit_event(
        &self,
        event: odf::MetadataEvent,
        opts: odf::dataset::CommitOpts<'_>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        self.storage_dataset.commit_event(event, opts).await
    }

    /// Helper function to commit [AddData] event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_add_data(
        &self,
        add_data: odf::dataset::AddDataParams,
        data: Option<file_utils::OwnedFile>,
        checkpoint: Option<odf::dataset::CheckpointRef>,
        opts: odf::dataset::CommitOpts<'_>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        self.storage_dataset
            .commit_add_data(add_data, data, checkpoint, opts)
            .await
    }

    /// Helper function to commit [ExecuteTransform] event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_execute_transform(
        &self,
        execute_transform: odf::dataset::ExecuteTransformParams,
        data: Option<file_utils::OwnedFile>,
        checkpoint: Option<odf::dataset::CheckpointRef>,
        opts: odf::dataset::CommitOpts<'_>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        self.storage_dataset
            .commit_execute_transform(execute_transform, data, checkpoint, opts)
            .await
    }

    /// Helper function to prepare [ExecuteTransform] event without committing
    /// it.
    async fn prepare_execute_transform(
        &self,
        execute_transform: odf::dataset::ExecuteTransformParams,
        data: Option<&file_utils::OwnedFile>,
        checkpoint: Option<&odf::dataset::CheckpointRef>,
    ) -> Result<odf::metadata::ExecuteTransform, InternalError> {
        self.storage_dataset
            .prepare_execute_transform(execute_transform, data, checkpoint)
            .await
    }

    fn get_storage_internal_url(&self) -> &Url {
        self.storage_dataset.get_storage_internal_url()
    }

    fn as_metadata_chain(&self) -> &dyn odf::MetadataChain {
        self.db_backed_metadata_chain.as_ref()
    }

    fn as_metadata_chain_ptr(&self) -> Arc<dyn odf::MetadataChain> {
        self.db_backed_metadata_chain.clone()
    }

    fn as_data_repo(&self) -> &dyn odf::storage::ObjectRepository {
        self.storage_dataset.as_data_repo()
    }

    fn as_checkpoint_repo(&self) -> &dyn odf::storage::ObjectRepository {
        self.storage_dataset.as_checkpoint_repo()
    }

    fn as_info_repo(&self) -> &dyn odf::storage::NamedObjectRepository {
        self.storage_dataset.as_info_repo()
    }

    /// Returns a brief summary of the dataset
    async fn get_summary(
        &self,
        opts: odf::dataset::GetSummaryOpts,
    ) -> Result<odf::DatasetSummary, odf::dataset::GetSummaryError> {
        self.storage_dataset.get_summary(opts).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
