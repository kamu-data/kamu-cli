// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use dill::Catalog;
use internal_error::InternalError;
use kamu_core::*;
use opendatafabric as odf;
use url::Url;

use super::{ManagedEntity, ManagedMetadataChainImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ManagedDatasetImpl {
    storage_dataset: Arc<dyn Dataset>,
    managed_chain: ManagedMetadataChainImpl,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ManagedDatasetImpl {
    pub fn new(storage_dataset: Arc<dyn Dataset>, dataset_id: odf::DatasetID) -> Self {
        let managed_chain = ManagedMetadataChainImpl::new(
            storage_dataset.clone(),
            dataset_id,
            BTreeMap::new(), // TODO
        );
        Self {
            storage_dataset,
            managed_chain,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsMetadataChain for ManagedDatasetImpl {
    fn as_metadata_chain(&self) -> &dyn MetadataChain {
        &self.managed_chain
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Dataset for ManagedDatasetImpl {
    async fn commit_event(
        &self,
        event: odf::MetadataEvent,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError> {
        self.storage_dataset.commit_event(event, opts).await
    }

    /// Helper function to commit [AddData] event into a local dataset.
    ///
    /// Will attempt to atomically move data and checkpoint files, so those have
    /// to be on the same file system as the workspace.
    async fn commit_add_data(
        &self,
        add_data: AddDataParams,
        data: Option<OwnedFile>,
        checkpoint: Option<CheckpointRef>,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError> {
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
        execute_transform: ExecuteTransformParams,
        data: Option<OwnedFile>,
        checkpoint: Option<CheckpointRef>,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError> {
        self.storage_dataset
            .commit_execute_transform(execute_transform, data, checkpoint, opts)
            .await
    }

    /// Helper function to prepare [ExecuteTransform] event without committing
    /// it.
    async fn prepare_execute_transform(
        &self,
        execute_transform: ExecuteTransformParams,
        data: Option<&OwnedFile>,
        checkpoint: Option<&CheckpointRef>,
    ) -> Result<odf::ExecuteTransform, InternalError> {
        self.storage_dataset
            .prepare_execute_transform(execute_transform, data, checkpoint)
            .await
    }

    fn get_storage_internal_url(&self) -> &Url {
        self.storage_dataset.get_storage_internal_url()
    }

    fn as_data_repo(&self) -> &dyn ObjectRepository {
        self.storage_dataset.as_data_repo()
    }

    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository {
        self.storage_dataset.as_checkpoint_repo()
    }

    fn as_info_repo(&self) -> &dyn NamedObjectRepository {
        self.storage_dataset.as_info_repo()
    }

    async fn get_summary(&self, opts: GetSummaryOpts) -> Result<DatasetSummary, GetSummaryError> {
        self.storage_dataset.get_summary(opts).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ManagedEntity for ManagedDatasetImpl {
    async fn do_commit(&self, transactional_catalog: &Catalog) -> Result<(), InternalError> {
        self.managed_chain.do_commit(transactional_catalog).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
