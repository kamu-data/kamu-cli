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
use kamu_core::*;
use kamu_datasets::DatasetKeyBlocksRepository;
use opendatafabric::{self as odf, MetadataEventTypeFlags};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedDataset {
    storage_dataset: Arc<dyn Dataset>,
    acceptor: KeyCachedBlocksCurrentStateAcceptor,
}

impl DatabaseBackedDataset {
    pub fn new(
        dataset_key_blocks_repo: Arc<dyn DatasetKeyBlocksRepository>,
        storage_dataset: Arc<dyn Dataset>,
        dataset_id: odf::DatasetID,
    ) -> Self {
        let acceptor = KeyCachedBlocksCurrentStateAcceptor::new(
            dataset_key_blocks_repo,
            storage_dataset.clone(),
            dataset_id,
        );

        Self {
            storage_dataset,
            acceptor,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Dataset for DatabaseBackedDataset {
    /// Helper function to append a generic event to metadata chain.
    ///
    /// Warning: Don't use when synchronizing blocks from another dataset.
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

    fn as_current_state_acceptor(&self) -> &dyn DatasetCurrentStateAcceptor {
        &self.acceptor
    }

    fn as_metadata_chain(&self) -> &dyn MetadataChain {
        self.storage_dataset.as_metadata_chain()
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

    /// Returns a brief summary of the dataset
    async fn get_summary(&self, opts: GetSummaryOpts) -> Result<DatasetSummary, GetSummaryError> {
        self.storage_dataset.get_summary(opts).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct KeyCachedBlocksCurrentStateAcceptor {
    dataset_key_blocks_repo: Arc<dyn DatasetKeyBlocksRepository>,
    storage_dataset: Arc<dyn Dataset>,
    dataset_id: odf::DatasetID,
}

impl KeyCachedBlocksCurrentStateAcceptor {
    fn new(
        dataset_key_blocks_repo: Arc<dyn DatasetKeyBlocksRepository>,
        storage_dataset: Arc<dyn Dataset>,
        dataset_id: odf::DatasetID,
    ) -> Self {
        Self {
            dataset_key_blocks_repo,
            storage_dataset,
            dataset_id,
        }
    }
}

#[async_trait::async_trait]
impl DatasetCurrentStateAcceptor for KeyCachedBlocksCurrentStateAcceptor {
    /// A method of accepting Visitors ([MetadataChainVisitor]) that allows us
    /// to go through the metadata chain once and, if desired,
    /// bypassing blocks of no interest.
    async fn accept(
        &self,
        visitors: &mut [&mut dyn MetadataChainVisitor<Error = Infallible>],
    ) -> Result<(), AcceptVisitorError<InternalError>> {
        let mut all_flags = MetadataEventTypeFlags::empty();
        for visitor in visitors.iter() {
            if let MetadataVisitorDecision::NextOfType(flags) = visitor.initial_decision() {
                all_flags = all_flags.union(flags);
            }
        }

        if !all_flags.is_empty() {
            let known_rows = self
                .dataset_key_blocks_repo
                .try_loading_key_dataset_blocks(&self.dataset_id, all_flags)
                .await
                .map_err(AcceptVisitorError::Visitor)?;

            for known_row in known_rows {
                for visitor in visitors.iter_mut() {
                    match visitor.visit((&known_row.block_hash, &known_row.block)) {
                        Ok(_) => {}
                        Err(_) => unreachable!(),
                    }
                }
            }
        }

        self.storage_dataset
            .as_current_state_acceptor()
            .accept(visitors)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
