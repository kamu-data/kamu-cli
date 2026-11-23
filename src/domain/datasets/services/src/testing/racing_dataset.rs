// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use file_utils::OwnedFile;
use internal_error::InternalError;
use url::Url;

use super::DatasetReferenceRace;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A dataset that works like the one it wraps, but is its own chain, whose
/// `set_ref` first runs what the [`DatasetReferenceRace`] has armed
pub struct RacingDataset {
    inner: Arc<dyn odf::Dataset>,
    dataset_id: odf::DatasetID,
    race: Arc<DatasetReferenceRace>,
}

impl RacingDataset {
    pub fn wrap(
        inner: Arc<dyn odf::Dataset>,
        dataset_id: &odf::DatasetID,
        race: &Arc<DatasetReferenceRace>,
    ) -> Arc<dyn odf::Dataset> {
        Arc::new(Self {
            inner,
            dataset_id: dataset_id.clone(),
            race: race.clone(),
        })
    }

    fn chain(&self) -> &dyn odf::MetadataChain {
        self.inner.as_metadata_chain()
    }
}

#[async_trait::async_trait]
impl odf::Dataset for RacingDataset {
    fn detach_from_transaction(&self) {
        self.inner.detach_from_transaction();
    }

    async fn commit_event<'a>(
        &self,
        event: odf::MetadataEvent,
        opts: odf::dataset::CommitOpts<'a>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        self.inner.commit_event(event, opts).await
    }

    async fn commit_add_data<'a>(
        &self,
        add_data: odf::dataset::AddDataParams,
        data: Option<OwnedFile>,
        checkpoint: Option<odf::dataset::CheckpointRef>,
        opts: odf::dataset::CommitOpts<'a>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        self.inner
            .commit_add_data(add_data, data, checkpoint, opts)
            .await
    }

    async fn commit_execute_transform<'a>(
        &self,
        execute_transform: odf::dataset::ExecuteTransformParams,
        data: Option<OwnedFile>,
        checkpoint: Option<odf::dataset::CheckpointRef>,
        opts: odf::dataset::CommitOpts<'a>,
    ) -> Result<odf::dataset::CommitResult, odf::dataset::CommitError> {
        self.inner
            .commit_execute_transform(execute_transform, data, checkpoint, opts)
            .await
    }

    async fn prepare_execute_transform<'a>(
        &self,
        execute_transform: odf::dataset::ExecuteTransformParams,
        data: Option<&'a OwnedFile>,
        checkpoint: Option<&'a odf::dataset::CheckpointRef>,
    ) -> Result<odf::metadata::ExecuteTransform, InternalError> {
        self.inner
            .prepare_execute_transform(execute_transform, data, checkpoint)
            .await
    }

    fn get_storage_internal_url(&self) -> &Url {
        self.inner.get_storage_internal_url()
    }

    fn as_metadata_chain(&self) -> &dyn odf::MetadataChain {
        self
    }

    fn as_data_repo(&self) -> &dyn odf::storage::ObjectRepository {
        self.inner.as_data_repo()
    }

    fn as_checkpoint_repo(&self) -> &dyn odf::storage::ObjectRepository {
        self.inner.as_checkpoint_repo()
    }

    fn as_info_repo(&self) -> &dyn odf::storage::NamedObjectRepository {
        self.inner.as_info_repo()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl odf::MetadataChain for RacingDataset {
    fn detach_from_transaction(&self) {
        self.chain().detach_from_transaction();
    }

    async fn contains_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<bool, odf::storage::ContainsBlockError> {
        self.chain().contains_block(hash).await
    }

    async fn get_block_size(
        &self,
        hash: &odf::Multihash,
    ) -> Result<u64, odf::storage::GetBlockDataError> {
        self.chain().get_block_size(hash).await
    }

    async fn get_block_bytes(
        &self,
        hash: &odf::Multihash,
    ) -> Result<odf::MetadataBlockBytes, odf::storage::GetBlockDataError> {
        self.chain().get_block_bytes(hash).await
    }

    async fn get_block(
        &self,
        hash: &odf::Multihash,
    ) -> Result<odf::MetadataBlock, odf::GetBlockError> {
        self.chain().get_block(hash).await
    }

    async fn get_preceding_block_with_hint(
        &self,
        head_block: &odf::MetadataBlock,
        tail_sequence_number: Option<u64>,
        hint: odf::dataset::MetadataVisitorDecision,
    ) -> Result<Option<(odf::Multihash, odf::MetadataBlock)>, odf::GetBlockError> {
        self.chain()
            .get_preceding_block_with_hint(head_block, tail_sequence_number, hint)
            .await
    }

    fn iter_blocks_interval<'a>(
        &'a self,
        head_boundary: odf::dataset::MetadataChainIterBoundary<'a>,
        tail_boundary: Option<odf::dataset::MetadataChainIterBoundary<'a>>,
        ignore_missing_tail: bool,
    ) -> odf::dataset::DynMetadataStream<'a> {
        self.chain()
            .iter_blocks_interval(head_boundary, tail_boundary, ignore_missing_tail)
    }

    fn iter_blocks_bytes_interval<'a>(
        &'a self,
        head_boundary: odf::dataset::MetadataChainIterBoundary<'a>,
        tail_boundary: Option<odf::dataset::MetadataChainIterBoundary<'a>>,
        ignore_missing_tail: bool,
    ) -> odf::dataset::DynMetadataBytesStream<'a> {
        self.chain()
            .iter_blocks_bytes_interval(head_boundary, tail_boundary, ignore_missing_tail)
    }

    async fn append<'a>(
        &'a self,
        block: odf::MetadataBlock,
        opts: odf::dataset::AppendOpts<'a>,
    ) -> Result<odf::Multihash, odf::dataset::AppendError> {
        self.chain().append(block, opts).await
    }

    async fn resolve_ref(&self, r: &odf::BlockRef) -> Result<odf::Multihash, odf::GetRefError> {
        self.chain().resolve_ref(r).await
    }

    async fn set_ref(
        &self,
        r: &odf::BlockRef,
        expected: Option<&odf::Multihash>,
        hash: &odf::Multihash,
        opts: odf::dataset::SetRefOpts,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        self.race.before_update(&self.dataset_id, r).await?;
        self.chain().set_ref(r, expected, hash, opts).await
    }

    fn as_uncached_chain(&self) -> &dyn odf::MetadataChain {
        self.chain().as_uncached_chain()
    }

    fn as_uncached_ref_repo(&self) -> &dyn odf::storage::ReferenceRepository {
        self.chain().as_uncached_ref_repo()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
