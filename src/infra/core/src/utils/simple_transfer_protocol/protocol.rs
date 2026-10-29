// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ErrorIntoInternal;
use kamu_core::*;
use odf::dataset::{
    MetadataChainExt as _,
    MetadataChainImpl,
    MetadataChainReferenceRepositoryImpl,
};
use odf::storage::inmem::{NamedObjectRepositoryInMemory, ObjectRepositoryInMemory};
use odf::storage::{MetadataBlockRepositoryImpl, ReferenceRepositoryImpl};

use super::progress::CompareChainsListenerAdapter;
use crate::utils::local_sync_writes::*;
use crate::utils::sync_increment::sync_increment;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: &str =
    "SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS";
const DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: usize = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct SimpleProtocolTransferOptions {
    pub max_parallel_transfers: usize,
    pub visibility_for_created_dataset: odf::DatasetVisibility,
}

impl Default for SimpleProtocolTransferOptions {
    fn default() -> Self {
        let max_parallel_transfers =
            match std::env::var(ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS) {
                Ok(string_value) => string_value
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS),
                _ => DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS,
            };
        Self {
            max_parallel_transfers,
            visibility_for_created_dataset: odf::DatasetVisibility::Private,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implements "Simple Transfer Protocol" as described in ODF spec
#[dill::component]
pub struct SimpleTransferProtocol {
    catalog: dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SimpleTransferProtocol {
    /// Pushes into a dataset that is not a dataset of this node.
    /// Moves its `HEAD` by the storage's own compare-and-swap from the head
    /// read at the start
    pub async fn push(
        &self,
        src_ref: &odf::DatasetRefAny,
        src: Arc<dyn odf::Dataset>,
        dst: Arc<dyn odf::Dataset>,
        validation: odf::dataset::AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        transfer_options: SimpleProtocolTransferOptions,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncResult, SyncError> {
        let dst_head = try_resolve_head(dst.as_metadata_chain()).await?;

        let listener_adapter = CompareChainsListenerAdapter::new(listener.clone());
        let Some((src_head, blocks_desc_ordered)) = Self::collect_new_blocks(
            src_ref,
            src.as_metadata_chain(),
            dst.as_metadata_chain(),
            dst_head.as_ref(),
            force,
            &listener_adapter,
        )
        .await?
        else {
            return Ok(SyncResult::UpToDate);
        };

        let num_blocks = u64::try_from(blocks_desc_ordered.len()).unwrap();

        // Read before the destination head moves, so a failure here aborts the push
        let increment =
            sync_increment(src.as_metadata_chain(), dst_head.as_ref(), &src_head).await?;

        let stats = self
            .transfer_objects(
                &blocks_desc_ordered,
                src.as_ref(),
                dst.as_ref(),
                trust_source_hashes,
                listener.clone(),
                &transfer_options,
                listener_adapter.into_status(),
            )
            .await?;

        Self::append_to_remote(
            dst.as_ref(),
            blocks_desc_ordered,
            odf::dataset::AppendBatchOpts {
                validation,
                trust_hashes: trust_source_hashes,
            },
            dst_head.as_ref(),
        )
        .await?;
        Self::report_blocks_written(listener.as_ref(), stats, num_blocks);

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
            increment,
        })
    }

    /// Writes the blocks and objects a local dataset lacks without moving its
    /// `HEAD`. A new dataset is stored from the source's `Seed`, without an
    /// entry or `HEAD`, for the commit to create
    pub async fn transfer_into_local(
        &self,
        src_ref: &odf::DatasetRefAny,
        src: Arc<dyn odf::Dataset>,
        dst: &LocalSyncTarget,
        validation: odf::dataset::AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        transfer_options: SimpleProtocolTransferOptions,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncTransfer, SyncError> {
        let empty_chain = MetadataChainImpl::new(
            MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
            MetadataChainReferenceRepositoryImpl::new(ReferenceRepositoryImpl::new(
                NamedObjectRepositoryInMemory::new(),
            )),
        );

        let (dst_chain, dst_head) = match dst {
            LocalSyncTarget::Existing {
                dataset,
                expected_head,
            } => (dataset.as_metadata_chain(), expected_head.as_ref()),
            LocalSyncTarget::New(_) => (&empty_chain as &dyn odf::MetadataChain, None),
        };

        let listener_adapter = CompareChainsListenerAdapter::new(listener.clone());
        let Some((src_head, mut blocks_desc_ordered)) = Self::collect_new_blocks(
            src_ref,
            src.as_metadata_chain(),
            dst_chain,
            dst_head,
            force,
            &listener_adapter,
        )
        .await?
        else {
            return Ok(SyncTransfer::UpToDate);
        };

        let num_blocks = u64::try_from(blocks_desc_ordered.len()).unwrap();

        // Safety: there are blocks, oldest last
        let (dst_dataset, target) = prepare_transfer_target(
            &self.catalog,
            dst,
            transfer_options.visibility_for_created_dataset,
            || blocks_desc_ordered.pop().unwrap(),
        )
        .await?;

        let stats = self
            .transfer_objects(
                &blocks_desc_ordered,
                src.as_ref(),
                dst_dataset.as_ref(),
                trust_source_hashes,
                listener.clone(),
                &transfer_options,
                listener_adapter.into_status(),
            )
            .await?;

        append_transferred_blocks(
            dst_dataset.as_ref(),
            Box::new(blocks_desc_ordered.into_iter().rev()),
            odf::dataset::AppendBatchOpts {
                validation,
                trust_hashes: trust_source_hashes,
            },
        )
        .await?;
        Self::report_blocks_written(listener.as_ref(), stats, num_blocks);

        let increment =
            sync_increment(dst_dataset.as_metadata_chain(), dst_head, &src_head).await?;

        Ok(SyncTransfer::Transferred(TransferredSync {
            target,
            new_head: src_head,
            num_blocks,
            increment,
        }))
    }

    /// The `HEAD` of a remote repository is not a dataset reference of this
    /// node, so the chain moves it by its own compare-and-set rather than
    /// through `DatasetHeadCommitter`
    async fn append_to_remote(
        dst: &dyn odf::Dataset,
        blocks_desc_ordered: Vec<odf::dataset::HashedMetadataBlockBytesDecoded>,
        append_opts: odf::dataset::AppendBatchOpts,
        expected_head: Option<&odf::Multihash>,
    ) -> Result<(), SyncError> {
        use odf::dataset::{AppendBatchAndSetHeadError as E, SetChainRefError};

        dst.as_metadata_chain()
            .append_batch_and_set_head(
                Box::new(blocks_desc_ordered.into_iter().rev()),
                append_opts,
                expected_head,
            )
            .await
            .map_err(|e| match e {
                E::Append(e) => map_append_batch_error(e),
                E::SetHead(SetChainRefError::CASFailed(e)) => SyncError::HeadMoved(e.into()),
                E::SetHead(SetChainRefError::Access(e)) => SyncError::Access(e),
                E::SetHead(
                    e @ (SetChainRefError::Internal(_) | SetChainRefError::BlockNotFound(_)),
                ) => SyncError::Internal(e.int_err()),
            })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
