// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cheap_clone::CheapClone;
use database_common::{
    InMemoryMapView,
    InMemoryPreparedCommit,
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    InMemoryWriteConflictError,
    TransactionId,
};
use kamu_datasets::DatasetBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A block by dataset, reference and sequence number
type DatasetBlockKey = (odf::DatasetID, odf::BlockRef, u64);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The blocks of every dataset reference, a key per block as a database keeps a
/// row per block: transactions saving blocks of one reference conflict only
/// over the same sequence number. Key blocks and data blocks are stored the
/// same way
pub(crate) struct DatasetBlocksMap(InMemoryTransactionalMap<DatasetBlockKey, DatasetBlock>);

impl DatasetBlocksMap {
    pub(crate) fn new() -> Self {
        Self(InMemoryTransactionalMap::new())
    }

    pub(crate) fn read<R>(
        &self,
        transaction_id: Option<TransactionId>,
        read: impl FnOnce(&DatasetBlocksView<'_, '_>) -> R,
    ) -> R {
        self.0
            .read(transaction_id, |blocks| read(&DatasetBlocksView(blocks)))
    }

    pub(crate) fn has_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> bool {
        self.0.read(transaction_id, |blocks| {
            blocks
                .iter()
                .any(|((id, r, _), _)| id == dataset_id && r == block_ref)
        })
    }

    pub(crate) fn all_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Vec<DatasetBlock> {
        self.read(transaction_id, |blocks| {
            blocks
                .blocks_for_ref(dataset_id, block_ref)
                .into_iter()
                .cloned()
                .collect()
        })
    }

    /// Adds the blocks, or fails with a sequence number that is already saved
    pub(crate) fn save_blocks_batch(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), u64> {
        if blocks.is_empty() {
            return Ok(());
        }

        let key_of = |block: &DatasetBlock| {
            (
                dataset_id.clone(),
                block_ref.cheap_clone(),
                block.sequence_number,
            )
        };
        self.0.write(transaction_id, |all_blocks| {
            let saved = all_blocks.view();
            if let Some(dup) = blocks.iter().find(|b| saved.contains_key(&key_of(b))) {
                return Err(dup.sequence_number);
            }

            for block in blocks {
                all_blocks.put(key_of(block), block.clone());
            }
            Ok(())
        })
    }

    pub(crate) fn delete_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) {
        self.0.write(transaction_id, |blocks| {
            blocks.delete_where(|(id, r, _)| id == dataset_id && r == block_ref);
        });
    }

    pub(crate) fn delete_dataset_blocks(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
    ) {
        self.0.write(transaction_id, |blocks| {
            blocks.delete_where(|(id, _, _)| id == dataset_id);
        });
    }
}

impl InMemoryTransactionParticipant for DatasetBlocksMap {
    fn begin(&self, transaction_id: TransactionId) {
        self.0.begin(transaction_id);
    }

    fn prepare_commit(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Box<dyn InMemoryPreparedCommit + '_>, InMemoryWriteConflictError> {
        self.0.prepare_commit(transaction_id)
    }

    fn rollback(&self, transaction_id: TransactionId) {
        self.0.rollback(transaction_id);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The blocks one transaction sees
pub(crate) struct DatasetBlocksView<'v, 'a>(&'v InMemoryMapView<'a, DatasetBlockKey, DatasetBlock>);

impl<'a> DatasetBlocksView<'_, 'a> {
    /// The blocks of the reference, ordered by sequence number
    pub(crate) fn blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Vec<&'a DatasetBlock> {
        let mut blocks: Vec<_> = self
            .0
            .iter()
            .filter(|((id, r, _), _)| id == dataset_id && r == block_ref)
            .map(|(_, block)| block)
            .collect();
        blocks.sort_by_key(|block| block.sequence_number);
        blocks
    }

    /// The blocks of every reference of the dataset, in no particular order
    pub(crate) fn dataset_blocks(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> impl Iterator<Item = &'a DatasetBlock> {
        self.0
            .iter()
            .filter(move |((id, _, _), _)| id == dataset_id)
            .map(|(_, block)| block)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
