// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use cheap_clone::CheapClone;
use database_common::{InMemoryMapView, InMemoryTransactionalMap, TransactionId};
use kamu_datasets::DatasetBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A block by dataset, reference and sequence number
pub(crate) type DatasetBlockKey = (odf::DatasetID, odf::BlockRef, u64);

/// The blocks of every dataset reference, a key per block as a database keeps a
/// row per block: transactions saving blocks of one reference conflict only
/// over the same sequence number. Key blocks and data blocks are stored the
/// same way
pub(crate) type DatasetBlocksMap = InMemoryTransactionalMap<DatasetBlockKey, DatasetBlock>;

pub(crate) type DatasetBlocksView<'a> = InMemoryMapView<'a, DatasetBlockKey, DatasetBlock>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) trait DatasetBlocksViewExt<'a> {
    /// The blocks of the reference, ordered by sequence number
    fn blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Vec<&'a DatasetBlock>;
}

impl<'a> DatasetBlocksViewExt<'a> for DatasetBlocksView<'a> {
    fn blocks_for_ref(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Vec<&'a DatasetBlock> {
        let mut blocks: Vec<_> = self
            .iter()
            .filter(|((id, r, _), _)| id == dataset_id && r == block_ref)
            .map(|(_, block)| block)
            .collect();
        blocks.sort_by_key(|block| block.sequence_number);
        blocks
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The operations the key-block and data-block repositories share
pub(crate) trait DatasetBlocksMapExt {
    fn has_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> bool;

    fn all_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Vec<DatasetBlock>;

    /// Adds the blocks, or fails with a sequence number that is already saved
    fn save_blocks_batch(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        blocks: &[DatasetBlock],
    ) -> Result<(), u64>;

    fn delete_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    );

    fn delete_dataset_blocks(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
    );
}

impl DatasetBlocksMapExt for DatasetBlocksMap {
    fn has_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> bool {
        self.read(transaction_id, |blocks| {
            blocks
                .iter()
                .any(|((id, r, _), _)| id == dataset_id && r == block_ref)
        })
    }

    fn all_blocks_for_ref(
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

    fn save_blocks_batch(
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
        self.write(transaction_id, |all_blocks| {
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

    fn delete_blocks_for_ref(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) {
        self.write(transaction_id, |blocks| {
            blocks.delete_where(|(id, r, _)| id == dataset_id && r == block_ref);
        });
    }

    fn delete_dataset_blocks(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
    ) {
        self.write(transaction_id, |blocks| {
            blocks.delete_where(|(id, _, _)| id == dataset_id);
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
