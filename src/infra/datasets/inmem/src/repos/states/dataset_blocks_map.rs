// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use cheap_clone::CheapClone;
use database_common::{InMemoryTransactionalMap, TransactionId};
use kamu_datasets::DatasetBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The blocks of each dataset reference, ordered by sequence number. Key
/// blocks and data blocks are stored the same way
pub(crate) type DatasetBlocksMap =
    InMemoryTransactionalMap<(odf::DatasetID, odf::BlockRef), Vec<DatasetBlock>>;

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
                .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                .is_some_and(|blocks| !blocks.is_empty())
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
                .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                .cloned()
                .unwrap_or_default()
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

        let key = (dataset_id.clone(), block_ref.cheap_clone());
        self.write(transaction_id, |all_blocks| {
            let mut saved = all_blocks.view().get(&key).cloned().unwrap_or_default();

            let existing: HashSet<_> = saved.iter().map(|b| b.sequence_number).collect();
            if let Some(dup) = blocks
                .iter()
                .find(|b| existing.contains(&b.sequence_number))
            {
                return Err(dup.sequence_number);
            }

            saved.extend_from_slice(blocks);
            saved.sort_by_key(|b| b.sequence_number);
            all_blocks.put(key, saved);
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
            blocks.delete((dataset_id.clone(), block_ref.cheap_clone()));
        });
    }

    fn delete_dataset_blocks(
        &self,
        transaction_id: Option<TransactionId>,
        dataset_id: &odf::DatasetID,
    ) {
        self.write(transaction_id, |blocks| {
            blocks.delete_where(|(id, _)| id == dataset_id);
        });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
