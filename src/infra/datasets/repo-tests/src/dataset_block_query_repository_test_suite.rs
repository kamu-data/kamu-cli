// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use dill::Catalog;
use kamu_datasets::*;

use crate::helpers::{
    init_dataset_entry,
    init_test_account,
    make_add_data_block,
    make_info_block,
    make_license_block,
    make_seed_block,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ALL_BLOCKS: PaginationOpts = PaginationOpts {
    limit: 100,
    offset: 0,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_blocks_unfiltered(catalog: &Catalog) {
    let harness = BlockQueryHarness::new(catalog).await;

    harness
        .assert_listing(
            &DatasetBlocksFilter::default(),
            ALL_BLOCKS,
            &[5, 4, 3, 2, 1, 0],
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_blocks_by_event_types(catalog: &Catalog) {
    let harness = BlockQueryHarness::new(catalog).await;

    // Key blocks only
    harness
        .assert_listing(
            &BlockQueryHarness::event_types_filter(
                odf::MetadataEventTypeFlags::SEED
                    | odf::MetadataEventTypeFlags::SET_INFO
                    | odf::MetadataEventTypeFlags::SET_LICENSE,
            ),
            ALL_BLOCKS,
            &[3, 1, 0],
        )
        .await;

    // Data blocks only
    harness
        .assert_listing(
            &BlockQueryHarness::event_types_filter(odf::MetadataEventTypeFlags::ADD_DATA),
            ALL_BLOCKS,
            &[5, 4, 2],
        )
        .await;

    // Both tables, interleaved
    harness
        .assert_listing(
            &BlockQueryHarness::event_types_filter(
                odf::MetadataEventTypeFlags::SET_LICENSE | odf::MetadataEventTypeFlags::ADD_DATA,
            ),
            ALL_BLOCKS,
            &[5, 4, 3, 2],
        )
        .await;

    // No blocks of this type
    harness
        .assert_listing(
            &BlockQueryHarness::event_types_filter(odf::MetadataEventTypeFlags::SET_VOCAB),
            ALL_BLOCKS,
            &[],
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_blocks_by_hash_prefix(catalog: &Catalog) {
    let harness = BlockQueryHarness::new(catalog).await;

    let data_block_hash = harness.block_hash(4).to_string();
    let key_block_hash = harness.block_hash(3).to_string();

    // A full hash matches exactly one block, in either table
    harness
        .assert_listing(
            &BlockQueryHarness::hash_prefix_filter(&data_block_hash),
            ALL_BLOCKS,
            &[4],
        )
        .await;
    harness
        .assert_listing(
            &BlockQueryHarness::hash_prefix_filter(&key_block_hash),
            ALL_BLOCKS,
            &[3],
        )
        .await;

    // The multihash header is shared by all blocks
    harness
        .assert_listing(
            &BlockQueryHarness::hash_prefix_filter("f1620"),
            ALL_BLOCKS,
            &[5, 4, 3, 2, 1, 0],
        )
        .await;

    // Odd and even digit counts select the same blocks as string matching
    for len in 6..=14 {
        let filter = BlockQueryHarness::hash_prefix_filter(&data_block_hash[..len]);
        let expected = harness.matching_sequence_numbers(&filter);
        assert!(expected.contains(&4));
        harness.assert_listing(&filter, ALL_BLOCKS, &expected).await;
    }

    // A prefix that disagrees with the multihash header
    harness
        .assert_listing(
            &BlockQueryHarness::hash_prefix_filter("f2"),
            ALL_BLOCKS,
            &[],
        )
        .await;

    // Conditions combine with AND
    harness
        .assert_listing(
            &DatasetBlocksFilter {
                event_types: Some(odf::MetadataEventTypeFlags::SET_INFO),
                block_hash_prefix: Some(BlockHashPrefix::parse(&data_block_hash).unwrap()),
            },
            ALL_BLOCKS,
            &[],
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_blocks_paginated(catalog: &Catalog) {
    let harness = BlockQueryHarness::new(catalog).await;

    harness
        .assert_listing(
            &DatasetBlocksFilter::default(),
            PaginationOpts {
                limit: 2,
                offset: 2,
            },
            &[3, 2],
        )
        .await;

    // Count stays the total of matching blocks, regardless of the page
    harness
        .assert_listing(
            &BlockQueryHarness::event_types_filter(odf::MetadataEventTypeFlags::ADD_DATA),
            PaginationOpts {
                limit: 2,
                offset: 1,
            },
            &[4, 2],
        )
        .await;

    harness
        .assert_listing(
            &DatasetBlocksFilter::default(),
            PaginationOpts {
                limit: 2,
                offset: 10,
            },
            &[],
        )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_list_blocks_of_unknown_dataset(catalog: &Catalog) {
    let repo = catalog
        .get_one::<dyn DatasetBlockQueryRepository>()
        .unwrap();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"unknown-ds");

    let blocks = repo
        .list_blocks(
            &dataset_id,
            &odf::BlockRef::Head,
            &DatasetBlocksFilter::default(),
            ALL_BLOCKS,
        )
        .await
        .unwrap();
    assert!(blocks.is_empty());

    let count = repo
        .count_blocks(
            &dataset_id,
            &odf::BlockRef::Head,
            &DatasetBlocksFilter::default(),
        )
        .await
        .unwrap();
    assert_eq!(count, 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A root dataset with key blocks at sequence numbers 0, 1, 3 and data blocks
/// at 2, 4, 5
struct BlockQueryHarness<'a> {
    catalog: &'a Catalog,
    dataset_id: odf::DatasetID,
    blocks: Vec<DatasetBlock>,
}

impl<'a> BlockQueryHarness<'a> {
    async fn new(catalog: &'a Catalog) -> Self {
        let (test_account_id, test_account_name) = init_test_account(catalog).await;

        let dataset_id = odf::DatasetID::new_seeded_ed25519(b"ds-block-query");
        let dataset_name = odf::DatasetName::new_unchecked("block-query-ds");

        init_dataset_entry(
            catalog,
            &test_account_id,
            &test_account_name,
            &dataset_id,
            &dataset_name,
            odf::DatasetKind::Root,
        )
        .await;

        let key_blocks = vec![
            make_seed_block(odf::DatasetKind::Root),
            make_info_block(1),
            make_license_block(3),
        ];
        let data_blocks = vec![
            make_add_data_block(2),
            make_add_data_block(4),
            make_add_data_block(5),
        ];

        catalog
            .get_one::<dyn DatasetKeyBlockRepository>()
            .unwrap()
            .save_key_blocks_batch(&dataset_id, &odf::BlockRef::Head, &key_blocks)
            .await
            .unwrap();
        catalog
            .get_one::<dyn DatasetDataBlockRepository>()
            .unwrap()
            .save_data_blocks_batch(&dataset_id, &odf::BlockRef::Head, &data_blocks)
            .await
            .unwrap();

        let mut blocks: Vec<_> = key_blocks.into_iter().chain(data_blocks).collect();
        blocks.sort_by_key(|block| block.sequence_number);

        Self {
            catalog,
            dataset_id,
            blocks,
        }
    }

    fn event_types_filter(event_types: odf::MetadataEventTypeFlags) -> DatasetBlocksFilter {
        DatasetBlocksFilter {
            event_types: Some(event_types),
            block_hash_prefix: None,
        }
    }

    fn hash_prefix_filter(prefix: &str) -> DatasetBlocksFilter {
        DatasetBlocksFilter {
            event_types: None,
            block_hash_prefix: Some(BlockHashPrefix::parse(prefix).unwrap()),
        }
    }

    fn block_hash(&self, sequence_number: u64) -> &odf::Multihash {
        &self.blocks[usize::try_from(sequence_number).unwrap()].block_hash
    }

    /// Sequence numbers of matching blocks, newest first
    fn matching_sequence_numbers(&self, filter: &DatasetBlocksFilter) -> Vec<u64> {
        self.blocks
            .iter()
            .rev()
            .filter(|block| filter.matches(&block.block_hash, block.event_kind))
            .map(|block| block.sequence_number)
            .collect()
    }

    /// Checks the listed page by sequence numbers, and that the count covers
    /// all matching blocks
    async fn assert_listing(
        &self,
        filter: &DatasetBlocksFilter,
        pagination: PaginationOpts,
        expected_sequence_numbers: &[u64],
    ) {
        let repo = self
            .catalog
            .get_one::<dyn DatasetBlockQueryRepository>()
            .unwrap();

        let blocks = repo
            .list_blocks(&self.dataset_id, &odf::BlockRef::Head, filter, pagination)
            .await
            .unwrap();

        let actual: Vec<_> = blocks
            .iter()
            .map(|block| (block.sequence_number, block.block_hash.clone()))
            .collect();
        let expected: Vec<_> = expected_sequence_numbers
            .iter()
            .map(|sequence_number| (*sequence_number, self.block_hash(*sequence_number).clone()))
            .collect();
        assert_eq!(actual, expected);

        let expected_count = self.matching_sequence_numbers(filter).len();
        let count = repo
            .count_blocks(&self.dataset_id, &odf::BlockRef::Head, filter)
            .await
            .unwrap();
        assert_eq!(count, expected_count);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
