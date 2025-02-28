// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use kamu_core::utils::metadata_chain_comparator::*;
use odf::dataset::{MetadataChainImpl, MetadataChainReferenceRepositoryImpl};
use odf::metadata::testing::MetadataFactory;
use odf::storage::inmem::{NamedObjectRepositoryInMemory, ObjectRepositoryInMemory};
use odf::storage::{MetadataBlockRepositoryImpl, ReferenceRepositoryImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn init_chain() -> impl odf::MetadataChain {
    let meta_block_repo = MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new());
    let meta_ref_repo = MetadataChainReferenceRepositoryImpl::new(ReferenceRepositoryImpl::new(
        NamedObjectRepositoryInMemory::new(),
    ));

    MetadataChainImpl::new(meta_block_repo, meta_ref_repo)
}

async fn push_block(chain: &dyn odf::MetadataChain, block: odf::MetadataBlock) -> odf::Multihash {
    chain
        .append(block, odf::dataset::AppendOpts::default())
        .await
        .unwrap()
}

async fn compare_chains(
    lhs_chain: &dyn odf::MetadataChain,
    rhs_chain: &dyn odf::MetadataChain,
) -> CompareChainsResult {
    let lhs_head = lhs_chain.resolve_ref(&odf::BlockRef::Head).await.unwrap();
    let rhs_head = match rhs_chain.resolve_ref(&odf::BlockRef::Head).await {
        Ok(h) => Some(h),
        Err(_) => None,
    };

    MetadataChainComparator::compare_chains(
        lhs_chain,
        &lhs_head,
        rhs_chain,
        rhs_head.as_ref(),
        &NullCompareChainsListener,
    )
    .await
    .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_same_seed_only() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();
    push_block(&lhs_chain, block_seed.clone()).await;
    push_block(&rhs_chain, block_seed).await;

    assert_eq!(
        CompareChainsResult::Equal,
        compare_chains(&lhs_chain, &rhs_chain).await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_different_seeds() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let lhs_block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();
    let rhs_block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();
    push_block(&lhs_chain, lhs_block_seed).await;
    push_block(&rhs_chain, rhs_block_seed).await;

    assert_matches!(
        compare_chains(&lhs_chain, &rhs_chain).await,
        CompareChainsResult::Divergence { uncommon_blocks_in_lhs, uncommon_blocks_in_rhs }
        if uncommon_blocks_in_lhs == 1 && uncommon_blocks_in_rhs == 1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_seed_only_in_first() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let lhs_block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();
    push_block(&lhs_chain, lhs_block_seed).await;

    assert_matches!(
        compare_chains(&lhs_chain, &rhs_chain).await,
        CompareChainsResult::LhsAhead { lhs_ahead_blocks }
        if lhs_ahead_blocks.len() == 1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_equal_multiple_blocks() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();
    let seed_hash = push_block(&lhs_chain, block_seed.clone()).await;
    push_block(&rhs_chain, block_seed).await;

    let block_schema = MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
        .prev(&seed_hash, 0)
        .build();

    let schema_hash = push_block(&lhs_chain, block_schema.clone()).await;
    push_block(&rhs_chain, block_schema).await;

    let block_data_1 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&schema_hash, 1)
    .build();

    let data_1_hash = push_block(&lhs_chain, block_data_1.clone()).await;
    push_block(&rhs_chain, block_data_1).await;

    let block_data_2 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&data_1_hash, 2)
    .build();
    push_block(&lhs_chain, block_data_2.clone()).await;
    push_block(&rhs_chain, block_data_2).await;

    assert_eq!(
        CompareChainsResult::Equal,
        compare_chains(&lhs_chain, &rhs_chain).await
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_one_ahead_another() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();

    let seed_hash = push_block(&lhs_chain, block_seed.clone()).await;
    push_block(&rhs_chain, block_seed).await;

    let block_schema = MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
        .prev(&seed_hash, 0)
        .build();

    let schema_hash = push_block(&lhs_chain, block_schema.clone()).await;
    push_block(&rhs_chain, block_schema).await;

    // Push data blocks only to RHS

    let block_data_1 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&schema_hash, 1)
    .build();
    let data_1_hash = push_block(&rhs_chain, block_data_1).await;

    let block_data_2 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&data_1_hash, 2)
    .build();
    push_block(&rhs_chain, block_data_2).await;

    assert_matches!(
        compare_chains(&lhs_chain, &rhs_chain).await,
        CompareChainsResult::LhsBehind { rhs_ahead_blocks }
        if rhs_ahead_blocks.len() == 2
    );

    assert_matches!(
        compare_chains(&rhs_chain, &lhs_chain).await,
        CompareChainsResult::LhsAhead { lhs_ahead_blocks }
        if lhs_ahead_blocks.len() == 2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_equal_length_divergence() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();

    let seed_hash = push_block(&lhs_chain, block_seed.clone()).await;
    push_block(&rhs_chain, block_seed).await;

    let block_schema = MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
        .prev(&seed_hash, 0)
        .build();

    let schema_hash = push_block(&lhs_chain, block_schema.clone()).await;
    push_block(&rhs_chain, block_schema).await;

    // Push two different data blocks after the same head

    let block1_data = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&schema_hash, 1)
    .build();
    push_block(&lhs_chain, block1_data).await;

    let block2_data = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&schema_hash, 1)
    .build();
    push_block(&rhs_chain, block2_data).await;

    assert_matches!(
        compare_chains(&lhs_chain, &rhs_chain).await,
        CompareChainsResult::Divergence { uncommon_blocks_in_lhs, uncommon_blocks_in_rhs }
        if uncommon_blocks_in_lhs == 1 && uncommon_blocks_in_rhs == 1
    );

    assert_matches!(
        compare_chains(&rhs_chain, &lhs_chain).await,
        CompareChainsResult::Divergence { uncommon_blocks_in_lhs, uncommon_blocks_in_rhs }
        if uncommon_blocks_in_lhs == 1 && uncommon_blocks_in_rhs == 1
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_different_length_divergence() {
    let lhs_chain = init_chain();
    let rhs_chain = init_chain();

    let block_seed =
        MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
            .build();

    let seed_hash = push_block(&lhs_chain, block_seed.clone()).await;
    push_block(&rhs_chain, block_seed).await;

    let block_schema = MetadataFactory::metadata_block(MetadataFactory::set_data_schema().build())
        .prev(&seed_hash, 0)
        .build();

    let schema_hash = push_block(&lhs_chain, block_schema.clone()).await;
    push_block(&rhs_chain, block_schema).await;

    // Push two data blocks to LHS and one unrelated block to RHS after the same
    // head

    let block1_data1 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&schema_hash, 1)
    .build();
    let block1_data1_hash = push_block(&lhs_chain, block1_data1).await;
    let block1_data2 = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(10, 19)
            .build(),
    )
    .prev(&block1_data1_hash, 2)
    .build();
    push_block(&lhs_chain, block1_data2).await;

    let block2_data = MetadataFactory::metadata_block(
        MetadataFactory::add_data()
            .new_offset_interval(0, 9)
            .build(),
    )
    .prev(&schema_hash, 1)
    .build();
    push_block(&rhs_chain, block2_data).await;

    assert_matches!(
        compare_chains(&lhs_chain, &rhs_chain).await,
        CompareChainsResult::Divergence { uncommon_blocks_in_lhs, uncommon_blocks_in_rhs }
        if uncommon_blocks_in_lhs == 2 && uncommon_blocks_in_rhs == 1
    );

    assert_matches!(
        compare_chains(&rhs_chain, &lhs_chain).await,
        CompareChainsResult::Divergence { uncommon_blocks_in_lhs, uncommon_blocks_in_rhs }
        if uncommon_blocks_in_lhs == 1 && uncommon_blocks_in_rhs == 2
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
