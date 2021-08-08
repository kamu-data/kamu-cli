use std::assert_matches::assert_matches;

use kamu::domain::*;
use kamu::infra::*;
use kamu::testing::*;

use chrono::{TimeZone, Utc};

#[test]
fn test_create_new_chain() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain_dir = tmp_dir.path().join("foo.test");

    let block = MetadataFactory::metadata_block().build();

    let (chain, _) = MetadataChainImpl::create(&chain_dir, block.clone()).unwrap();

    assert_eq!(chain.iter_blocks().count(), 1);
}

#[test]
fn test_create_new_chain_error_dir_already_exists() {
    let tmp_dir = tempfile::tempdir().unwrap();

    let block = MetadataFactory::metadata_block().build();

    let res = MetadataChainImpl::create(tmp_dir.path(), block);
    assert_matches!(res, Err(InfraError::IOError { .. }));
}

#[test]
fn test_append_and_iter_blocks() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain_dir = tmp_dir.path().join("foo.test");

    let mut block1 = MetadataFactory::metadata_block()
        .system_time(Utc.ymd(2000, 1, 1).and_hms(12, 0, 0))
        .build();
    let (mut chain, block1_hash) = MetadataChainImpl::create(&chain_dir, block1.clone()).unwrap();
    block1.block_hash = block1_hash;

    let mut block2 = MetadataFactory::metadata_block()
        .system_time(Utc.ymd(2000, 1, 2).and_hms(12, 0, 0))
        .prev(&block1.block_hash)
        .build();
    block2.block_hash = chain.append(block2.clone());

    let mut block3 = MetadataFactory::metadata_block()
        .system_time(Utc.ymd(2000, 1, 3).and_hms(12, 0, 0))
        .prev(&block2.block_hash)
        .build();
    block3.block_hash = chain.append(block3.clone());

    let mut block_iter = chain.iter_blocks();
    assert_eq!(block_iter.next(), Some(block3));
    assert_eq!(block_iter.next(), Some(block2));
    assert_eq!(block_iter.next(), Some(block1));
    assert_eq!(block_iter.next(), None);
}
