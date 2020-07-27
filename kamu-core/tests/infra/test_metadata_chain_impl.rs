use kamu::domain::*;
use kamu::infra::*;
use kamu_test::*;

#[test]
fn test_block_hashing() {
    assert_eq!(
        MetadataChainImpl::block_hash(&MetadataFactory::metadata_block().build()),
        "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"
    );
    assert_eq!(
        MetadataChainImpl::block_hash(
            &MetadataFactory::metadata_block()
                .prev("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
                .build()
        ),
        "42012470e20e90036b3098da71e1056ce0e561031bc41fa47faa1d1269f93a2e"
    );
    // TODO: other fields
}

#[test]
fn test_create_new_chain() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain_dir = tmp_dir.path().join("foo.test");

    let block = MetadataFactory::metadata_block().build();

    let (chain, _) = MetadataChainImpl::create(chain_dir, block.clone()).unwrap();

    assert_eq!(chain.iter_blocks().count(), 1);
}

#[test]
fn test_create_new_chain_err() {
    let tmp_dir = tempfile::tempdir().unwrap();

    let block = MetadataFactory::metadata_block().build();

    let res = MetadataChainImpl::create(tmp_dir.path().to_owned(), block);
    assert_err!(res, InfraError::IOError { .. });
}

#[test]
fn test_append_and_iter_blocks() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let chain_dir = tmp_dir.path().join("foo.test");

    let mut block1 = MetadataFactory::metadata_block().build();
    let mut block2 = MetadataFactory::metadata_block().build();
    let mut block3 = MetadataFactory::metadata_block().build();

    let (mut chain, hash) = MetadataChainImpl::create(chain_dir, block1.clone()).unwrap();
    block1.block_hash = hash;
    block2.prev_block_hash = block1.block_hash.clone();
    block2.block_hash = chain.append(block2.clone());
    block3.prev_block_hash = block2.block_hash.clone();
    block3.block_hash = chain.append(block3.clone());

    let mut block_iter = chain.iter_blocks();
    assert_eq!(block_iter.next(), Some(block3));
    assert_eq!(block_iter.next(), Some(block2));
    assert_eq!(block_iter.next(), Some(block1));
    assert_eq!(block_iter.next(), None);
}
