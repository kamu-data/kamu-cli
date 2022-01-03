// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::*;
use opendatafabric::serde::flatbuffers::*;
use opendatafabric::*;

use std::io::prelude::*;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct MetadataChainImpl {
    meta_path: PathBuf,
}

// TODO: Error handling is very poor here
impl MetadataChainImpl {
    pub fn new(meta_path: &Path) -> Self {
        Self {
            meta_path: meta_path.to_owned(),
        }
    }

    pub fn create(
        meta_path: &Path,
        first_block: MetadataBlock,
    ) -> Result<(Self, Multihash), InfraError> {
        assert_eq!(
            first_block.prev_block_hash, None,
            "First block cannot reference previous blocks"
        );

        if let MetadataEvent::Seed(_) = first_block.event {
        } else {
            panic!("First block has to be a Seed")
        }

        std::fs::create_dir(&meta_path)?;
        std::fs::create_dir(meta_path.join("blocks"))?;
        std::fs::create_dir(meta_path.join("refs"))?;
        let mut chain = Self::new(meta_path);

        // TODO: Validate that first block contains source info, schema, etc.?
        let hash = chain.write_block_unchecked(&first_block)?;
        chain.write_ref(&BlockRef::Head, &hash)?;

        Ok((chain, hash))
    }

    pub fn from_blocks(
        meta_path: &Path,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(Self, Multihash), InfraError> {
        let first_block = blocks
            .next()
            .expect("Cannot create MetadataChain from empty set of blocks");

        let (mut chain, mut last_hash) = Self::create(meta_path, first_block)?;

        for b in blocks {
            last_hash = chain.append(b);
        }

        Ok((chain, last_hash))
    }

    fn read_block_unchecked(path: &Path) -> MetadataBlock {
        // TODO: Use mmap
        let buffer = std::fs::read(path).unwrap_or_else(|e| {
            panic!("Failed to open the block file at {}: {}", path.display(), e)
        });

        FlatbuffersMetadataBlockDeserializer
            .read_manifest(&buffer)
            .unwrap_or_else(|e| {
                panic!("Failed to open the block file at {}: {}", path.display(), e)
            })
    }

    fn write_block_unchecked(&mut self, block: &MetadataBlock) -> Result<Multihash, InfraError> {
        let buffer = FlatbuffersMetadataBlockSerializer
            .write_manifest(block)
            .unwrap();

        let hash = Multihash::from_digest_sha3_256(&buffer);

        let mut file = std::fs::File::options()
            .write(true)
            .create_new(true)
            .open(self.block_path(&hash))?;
        file.write_all(&buffer)?;

        Ok(hash)
    }

    // TODO: atomicity
    fn write_ref(&mut self, r: &BlockRef, hash: &Multihash) -> Result<(), InfraError> {
        let mut file = std::fs::File::create(self.ref_path(r))?;
        file.write_all(hash.to_multibase_string().as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn block_path(&self, hash: &Multihash) -> PathBuf {
        let mut p = self.meta_path.join("blocks");
        p.push(hash.to_multibase_string());
        p
    }

    fn ref_path(&self, r: &BlockRef) -> PathBuf {
        let ref_name = match r {
            BlockRef::Head => "head",
        };

        let mut p = self.meta_path.join("refs");
        p.push(ref_name);
        p
    }
}

impl MetadataChain for MetadataChainImpl {
    fn read_ref(&self, r: &BlockRef) -> Option<Multihash> {
        let path = self.ref_path(r);
        if !path.exists() {
            None
        } else {
            let s = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("Failed to read ref at {}: {}", path.display(), e));
            Some(Multihash::from_multibase_str(s.trim()).expect("Ref contains invalid multihash"))
        }
    }

    fn get_block(&self, block_hash: &Multihash) -> Option<MetadataBlock> {
        let path = self.block_path(block_hash);
        if !path.exists() {
            None
        } else {
            Some(Self::read_block_unchecked(&path))
        }
    }

    fn iter_blocks_starting(
        &self,
        block_hash: &Multihash,
    ) -> Option<Box<dyn Iterator<Item = (Multihash, MetadataBlock)>>> {
        if !self.block_path(block_hash).exists() {
            None
        } else {
            Some(Box::new(MetadataBlockIter {
                reader: BlockReader {
                    blocks_dir: self.meta_path.join("blocks"),
                },
                next_hash: Some(block_hash.clone()),
            }))
        }
    }

    fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> Multihash {
        let last_hash = self
            .read_ref(r)
            .unwrap_or_else(|| panic!("Ref {:?} does not exist", r));

        assert_eq!(
            block.prev_block_hash,
            Some(last_hash.clone()),
            "New block doesn't specify correct prev block hash"
        );

        match &block.event {
            MetadataEvent::Seed(_) => panic!("Only starting block can have a seed"),
            MetadataEvent::SetTransform(st) => {
                for input in &st.inputs {
                    assert!(
                        input.id.is_some(),
                        "Transform inputs must be resolved to DatasetIDs"
                    );
                }
            }
            _ => (),
        }

        let last_block = Self::read_block_unchecked(&self.block_path(&last_hash));

        assert!(
            last_block.system_time <= block.system_time,
            "New block's system_time must be greater or equal to the previous block's: {} < {}",
            block.system_time,
            last_block.system_time,
        );

        let hash = self.write_block_unchecked(&block).unwrap();
        self.write_ref(r, &hash).unwrap();

        hash
    }
}

struct BlockReader {
    blocks_dir: PathBuf,
}

impl BlockReader {
    fn read_block(&self, hash: &Multihash) -> MetadataBlock {
        let path = self.blocks_dir.join(hash.to_string());
        MetadataChainImpl::read_block_unchecked(&path)
    }
}

struct MetadataBlockIter {
    reader: BlockReader,
    next_hash: Option<Multihash>,
}

impl Iterator for MetadataBlockIter {
    type Item = (Multihash, MetadataBlock);

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_hash.clone() {
            None => None,
            Some(hash) => {
                let block = self.reader.read_block(&hash);
                self.next_hash = block.prev_block_hash.clone();
                Some((hash, block))
            }
        }
    }
}
