use crate::domain::*;
use crate::infra::*;
use opendatafabric::serde::yaml::*;
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
    ) -> Result<(Self, Sha3_256), InfraError> {
        assert_eq!(first_block.prev_block_hash, None);

        std::fs::create_dir(&meta_path)?;
        std::fs::create_dir(meta_path.join("blocks"))?;
        std::fs::create_dir(meta_path.join("refs"))?;
        let mut chain = Self::new(meta_path);

        // TODO: Validate that first block contains source info, schema, etc.?
        let hash = chain.write_block(&first_block)?;
        chain.write_ref(&BlockRef::Head, &hash)?;

        Ok((chain, hash))
    }

    pub fn from_blocks(
        meta_path: &Path,
        blocks: &mut dyn Iterator<Item = MetadataBlock>,
    ) -> Result<(Self, Sha3_256), InfraError> {
        std::fs::create_dir(&meta_path)?;
        std::fs::create_dir(meta_path.join("blocks"))?;
        std::fs::create_dir(meta_path.join("refs"))?;

        let mut chain = Self::new(meta_path);
        let mut last_hash = Sha3_256::zero();

        for b in blocks {
            last_hash = if last_hash.is_zero() {
                assert_eq!(b.prev_block_hash, None);
                let hash = chain.write_block(&b)?;
                chain.write_ref(&BlockRef::Head, &hash)?;
                hash
            } else {
                chain.append(b)
            };
        }

        assert!(!last_hash.is_zero());
        Ok((chain, last_hash))
    }

    fn read_block(path: &Path) -> MetadataBlock {
        // TODO: Use mmap
        let buffer = std::fs::read(path).unwrap_or_else(|e| {
            panic!("Failed to open the block file at {}: {}", path.display(), e)
        });

        YamlMetadataBlockDeserializer
            .read_manifest(&buffer)
            .unwrap_or_else(|e| {
                panic!("Failed to open the block file at {}: {}", path.display(), e)
            })
    }

    fn write_block(&mut self, block: &MetadataBlock) -> Result<Sha3_256, InfraError> {
        let (hash, buffer) = YamlMetadataBlockSerializer.write_manifest(block).unwrap();

        let mut file = std::fs::File::with_options()
            .write(true)
            .create_new(true)
            .open(self.block_path(&hash))?;
        file.write_all(&buffer)?;

        Ok(hash)
    }

    // TODO: atomicity
    fn write_ref(&mut self, r: &BlockRef, hash: &Sha3_256) -> Result<(), InfraError> {
        let mut file = std::fs::File::create(self.ref_path(r))?;
        file.write_all(hash.to_string().as_bytes())?;
        file.sync_all()?;
        Ok(())
    }

    fn block_path(&self, hash: &Sha3_256) -> PathBuf {
        let mut p = self.meta_path.join("blocks");
        p.push(hash.to_string());
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
    fn read_ref(&self, r: &BlockRef) -> Option<Sha3_256> {
        let path = self.ref_path(r);
        if !path.exists() {
            None
        } else {
            let s = std::fs::read_to_string(&path)
                .unwrap_or_else(|e| panic!("Failed to read ref at {}: {}", path.display(), e));
            Some(Sha3_256::from_str(s.trim()).unwrap())
        }
    }

    fn get_block(&self, block_hash: &Sha3_256) -> Option<MetadataBlock> {
        let path = self.block_path(block_hash);
        if !path.exists() {
            None
        } else {
            Some(Self::read_block(&path))
        }
    }

    fn iter_blocks_starting(
        &self,
        block_hash: &Sha3_256,
    ) -> Option<Box<dyn Iterator<Item = MetadataBlock>>> {
        if !self.block_path(block_hash).exists() {
            None
        } else {
            Some(Box::new(MetadataBlockIter {
                reader: BlockReader {
                    blocks_dir: self.meta_path.join("blocks"),
                },
                next_hash: Some(*block_hash),
            }))
        }
    }

    fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> Sha3_256 {
        let last_hash = self
            .read_ref(r)
            .unwrap_or_else(|| panic!("Ref {:?} does not exist", r));

        assert_eq!(
            block.prev_block_hash,
            Some(last_hash),
            "New block doesn't specify correct prev block hash"
        );

        let last_block = Self::read_block(&self.block_path(&last_hash));

        assert!(
            last_block.system_time < block.system_time,
            "New block's system_time must be greater than the previous block's: {} <= {}",
            block.system_time,
            last_block.system_time,
        );

        match (
            &block.source,
            &block.vocab,
            &block.input_slices,
            &block.output_slice,
            &block.output_watermark,
        ) {
            (None, None, None, None, None) => panic!("Block cannot be empty: {:?}", block),
            (_, _, Some(_), None, None) => panic!("Block has an input but no outputs: {:?}", block),
            _ => (),
        }

        let hash = self.write_block(&block).unwrap();
        self.write_ref(r, &hash).unwrap();

        hash
    }
}

struct BlockReader {
    blocks_dir: PathBuf,
}

impl BlockReader {
    fn read_block(&self, hash: &Sha3_256) -> MetadataBlock {
        let path = self.blocks_dir.join(hash.to_string());
        MetadataChainImpl::read_block(&path)
    }
}

struct MetadataBlockIter {
    reader: BlockReader,
    next_hash: Option<Sha3_256>,
}

impl Iterator for MetadataBlockIter {
    type Item = MetadataBlock;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_hash {
            None => None,
            Some(ref hash) => {
                let block = self.reader.read_block(hash);
                self.next_hash = block.prev_block_hash;
                Some(block)
            }
        }
    }
}
