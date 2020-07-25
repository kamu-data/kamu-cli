use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

use crypto::digest::Digest;
use crypto::sha3::Sha3;
use std::io::prelude::*;
use std::path::PathBuf;

pub struct MetadataChainImpl {
  meta_path: PathBuf,
}

impl MetadataChainImpl {
  pub fn new(meta_path: PathBuf) -> Self {
    Self {
      meta_path: meta_path,
    }
  }

  pub fn create(
    meta_path: PathBuf,
    first_block: MetadataBlock,
  ) -> Result<(Self, String), InfraError> {
    std::fs::create_dir(&meta_path)?;
    std::fs::create_dir(meta_path.join("blocks"))?;
    std::fs::create_dir(meta_path.join("refs"))?;

    let mut chain = Self::new(meta_path);
    let first_block_hashed = chain.hashed(first_block);

    chain.write_block(&first_block_hashed)?;
    chain.write_ref(&BlockRef::Head, &first_block_hashed.block_hash)?;

    Ok((chain, first_block_hashed.block_hash))
  }

  pub fn block_hash(block: &MetadataBlock) -> String {
    let mut digest = Sha3::sha3_256();
    // TODO: use generated hashers
    digest.input_str(&block.prev_block_hash);
    digest.result_str()
  }

  fn hashed(&self, block: MetadataBlock) -> MetadataBlock {
    assert!(block.block_hash.is_empty(), "Got an already hashed block");
    let mut b = block;
    b.block_hash = Self::block_hash(&b);
    b
  }

  fn read_block(&self, hash: &str) -> MetadataBlock {
    let path = self.block_path(hash);

    let file = std::fs::File::open(path.clone())
      .unwrap_or_else(|e| panic!("Failed to open the block file at {}: {}", path.display(), e));

    let manifest: Manifest<MetadataBlock> = serde_yaml::from_reader(&file).unwrap_or_else(|e| {
      panic!(
        "Failed to deserialize the MetadataBlock at {}: {}",
        path.display(),
        e
      )
    });

    assert_eq!(manifest.kind, "MetadataBlock");
    manifest.content
  }

  fn write_block(&mut self, block: &MetadataBlock) -> Result<(), InfraError> {
    assert!(
      !block.block_hash.is_empty(),
      "Attempt to write non-hashed block"
    );

    let file = std::fs::File::with_options()
      .write(true)
      .create_new(true)
      .open(self.block_path(&block.block_hash))?;

    let manifest = Manifest {
      api_version: 1,
      kind: "MetadataBlock".to_owned(),
      content: block,
    };

    serde_yaml::to_writer(file, &manifest)?;
    Ok(())
  }

  // TODO: atomicity
  fn write_ref(&mut self, r: &BlockRef, hash: &str) -> Result<(), InfraError> {
    let mut file = std::fs::File::create(self.ref_path(r))?;
    file.write_all(hash.as_bytes())?;
    file.sync_all()?;
    Ok(())
  }

  fn block_path(&self, hash: &str) -> PathBuf {
    let mut p = self.meta_path.join("blocks");
    p.push(hash);
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
  fn read_ref(&self, r: &BlockRef) -> Option<String> {
    let path = self.ref_path(r);
    if !path.exists() {
      None
    } else {
      Some(
        std::fs::read_to_string(&path)
          .unwrap_or_else(|e| panic!("Failed to read ref at {}: {}", path.display(), e)),
      )
    }
  }

  fn iter_blocks_ref<'c>(&'c self, r: &BlockRef) -> Box<dyn Iterator<Item = MetadataBlock> + 'c> {
    let hash = self
      .read_ref(r)
      .unwrap_or_else(|| panic!("Ref {:?} does not exist", r));

    Box::new(MetadataBlockIter {
      chain: &self,
      next_hash: Some(hash),
    })
  }

  fn append_ref(&mut self, r: &BlockRef, block: MetadataBlock) -> String {
    let last_hash = self
      .read_ref(r)
      .unwrap_or_else(|| panic!("Ref {:?} does not exist", r));

    assert_eq!(
      block.prev_block_hash, last_hash,
      "New block doesn't specify correct prev block hash"
    );

    let block_hashed = self.hashed(block);

    self.write_block(&block_hashed).unwrap();
    self.write_ref(r, &block_hashed.block_hash).unwrap();

    block_hashed.block_hash
  }
}

struct MetadataBlockIter<'c> {
  chain: &'c MetadataChainImpl,
  next_hash: Option<String>,
}

impl Iterator for MetadataBlockIter<'_> {
  type Item = MetadataBlock;

  fn next(&mut self) -> Option<Self::Item> {
    match self.next_hash {
      None => None,
      Some(ref hash) => {
        let block = self.chain.read_block(hash);
        self.next_hash = Some(block.prev_block_hash.clone()).filter(|h| !h.is_empty());
        Some(block)
      }
    }
  }
}
