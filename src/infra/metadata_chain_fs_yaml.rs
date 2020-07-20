use crate::domain::*;
use crate::infra::serde::yaml::*;
use crate::infra::*;

use crypto::digest::Digest;
use crypto::sha3::Sha3;
use std::io::prelude::*;
use std::path::PathBuf;

pub struct MetadataChainFsYaml {
  meta_path: PathBuf,
}

impl MetadataChainFsYaml {
  pub fn new(meta_path: PathBuf) -> MetadataChainFsYaml {
    MetadataChainFsYaml {
      meta_path: meta_path,
    }
  }

  pub fn init(
    meta_path: PathBuf,
    first_block: MetadataBlock,
  ) -> Result<MetadataChainFsYaml, InfraError> {
    std::fs::create_dir(&meta_path)?;
    std::fs::create_dir(meta_path.join("blocks"))?;
    std::fs::create_dir(meta_path.join("refs"))?;

    assert!(first_block.block_hash.is_empty());

    let mut chain = Self::new(meta_path);
    let first_block_hashed = chain.hash_block(first_block);

    let hash = first_block_hashed.block_hash.clone();
    chain.write_block(first_block_hashed)?;
    chain.write_ref(&BlockRef::Head, &hash)?;

    Ok(chain)
  }

  fn hash_block(&self, block: MetadataBlock) -> MetadataBlock {
    let mut b = block;
    let mut digest = Sha3::sha3_256();
    // TODO: use generated hashers
    digest.input_str(&b.prev_block_hash);
    b.block_hash = digest.result_str();
    b
  }

  fn read_block(&self, hash: &str) -> MetadataBlock {
    let path = self.block_path(hash);
    let file = std::fs::File::open(path.clone()).expect(&format!(
      "Failed to open the block file at: {}",
      path.display()
    ));
    let manifest: Manifest<MetadataBlock> = serde_yaml::from_reader(&file).expect(&format!(
      "Failed to deserialize the MetadataBlock at: {}",
      path.display()
    ));
    assert_eq!(manifest.kind, "MetadataBlock");
    manifest.content
  }

  fn write_block(&mut self, block: MetadataBlock) -> Result<(), InfraError> {
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

impl MetadataChain for MetadataChainFsYaml {
  fn read_ref(&self, r: &BlockRef) -> String {
    let path = self.ref_path(r);
    std::fs::read_to_string(&path).expect(&format!("Failed to read ref at: {}", path.display()))
  }

  fn list_blocks_ref<'c>(&'c self, r: &BlockRef) -> Box<dyn Iterator<Item = MetadataBlock> + 'c> {
    let hash = self.read_ref(r);
    Box::new(MetadataBlockIter {
      chain: &self,
      next_hash: Some(hash),
    })
  }
}

struct MetadataBlockIter<'c> {
  chain: &'c MetadataChainFsYaml,
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
