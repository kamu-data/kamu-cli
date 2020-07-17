use crate::domain::*;
use crate::infra::serde::yaml::*;

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
