use crate::domain::metadata::MetadataBlock;
use crate::domain::BlockIterator;
use crate::domain::MetadataChain;
use std::path::{Path, PathBuf};

pub struct MetadataChainFsYaml {
    _path: PathBuf,
}

impl MetadataChainFsYaml {
    pub fn new(path: &Path) -> Self {
        Self {
            _path: PathBuf::from(path),
        }
    }

    pub fn path(&self) -> &Path {
        &self._path
    }
}

impl MetadataChain for MetadataChainFsYaml {
    fn iter_blocks(&self) -> Box<BlockIterator> {
        let v = vec![
            MetadataBlock {
                block_hash: "a".to_string(),
                prev_block_hash: "aa".to_string(),
            },
            MetadataBlock {
                block_hash: "b".to_string(),
                prev_block_hash: "bb".to_string(),
            },
        ];
        Box::new(v.into_iter())
    }
}
