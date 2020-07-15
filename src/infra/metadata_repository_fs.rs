use super::MetadataChainFsYaml;
use crate::domain::*;

use std::convert::TryFrom;
use std::path::{Path, PathBuf};

pub struct MetadataRepositoryFs {
    meta_path: PathBuf,
}

impl MetadataRepositoryFs {
    pub fn new(meta_path: &Path) -> MetadataRepositoryFs {
        MetadataRepositoryFs {
            meta_path: meta_path.to_owned(),
        }
    }
}

impl MetadataRepository for MetadataRepositoryFs {
    fn list_datasets(&self) -> Vec<DatasetIDBuf> {
        vec![
            DatasetIDBuf::try_from("AAAA").unwrap(),
            DatasetIDBuf::try_from("BBBB").unwrap(),
        ]
    }

    fn get_metadata_chain(&self, dataset_id: &DatasetID) -> Box<dyn MetadataChain> {
        Box::new(MetadataChainFsYaml::new(&self.meta_path))
    }
}
