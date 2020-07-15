use std::fs;
use std::path::{Path, PathBuf};

pub struct DatasetID(String);

pub struct MetadataRepository {
    root_dir: PathBuf,
}

impl MetadataRepository {
    pub fn new() -> MetadataRepository {
        MetadataRepository {
            root_dir: PathBuf::from(Path::new(".")),
        }
    }

    pub fn get_all_datasets(&self) {
        let iter = fs::read_dir(&self.root_dir).unwrap();
        //DatasetIter { read_dir: iter }
    }

    /*pub fn get_metadata_chain(&self) -> Box<dyn MetadataChain> {
        let chain = MetadataChainFsYaml::new(Path::new("."));
        Box::new(chain)
    }*/
}
