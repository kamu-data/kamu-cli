use std::{
    convert::TryInto,
    path::{Path, PathBuf},
};

use opendatafabric::{DatasetIDBuf, MetadataBlock};

use super::IDFactory;
use crate::{domain::MetadataChain, infra::MetadataChainImpl};

/// Allows creating ODF-compliant datasets in a repository backed by local file system.
pub struct RepositoryFactoryFS {
    repo_path: PathBuf,
}

impl RepositoryFactoryFS {
    pub fn new(repo_path: &Path) -> Self {
        Self {
            repo_path: repo_path.to_owned(),
        }
    }

    pub fn new_dataset(&self) -> DatasetBuilderFSInitial {
        DatasetBuilderFSInitial {
            dataset_id: IDFactory::dataset_id(),
            repo_path: self.repo_path.clone(),
        }
    }
}

pub struct DatasetBuilderFSInitial {
    dataset_id: DatasetIDBuf,
    repo_path: PathBuf,
}

impl DatasetBuilderFSInitial {
    pub fn id(self, dataset_id: &str) -> Self {
        Self {
            dataset_id: dataset_id.try_into().unwrap(),
            ..self
        }
    }

    pub fn append(self, first_block: MetadataBlock) -> DatasetBuilderFSContinued {
        let dataset_path = self.repo_path.join(&self.dataset_id);
        std::fs::create_dir(&dataset_path).unwrap();
        let meta_path = dataset_path.join("meta");
        let (meta_chain, _) = MetadataChainImpl::create(&meta_path, first_block).unwrap();
        DatasetBuilderFSContinued {
            meta_chain,
            dataset_path,
        }
    }
}

pub struct DatasetBuilderFSContinued {
    meta_chain: MetadataChainImpl,
    dataset_path: PathBuf,
}

impl DatasetBuilderFSContinued {
    pub fn append(mut self, block: MetadataBlock, data: Option<&[u8]>) -> Self {
        let new_head = self.meta_chain.append(block);
        if let Some(data) = data {
            let data_path = self.dataset_path.join("data");
            std::fs::create_dir_all(&data_path).unwrap();
            let part_path = data_path.join(new_head.to_string());
            std::fs::write(part_path, data).unwrap();
        }
        self
    }
}
