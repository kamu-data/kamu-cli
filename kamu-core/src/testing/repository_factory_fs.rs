// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    convert::TryInto,
    path::{Path, PathBuf},
};

use opendatafabric::*;

use super::IDFactory;
use crate::{
    domain::{BlockRef, MetadataChain},
    infra::MetadataChainImpl,
};

// TODO: These classes should be scrapped
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
            name: IDFactory::dataset_name(),
            repo_path: self.repo_path.clone(),
        }
    }
}

pub struct DatasetBuilderFSInitial {
    name: DatasetName,
    repo_path: PathBuf,
}

impl DatasetBuilderFSInitial {
    pub fn name<S: TryInto<DatasetName>>(self, name: S) -> Self
    where
        <S as TryInto<DatasetName>>::Error: std::fmt::Debug,
    {
        Self {
            name: name.try_into().unwrap(),
            ..self
        }
    }

    pub fn append(self, first_block: MetadataBlock) -> DatasetBuilderFSContinued {
        let dataset_path = self.repo_path.join(&self.name);
        std::fs::create_dir(&dataset_path).unwrap();
        let meta_path = dataset_path.join("meta");
        let (meta_chain, _) = MetadataChainImpl::create(&meta_path, first_block).unwrap();
        DatasetBuilderFSContinued {
            meta_chain,
            dataset_path,
        }
    }
}

#[allow(dead_code)]
pub struct DatasetBuilderFSContinued {
    meta_chain: MetadataChainImpl,
    dataset_path: PathBuf,
}

impl DatasetBuilderFSContinued {
    pub fn append(mut self, mut block: MetadataBlock) -> Self {
        let old_head = self.meta_chain.read_ref(&BlockRef::Head);
        block.prev_block_hash = old_head;

        self.meta_chain.append(block);
        self
    }
}
