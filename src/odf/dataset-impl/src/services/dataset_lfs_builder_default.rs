// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use dill::component;
use odf_dataset::*;
use odf_storage::*;
use odf_storage_lfs::*;
use url::Url;

use crate::{DatasetImpl, MetadataChainImpl, MetadataChainReferenceRepositoryImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct DatasetLfsBuilderDefault {}

impl DatasetLfsBuilderDefault {
    pub fn build_meta_block_repo(
        blocks_dir: PathBuf,
    ) -> MetadataBlockRepositoryCachingInMem<MetadataBlockRepositoryImpl<ObjectRepositoryLocalFSSha3>>
    {
        MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
            ObjectRepositoryLocalFSSha3::new(blocks_dir),
        ))
    }

    pub fn build_meta_ref_repo<TRefRepo: ReferenceRepository + Send + Sync>(
        ref_repo: TRefRepo,
    ) -> MetadataChainReferenceRepositoryImpl<TRefRepo> {
        MetadataChainReferenceRepositoryImpl::new(ref_repo)
    }

    pub fn build_refs_repo(
        refs_dir: PathBuf,
    ) -> ReferenceRepositoryImpl<NamedObjectRepositoryLocalFS> {
        ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir))
    }

    pub fn build_data_repo(data_dir: PathBuf) -> ObjectRepositoryLocalFSSha3 {
        ObjectRepositoryLocalFSSha3::new(data_dir)
    }

    pub fn build_checkpoint_repo(checkpoints_dir: PathBuf) -> ObjectRepositoryLocalFSSha3 {
        ObjectRepositoryLocalFSSha3::new(checkpoints_dir)
    }

    pub fn build_info_repo(info_dir: PathBuf) -> NamedObjectRepositoryLocalFS {
        NamedObjectRepositoryLocalFS::new(info_dir)
    }
}

impl DatasetLfsBuilder for DatasetLfsBuilderDefault {
    fn build_lfs_dataset(
        &self,
        _dataset_id: &odf_metadata::DatasetID,
        layout: DatasetLayout,
    ) -> Arc<dyn Dataset> {
        Arc::new(DatasetImpl::new(
            MetadataChainImpl::new(
                Self::build_meta_block_repo(layout.blocks_dir),
                Self::build_meta_ref_repo(Self::build_refs_repo(layout.refs_dir)),
            ),
            Self::build_data_repo(layout.data_dir),
            Self::build_checkpoint_repo(layout.checkpoints_dir),
            Self::build_info_repo(layout.info_dir),
            Url::from_directory_path(&layout.root_dir).unwrap(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
