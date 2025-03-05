// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use odf_dataset::{Dataset, DatasetLayout, DatasetLfsBuilder};
use odf_storage::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryImpl,
    ReferenceRepositoryImpl,
};
use odf_storage_lfs::{NamedObjectRepositoryLocalFS, ObjectRepositoryLocalFSSha3};
use url::Url;

use crate::{DatasetImpl, MetadataChainImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
pub struct DatasetDefaultLfsBuilderImpl {}

impl DatasetLfsBuilder for DatasetDefaultLfsBuilderImpl {
    fn build_lfs_dataset(&self, layout: DatasetLayout) -> Arc<dyn Dataset> {
        Arc::new(DatasetImpl::new(
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryLocalFSSha3::new(layout.blocks_dir),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFSSha3::new(layout.data_dir),
            ObjectRepositoryLocalFSSha3::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
            Url::from_directory_path(&layout.root_dir).unwrap(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
