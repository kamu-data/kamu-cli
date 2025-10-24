// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, component, interface};
use odf::Dataset;
use url::Url;

use crate::{
    MetadataChainDatabaseBackedImpl,
    MetadataChainDbBackedConfig,
    MetadataChainRefRepositoryDatabaseBackedImpl,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn odf::dataset::DatasetLfsBuilder)]

pub struct DatasetLfsBuilderDatabaseBackedImpl {
    catalog: Catalog,
    config: Arc<MetadataChainDbBackedConfig>,
}

impl odf::dataset::DatasetLfsBuilder for DatasetLfsBuilderDatabaseBackedImpl {
    fn build_lfs_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        layout: odf::dataset::DatasetLayout,
    ) -> Arc<dyn Dataset> {
        use odf::dataset::*;

        Arc::new(DatasetImpl::new(
            MetadataChainDatabaseBackedImpl::new(
                *(self.config.as_ref()),
                dataset_id.clone(),
                self.catalog.get_one().unwrap(),
                self.catalog.get_one().unwrap(),
                MetadataChainImpl::new(
                    DatasetLfsBuilderDefault::build_meta_block_repo(layout.blocks_dir),
                    MetadataChainRefRepositoryDatabaseBackedImpl::new(
                        self.catalog.get_one().unwrap(),
                        DatasetLfsBuilderDefault::build_refs_repo(layout.refs_dir),
                        dataset_id.clone(),
                    ),
                ),
            ),
            DatasetLfsBuilderDefault::build_data_repo(layout.data_dir),
            DatasetLfsBuilderDefault::build_checkpoint_repo(layout.checkpoints_dir),
            DatasetLfsBuilderDefault::build_info_repo(layout.info_dir),
            Url::from_directory_path(&layout.root_dir).unwrap(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
