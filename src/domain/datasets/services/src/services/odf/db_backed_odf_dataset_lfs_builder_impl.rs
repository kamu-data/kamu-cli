// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface, Catalog};
use odf::Dataset;
use url::Url;

use crate::{
    DatabaseBackedOdfMetadataBlockQuickSearch,
    DatabaseBackedOdfMetadataChainRefRepositoryImpl,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedOdfDatasetLfsBuilderImpl {
    catalog: Catalog,
}

#[component(pub)]
#[interface(dyn odf::dataset::DatasetLfsBuilder)]
impl DatabaseBackedOdfDatasetLfsBuilderImpl {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }
}

impl odf::dataset::DatasetLfsBuilder for DatabaseBackedOdfDatasetLfsBuilderImpl {
    fn build_lfs_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        layout: odf::dataset::DatasetLayout,
    ) -> Arc<dyn Dataset> {
        use odf::dataset::*;

        Arc::new(DatasetImpl::new(
            MetadataChainImpl::new(
                DatasetDefaultLfsBuilder::build_meta_block_repo(layout.blocks_dir),
                DatabaseBackedOdfMetadataChainRefRepositoryImpl::new(
                    self.catalog.get_one().unwrap(),
                    DatasetDefaultLfsBuilder::build_refs_repo(layout.refs_dir),
                    dataset_id.clone(),
                ),
                DatabaseBackedOdfMetadataBlockQuickSearch::new(
                    dataset_id.clone(),
                    self.catalog.get_one().unwrap(),
                ),
            ),
            DatasetDefaultLfsBuilder::build_data_repo(layout.data_dir),
            DatasetDefaultLfsBuilder::build_checkpoint_repo(layout.checkpoints_dir),
            DatasetDefaultLfsBuilder::build_info_repo(layout.info_dir),
            Url::from_directory_path(&layout.root_dir).unwrap(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
