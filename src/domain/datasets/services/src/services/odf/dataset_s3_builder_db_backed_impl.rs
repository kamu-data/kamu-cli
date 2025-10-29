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

use dill::*;
use s3_utils::S3Context;

use crate::{
    MetadataChainDatabaseBackedImpl,
    MetadataChainDbBackedConfig,
    MetadataChainRefRepositoryDatabaseBackedImpl,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetS3BuilderDatabaseBackedImpl {
    catalog: Catalog,
    config: Arc<MetadataChainDbBackedConfig>,
    metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
}

#[component(pub)]
#[interface(dyn odf::dataset::DatasetS3Builder)]
impl DatasetS3BuilderDatabaseBackedImpl {
    pub fn new(
        catalog: Catalog,
        config: Arc<MetadataChainDbBackedConfig>,
        metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
    ) -> Self {
        Self {
            catalog,
            config,
            metadata_cache_local_fs_path,
        }
    }
}

impl odf::dataset::DatasetS3Builder for DatasetS3BuilderDatabaseBackedImpl {
    fn build_s3_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        s3_context: S3Context,
    ) -> std::sync::Arc<dyn odf::Dataset> {
        use odf::dataset::*;

        let s3_context_url = s3_context.url().clone();

        if let Some(metadata_cache_local_fs_path) = &self.metadata_cache_local_fs_path {
            Arc::new(DatasetImpl::new(
                MetadataChainDatabaseBackedImpl::new(
                    *(self.config.as_ref()),
                    dataset_id.clone(),
                    self.catalog.get_one().unwrap(),
                    self.catalog.get_one().unwrap(),
                    MetadataChainImpl::new(
                        DatasetS3BuilderDefault::build_meta_block_repo(
                            odf::storage::lfs::ObjectRepositoryCachingLocalFs::new(
                                DatasetS3BuilderDefault::build_base_block_repo(&s3_context),
                                metadata_cache_local_fs_path.clone(),
                            ),
                        ),
                        MetadataChainRefRepositoryDatabaseBackedImpl::new(
                            self.catalog.get_one().unwrap(),
                            DatasetS3BuilderDefault::build_refs_repo(&s3_context),
                            dataset_id.clone(),
                        ),
                    ),
                ),
                DatasetS3BuilderDefault::build_data_repo(&s3_context),
                DatasetS3BuilderDefault::build_checkpoint_repo(&s3_context),
                DatasetS3BuilderDefault::build_info_repo(&s3_context),
                s3_context_url,
            ))
        } else {
            Arc::new(DatasetImpl::new(
                MetadataChainDatabaseBackedImpl::new(
                    *(self.config.as_ref()),
                    dataset_id.clone(),
                    self.catalog.get_one().unwrap(),
                    self.catalog.get_one().unwrap(),
                    MetadataChainImpl::new(
                        DatasetS3BuilderDefault::build_meta_block_repo(
                            DatasetS3BuilderDefault::build_base_block_repo(&s3_context),
                        ),
                        MetadataChainRefRepositoryDatabaseBackedImpl::new(
                            self.catalog.get_one().unwrap(),
                            DatasetS3BuilderDefault::build_refs_repo(&s3_context),
                            dataset_id.clone(),
                        ),
                    ),
                ),
                DatasetS3BuilderDefault::build_data_repo(&s3_context),
                DatasetS3BuilderDefault::build_checkpoint_repo(&s3_context),
                DatasetS3BuilderDefault::build_info_repo(&s3_context),
                s3_context_url,
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
