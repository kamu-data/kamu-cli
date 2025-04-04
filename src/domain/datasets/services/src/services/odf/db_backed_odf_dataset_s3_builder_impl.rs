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

use crate::{DatabaseBackedOdfMetadataChainImpl, DatabaseBackedOdfMetadataChainRefRepositoryImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatabaseBackedOdfDatasetS3BuilderImpl {
    catalog: Catalog,
    metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
}

#[component(pub)]
#[interface(dyn odf::dataset::DatasetS3Builder)]
impl DatabaseBackedOdfDatasetS3BuilderImpl {
    pub fn new(catalog: Catalog, metadata_cache_local_fs_path: Option<Arc<PathBuf>>) -> Self {
        Self {
            catalog,
            metadata_cache_local_fs_path,
        }
    }
}

impl odf::dataset::DatasetS3Builder for DatabaseBackedOdfDatasetS3BuilderImpl {
    fn build_s3_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        s3_context: S3Context,
    ) -> std::sync::Arc<dyn odf::Dataset> {
        use odf::dataset::*;

        let s3_context_url = s3_context.url().clone();

        if let Some(metadata_cache_local_fs_path) = &self.metadata_cache_local_fs_path {
            Arc::new(DatasetImpl::new(
                DatabaseBackedOdfMetadataChainImpl::new(
                    dataset_id.clone(),
                    self.catalog.get_one().unwrap(),
                    MetadataChainImpl::new(
                        DatasetDefaultS3Builder::build_meta_block_repo(
                            odf::storage::lfs::ObjectRepositoryCachingLocalFs::new(
                                DatasetDefaultS3Builder::build_base_block_repo(&s3_context),
                                metadata_cache_local_fs_path.clone(),
                            ),
                        ),
                        DatabaseBackedOdfMetadataChainRefRepositoryImpl::new(
                            self.catalog.get_one().unwrap(),
                            DatasetDefaultS3Builder::build_refs_repo(&s3_context),
                            dataset_id.clone(),
                        ),
                    ),
                ),
                DatasetDefaultS3Builder::build_data_repo(&s3_context),
                DatasetDefaultS3Builder::build_checkpoint_repo(&s3_context),
                DatasetDefaultS3Builder::build_info_repo(&s3_context),
                s3_context_url,
            ))
        } else {
            Arc::new(DatasetImpl::new(
                DatabaseBackedOdfMetadataChainImpl::new(
                    dataset_id.clone(),
                    self.catalog.get_one().unwrap(),
                    MetadataChainImpl::new(
                        DatasetDefaultS3Builder::build_meta_block_repo(
                            DatasetDefaultS3Builder::build_base_block_repo(&s3_context),
                        ),
                        DatabaseBackedOdfMetadataChainRefRepositoryImpl::new(
                            self.catalog.get_one().unwrap(),
                            DatasetDefaultS3Builder::build_refs_repo(&s3_context),
                            dataset_id.clone(),
                        ),
                    ),
                ),
                DatasetDefaultS3Builder::build_data_repo(&s3_context),
                DatasetDefaultS3Builder::build_checkpoint_repo(&s3_context),
                DatasetDefaultS3Builder::build_info_repo(&s3_context),
                s3_context_url,
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
