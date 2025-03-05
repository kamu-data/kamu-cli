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
use odf_dataset::{Dataset, DatasetS3Builder};
use odf_storage::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryImpl,
    ReferenceRepositoryImpl,
};
use odf_storage_lfs::ObjectRepositoryCachingLocalFs;
use odf_storage_s3::{NamedObjectRepositoryS3, ObjectRepositoryS3Sha3};
use s3_utils::S3Context;

use crate::{DatasetImpl, MetadataChainImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetDefaultS3BuilderImpl {
    metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
}

#[component(pub)]
impl DatasetDefaultS3BuilderImpl {
    /// # Arguments
    ///
    /// * `metadata_cache_local_fs_path` - when specified enables the local FS
    ///   cache of metadata blocks, allowing to dramatically reduce the number
    ///   of requests to S3
    pub fn new(metadata_cache_local_fs_path: Option<Arc<PathBuf>>) -> Self {
        Self {
            metadata_cache_local_fs_path,
        }
    }
}

impl DatasetS3Builder for DatasetDefaultS3BuilderImpl {
    fn build_s3_dataset(&self, s3_context: S3Context) -> Arc<dyn Dataset> {
        let s3_context_url = s3_context.url().clone();

        if let Some(metadata_cache_local_fs_path) = &self.metadata_cache_local_fs_path {
            Arc::new(DatasetImpl::new(
                MetadataChainImpl::new(
                    MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                        ObjectRepositoryCachingLocalFs::new(
                            ObjectRepositoryS3Sha3::new(s3_context.sub_context("blocks/")),
                            metadata_cache_local_fs_path.clone(),
                        ),
                    )),
                    ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(
                        s3_context.sub_context("refs/"),
                    )),
                ),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("data/")),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("checkpoints/")),
                NamedObjectRepositoryS3::new(s3_context.into_sub_context("info/")),
                s3_context_url,
            ))
        } else {
            Arc::new(DatasetImpl::new(
                MetadataChainImpl::new(
                    MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                        ObjectRepositoryS3Sha3::new(s3_context.sub_context("blocks/")),
                    )),
                    ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(
                        s3_context.sub_context("refs/"),
                    )),
                ),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("data/")),
                ObjectRepositoryS3Sha3::new(s3_context.sub_context("checkpoints/")),
                NamedObjectRepositoryS3::new(s3_context.into_sub_context("info/")),
                s3_context_url,
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
