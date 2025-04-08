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

use odf_dataset::*;
use odf_storage::*;
use odf_storage_lfs::ObjectRepositoryCachingLocalFs;
use odf_storage_s3::*;
use s3_utils::S3Context;

use crate::{DatasetImpl, MetadataChainImpl, MetadataChainReferenceRepositoryImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn DatasetS3Builder)]
pub struct DatasetS3BuilderDefault {
    /// When specified enables the local FS
    /// cache of metadata blocks, allowing to dramatically reduce the number
    /// of requests to S3
    #[dill::component(explicit)]
    metadata_cache_local_fs_path: Option<Arc<PathBuf>>,
}

impl DatasetS3BuilderDefault {
    pub fn build_meta_block_repo<TObjectRepo: ObjectRepository>(
        base_block_repo: TObjectRepo,
    ) -> MetadataBlockRepositoryCachingInMem<MetadataBlockRepositoryImpl<TObjectRepo>> {
        MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(base_block_repo))
    }

    pub fn build_base_block_repo(s3_context: &S3Context) -> ObjectRepositoryS3Sha3 {
        ObjectRepositoryS3Sha3::new(s3_context.sub_context("blocks/"))
    }

    pub fn build_meta_ref_repo<TRefRepo: ReferenceRepository + Send + Sync>(
        ref_repo: TRefRepo,
    ) -> MetadataChainReferenceRepositoryImpl<TRefRepo> {
        MetadataChainReferenceRepositoryImpl::new(ref_repo)
    }

    pub fn build_refs_repo(
        s3_context: &S3Context,
    ) -> ReferenceRepositoryImpl<NamedObjectRepositoryS3> {
        ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(
            s3_context.sub_context("refs/"),
        ))
    }

    pub fn build_data_repo(s3_context: &S3Context) -> ObjectRepositoryS3Sha3 {
        ObjectRepositoryS3Sha3::new(s3_context.sub_context("data/"))
    }

    pub fn build_checkpoint_repo(s3_context: &S3Context) -> ObjectRepositoryS3Sha3 {
        ObjectRepositoryS3Sha3::new(s3_context.sub_context("checkpoints/"))
    }

    pub fn build_info_repo(s3_context: &S3Context) -> NamedObjectRepositoryS3 {
        NamedObjectRepositoryS3::new(s3_context.sub_context("info/"))
    }
}

impl DatasetS3Builder for DatasetS3BuilderDefault {
    fn build_s3_dataset(
        &self,
        _dataset_id: &odf_metadata::DatasetID,
        s3_context: S3Context,
    ) -> Arc<dyn Dataset> {
        let s3_context_url = s3_context.url().clone();

        if let Some(metadata_cache_local_fs_path) = &self.metadata_cache_local_fs_path {
            Arc::new(DatasetImpl::new(
                MetadataChainImpl::new(
                    Self::build_meta_block_repo(ObjectRepositoryCachingLocalFs::new(
                        Self::build_base_block_repo(&s3_context),
                        metadata_cache_local_fs_path.clone(),
                    )),
                    Self::build_meta_ref_repo(Self::build_refs_repo(&s3_context)),
                ),
                Self::build_data_repo(&s3_context),
                Self::build_checkpoint_repo(&s3_context),
                Self::build_info_repo(&s3_context),
                s3_context_url,
            ))
        } else {
            Arc::new(DatasetImpl::new(
                MetadataChainImpl::new(
                    Self::build_meta_block_repo(Self::build_base_block_repo(&s3_context)),
                    Self::build_meta_ref_repo(Self::build_refs_repo(&s3_context)),
                ),
                Self::build_data_repo(&s3_context),
                Self::build_checkpoint_repo(&s3_context),
                Self::build_info_repo(&s3_context),
                s3_context_url,
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
