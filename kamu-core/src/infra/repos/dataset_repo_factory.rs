// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use crate::infra::*;

use std::path::{Path, PathBuf};
use thiserror::Error;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRepoFactory;

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Pass hasher generic type into factory functions
type ObjectStoreLocalFSDefault = ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>;
type DatasetImplLocalFS = DatasetImpl<
    MetadataChain2Impl<
        ObjectStoreLocalFSDefault,
        ReferenceRepositoryImpl<NamedObjectRepositoryLocalFS>,
    >,
    ObjectStoreLocalFSDefault,
    ObjectStoreLocalFSDefault,
    NamedObjectRepositoryLocalFS,
>;

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetRepoFactory {
    pub fn get_local_fs_legacy(
        meta_dir: PathBuf,
        layout: DatasetLayout,
    ) -> Result<impl Dataset, InternalError> {
        let blocks_dir = meta_dir.join("blocks");
        let refs_dir = meta_dir.join("refs");
        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir)),
            ),
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(layout.data_dir),
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(meta_dir),
        ))
    }

    pub fn get_or_create_local_fs<P: AsRef<Path>>(
        root: P,
    ) -> Result<DatasetImplLocalFS, InternalError> {
        let root = root.as_ref();
        if !root.exists() || root.read_dir().into_internal_error()?.next().is_none() {
            Self::create_local_fs(root)
        } else {
            Self::get_local_fs(root)
        }
    }

    pub fn get_local_fs<P: AsRef<Path>>(
        root: P,
    ) -> Result<DatasetImplLocalFS, InternalError> {
        let root = root.as_ref();
        let blocks_dir = root.join("blocks");
        let refs_dir = root.join("refs");
        let data_dir = root.join("data");
        let checkpoints_dir = root.join("checkpoints");
        let info_dir = root.join("info");

        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir)),
            ),
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(data_dir),
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(info_dir),
        ))
    }

    pub fn create_local_fs<P: AsRef<Path>>(
        root: P,
    ) -> Result<DatasetImplLocalFS, InternalError> {
        let root = root.as_ref();
        if !root.exists() {
            std::fs::create_dir(&root).into_internal_error()?;
        }

        let blocks_dir = root.join("blocks");
        std::fs::create_dir(&blocks_dir).into_internal_error()?;

        let refs_dir = root.join("refs");
        std::fs::create_dir(&refs_dir).into_internal_error()?;

        let data_dir = root.join("data");
        std::fs::create_dir(&data_dir).into_internal_error()?;

        let checkpoints_dir = root.join("checkpoints");
        std::fs::create_dir(&checkpoints_dir).into_internal_error()?;

        let info_dir = root.join("info");
        std::fs::create_dir(&info_dir).into_internal_error()?;

        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir)),
            ),
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(data_dir),
            ObjectRepositoryLocalFS::<sha3::Sha3_256, 0x16>::new(checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(info_dir),
        ))
    }

    pub fn get_http(base_url: Url) -> Result<impl Dataset, InternalError> {
        let client = reqwest::Client::new();
        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryHttp::new(client.clone(), base_url.join("blocks/").unwrap()),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryHttp::new(
                    client.clone(),
                    base_url.join("refs/").unwrap(),
                )),
            ),
            ObjectRepositoryHttp::new(client.clone(), base_url.join("data/").unwrap()),
            ObjectRepositoryHttp::new(client.clone(), base_url.join("checkpoints/").unwrap()),
            NamedObjectRepositoryHttp::new(client.clone(), base_url.join("info/").unwrap()),
        ))
    }

    pub fn get_s3(base_url: Url) -> Result<impl Dataset, InternalError> {
        use rusoto_core::Region;
        use rusoto_s3::S3Client;

        let (endpoint, bucket, key_prefix) = ObjectRepositoryS3::<(), 0>::split_url(&base_url);
        assert!(
            key_prefix.is_empty() || key_prefix.ends_with('/'),
            "Base URL does not contain a trailing slash: {}",
            base_url
        );

        let region = match endpoint {
            None => Region::default(),
            Some(endpoint) => Region::Custom {
                name: "custom".to_owned(),
                endpoint: endpoint,
            },
        };

        let client = S3Client::new(region);

        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::new(
                    client.clone(),
                    bucket.clone(),
                    format!("{}blocks/", key_prefix),
                ),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(
                    client.clone(),
                    bucket.clone(),
                    format!("{}refs/", key_prefix),
                )),
            ),
            ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::new(
                client.clone(),
                bucket.clone(),
                format!("{}data/", key_prefix),
            ),
            ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::new(
                client.clone(),
                bucket.clone(),
                format!("{}checkpoints/", key_prefix),
            ),
            NamedObjectRepositoryS3::new(
                client.clone(),
                bucket.clone(),
                format!("{}info/", key_prefix),
            ),
        ))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetRepoError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}
