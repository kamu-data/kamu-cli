// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::services::dataset_factory::GetDatasetError;
use crate::domain::*;
use crate::infra::*;

use dill::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFactoryImpl {
    ipfs_gateway: IpfsGateway,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct IpfsGateway {
    pub url: Url,
}

impl Default for IpfsGateway {
    fn default() -> Self {
        Self {
            url: Url::parse("http://localhost:8080").unwrap(),
        }
    }
}

// TODO: Make digest configurable
type DatasetImplLocalFS = DatasetImpl<
    MetadataChain2Impl<
        ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>,
        ReferenceRepositoryImpl<NamedObjectRepositoryLocalFS>,
    >,
    ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>,
    ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>,
    NamedObjectRepositoryLocalFS,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetFactoryImpl {
    pub fn new(ipfs_gateway: IpfsGateway) -> Self {
        Self { ipfs_gateway }
    }

    // TODO: Eliminate
    pub fn get_local_fs_legacy(
        meta_dir: PathBuf,
        layout: DatasetLayout,
    ) -> Result<DatasetImplLocalFS, InternalError> {
        let blocks_dir = meta_dir.join("blocks");
        let refs_dir = meta_dir.join("refs");
        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryLocalFS::new(blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(layout.data_dir),
            ObjectRepositoryLocalFS::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(meta_dir),
        ))
    }

    pub fn get_or_create_local_fs<P: AsRef<Path>>(
        root: P,
    ) -> Result<DatasetImplLocalFS, InternalError> {
        let root = root.as_ref();
        if !root.exists() || root.read_dir().int_err()?.next().is_none() {
            Self::create_local_fs(root)
        } else {
            Self::get_local_fs(root)
        }
    }

    pub fn get_local_fs<P: AsRef<Path>>(root: P) -> Result<DatasetImplLocalFS, InternalError> {
        let root = root.as_ref();
        let blocks_dir = root.join("blocks");
        let refs_dir = root.join("refs");
        let data_dir = root.join("data");
        let checkpoints_dir = root.join("checkpoints");
        let info_dir = root.join("info");

        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryLocalFS::new(blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(data_dir),
            ObjectRepositoryLocalFS::new(checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(info_dir),
        ))
    }

    pub fn create_local_fs<P: AsRef<Path>>(root: P) -> Result<DatasetImplLocalFS, InternalError> {
        let root = root.as_ref();
        if !root.exists() {
            std::fs::create_dir(&root).int_err()?;
        }

        let blocks_dir = root.join("blocks");
        std::fs::create_dir(&blocks_dir).int_err()?;

        let refs_dir = root.join("refs");
        std::fs::create_dir(&refs_dir).int_err()?;

        let data_dir = root.join("data");
        std::fs::create_dir(&data_dir).int_err()?;

        let checkpoints_dir = root.join("checkpoints");
        std::fs::create_dir(&checkpoints_dir).int_err()?;

        let info_dir = root.join("info");
        std::fs::create_dir(&info_dir).int_err()?;

        Ok(DatasetImpl::new(
            MetadataChain2Impl::new(
                ObjectRepositoryLocalFS::new(blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(data_dir),
            ObjectRepositoryLocalFS::new(checkpoints_dir),
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

impl DatasetFactory for DatasetFactoryImpl {
    fn get_dataset(&self, url: Url) -> Result<Arc<dyn Dataset>, GetDatasetError> {
        match url.scheme() {
            "file" => {
                let ds = Self::get_or_create_local_fs(url.to_file_path().unwrap())?;
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            "http" | "https" => {
                let ds = Self::get_http(url)?;
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            "s3" | "s3+http" | "s3+https" => {
                let ds = Self::get_s3(url)?;
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            "ipfs" => {
                let cid = match url.host() {
                    Some(url::Host::Domain(cid)) => Ok(cid),
                    _ => Err("Malformed IPFS URL").int_err(),
                }?;

                let gw_url = self
                    .ipfs_gateway
                    .url
                    .join(&format!("ipfs/{}{}", cid, url.path()))
                    .unwrap();

                info!(ipfs_url = %url, gateway_url = %gw_url, "Mapping IPFS URL to the configured HTTP gateway");

                let ds = Self::get_http(gw_url)?;
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            _ => Err(UnsupportedProtocolError { url }.into()),
        }
    }
}
