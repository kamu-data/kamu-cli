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

use dill::*;
use std::sync::Arc;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFactoryImpl {}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Make digest configurable
type DatasetImplLocalFS = DatasetImpl<
    MetadataChainImpl<
        ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>,
        ReferenceRepositoryImpl<NamedObjectRepositoryLocalFS>,
    >,
    ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>,
    ObjectRepositoryLocalFS<sha3::Sha3_256, 0x16>,
    NamedObjectRepositoryLocalFS,
    NamedObjectRepositoryLocalFS,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetFactoryImpl {
    pub fn new() -> Self {
        Self {}
    }

    pub fn get_local_fs(layout: DatasetLayout) -> DatasetImplLocalFS {
        DatasetImpl::new(
            MetadataChainImpl::new(
                ObjectRepositoryLocalFS::new(layout.blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(layout.data_dir),
            ObjectRepositoryLocalFS::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.cache_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
        )
    }

    pub fn get_http(base_url: Url) -> Result<impl Dataset, InternalError> {
        let client = reqwest::Client::new();
        Ok(DatasetImpl::new(
            MetadataChainImpl::new(
                ObjectRepositoryHttp::new(client.clone(), base_url.join("blocks/").unwrap()),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryHttp::new(
                    client.clone(),
                    base_url.join("refs/").unwrap(),
                )),
            ),
            ObjectRepositoryHttp::new(client.clone(), base_url.join("data/").unwrap()),
            ObjectRepositoryHttp::new(client.clone(), base_url.join("checkpoints/").unwrap()),
            NamedObjectRepositoryHttp::new(client.clone(), base_url.join("cache/").unwrap()),
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
            MetadataChainImpl::new(
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
                format!("{}cache/", key_prefix),
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
    fn get_dataset(
        &self,
        url: &Url,
        create_if_not_exists: bool,
    ) -> Result<Arc<dyn Dataset>, BuildDatasetError> {
        match url.scheme() {
            "file" => {
                let path = url
                    .to_file_path()
                    .map_err(|_| "Invalid file url".int_err())?;
                let layout = if create_if_not_exists
                    && (!path.exists() || path.read_dir().int_err()?.next().is_none())
                {
                    std::fs::create_dir_all(&path).int_err()?;
                    DatasetLayout::create(path).int_err()?
                } else {
                    DatasetLayout::new(path)
                };
                let ds = Self::get_local_fs(layout);
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            "http" | "https" => {
                let ds = Self::get_http(url.clone())?;
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            "s3" | "s3+http" | "s3+https" => {
                let ds = Self::get_s3(url.clone())?;
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            _ => Err(UnsupportedProtocolError {
                message: None,
                url: url.clone(),
            }
            .into()),
        }
    }
}
