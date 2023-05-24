// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use url::Url;

use crate::domain::*;
use crate::infra::utils::s3_context::S3Context;
use crate::infra::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFactoryImpl {
    ipfs_gateway: IpfsGateway,
}

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
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl DatasetFactoryImpl {
    pub fn new(ipfs_gateway: IpfsGateway) -> Self {
        Self { ipfs_gateway }
    }

    pub fn get_local_fs(layout: DatasetLayout) -> DatasetImplLocalFS {
        DatasetImpl::new(
            MetadataChainImpl::new(
                ObjectRepositoryLocalFS::new(layout.blocks_dir),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(layout.data_dir),
            ObjectRepositoryLocalFS::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
        )
    }

    fn get_http(base_url: Url) -> Result<impl Dataset, InternalError> {
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
            NamedObjectRepositoryHttp::new(client.clone(), base_url.join("info/").unwrap()),
        ))
    }

    pub async fn get_s3(base_url: Url) -> Result<impl Dataset, InternalError> {
        let s3_context = S3Context::from_url(&base_url).await;
        let client = s3_context.client;
        let endpoint = s3_context.endpoint;
        let bucket = s3_context.bucket;
        let key_prefix = s3_context.root_folder_key;

        Ok(DatasetImpl::new(
            MetadataChainImpl::new(
                ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::new(S3Context::new(
                    client.clone(),
                    endpoint.clone(),
                    bucket.clone(),
                    format!("{}blocks/", key_prefix),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(S3Context::new(
                    client.clone(),
                    endpoint.clone(),
                    bucket.clone(),
                    format!("{}refs/", key_prefix),
                ))),
            ),
            ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::new(S3Context::new(
                client.clone(),
                endpoint.clone(),
                bucket.clone(),
                format!("{}data/", key_prefix),
            )),
            ObjectRepositoryS3::<sha3::Sha3_256, 0x16>::new(S3Context::new(
                client.clone(),
                endpoint.clone(),
                bucket.clone(),
                format!("{}checkpoints/", key_prefix),
            )),
            NamedObjectRepositoryS3::new(S3Context::new(
                client.clone(),
                endpoint.clone(),
                bucket.clone(),
                format!("{}info/", key_prefix),
            )),
        ))
    }

    async fn get_ipfs_http(&self, base_url: Url) -> Result<impl Dataset, InternalError> {
        // Resolve IPNS DNSLink names if configured
        let dataset_url = match base_url.scheme() {
            "ipns" if self.ipfs_gateway.pre_resolve_dnslink => {
                let key = match base_url.host() {
                    Some(url::Host::Domain(k)) => Ok(k),
                    _ => Err("Malformed IPNS URL").int_err(),
                }?;

                if !key.contains('.') {
                    base_url
                } else {
                    tracing::info!(ipns_url = %base_url, "Resolving DNSLink name");
                    let cid = self.resolve_ipns_dnslink(key).await?;

                    let mut ipfs_url =
                        Url::parse(&format!("ipfs://{}{}", cid, base_url.path())).unwrap();

                    if !ipfs_url.path().ends_with('/') {
                        ipfs_url.set_path(&format!("{}/", ipfs_url.path()));
                    }

                    tracing::info!(ipns_url = %base_url, %ipfs_url, "Resolved DNSLink name");
                    ipfs_url
                }
            }
            _ => base_url,
        };

        // Re-map IPFS/IPNS urls to HTTP gateway URLs
        // Note: This is for read path only, write path is handled separately
        let dataset_url = {
            let cid = match dataset_url.host() {
                Some(url::Host::Domain(cid)) => Ok(cid),
                _ => Err("Malformed IPFS URL").int_err(),
            }?;

            let gw_url = self
                .ipfs_gateway
                .url
                .join(&format!(
                    "{}/{}{}",
                    dataset_url.scheme(),
                    cid,
                    dataset_url.path()
                ))
                .unwrap();

            tracing::info!(url = %dataset_url, gateway_url = %gw_url, "Mapping IPFS URL to the configured HTTP gateway");
            gw_url
        };

        let client = reqwest::Client::new();
        Ok(DatasetImpl::new(
            MetadataChainImpl::new(
                ObjectRepositoryHttp::new(client.clone(), dataset_url.join("blocks/").unwrap()),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryIpfsHttp::new(
                    client.clone(),
                    dataset_url.join("refs/").unwrap(),
                )),
            ),
            ObjectRepositoryHttp::new(client.clone(), dataset_url.join("data/").unwrap()),
            ObjectRepositoryHttp::new(client.clone(), dataset_url.join("checkpoints/").unwrap()),
            NamedObjectRepositoryIpfsHttp::new(client.clone(), dataset_url.join("info/").unwrap()),
        ))
    }

    async fn resolve_ipns_dnslink(&self, domain: &str) -> Result<String, InternalError> {
        let r = trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf().int_err()?;
        let query = format!("_dnslink.{}", domain);
        let result = r.txt_lookup(&query).await.int_err()?;

        let dnslink_re = regex::Regex::new(r"_?dnslink=/ipfs/(.*)").unwrap();

        for record in result {
            let data = record.to_string();
            tracing::debug!(%data, "Observed TXT record");

            if let Some(c) = dnslink_re.captures(&data) {
                return Ok(c.get(1).unwrap().as_str().to_owned());
            }
        }
        Err(DnsLinkResolutionError { record: query }.int_err())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Refactor to multiple factory providers that get dispatched based on URL
// schema
#[async_trait::async_trait]
impl DatasetFactory for DatasetFactoryImpl {
    async fn get_dataset(
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
            "http" | "https" | "odf+http" | "odf+https" => {
                let ds = Self::get_http(url.clone())?;
                Ok(Arc::new(ds))
            }
            "ipfs" | "ipns" | "ipfs+http" | "ipfs+https" | "ipns+http" | "ipns+https" => {
                let ds = self.get_ipfs_http(url.clone()).await?;
                Ok(Arc::new(ds))
            }
            "s3" | "s3+http" | "s3+https" => {
                let ds = Self::get_s3(url.clone()).await?;
                Ok(Arc::new(ds))
            }
            _ => Err(UnsupportedProtocolError {
                message: None,
                url: url.clone(),
            }
            .into()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct IpfsGateway {
    pub url: Url,
    pub pre_resolve_dnslink: bool,
}

impl Default for IpfsGateway {
    fn default() -> Self {
        Self {
            url: Url::parse("http://localhost:8080").unwrap(),
            pre_resolve_dnslink: true,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("Failed to resolve DNSLink record: {record}")]
struct DnsLinkResolutionError {
    pub record: String,
}
