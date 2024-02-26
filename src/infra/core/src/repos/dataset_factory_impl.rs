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
use event_bus::EventBus;
use kamu_core::*;
use url::Url;

use crate::utils::s3_context::S3Context;
use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFactoryImpl {
    ipfs_gateway: IpfsGateway,
    access_token_resolver: Arc<dyn kamu_core::auth::OdfServerAccessTokenResolver>,
    event_bus: Arc<EventBus>,
}

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Make digest configurable
type DatasetImplLocalFS = DatasetImpl<
    MetadataChainImpl<
        MetadataBlockRepositoryCachingInMem<
            MetadataBlockRepositoryImpl<ObjectRepositoryLocalFSSha3>,
        >,
        ReferenceRepositoryImpl<NamedObjectRepositoryLocalFS>,
    >,
    ObjectRepositoryLocalFSSha3,
    ObjectRepositoryLocalFSSha3,
    NamedObjectRepositoryLocalFS,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetFactory)]
impl DatasetFactoryImpl {
    pub fn new(
        ipfs_gateway: IpfsGateway,
        access_token_resolver: Arc<dyn kamu_core::auth::OdfServerAccessTokenResolver>,
        event_bus: Arc<EventBus>,
    ) -> Self {
        Self {
            ipfs_gateway,
            access_token_resolver,
            event_bus,
        }
    }

    pub fn get_local_fs(layout: DatasetLayout, event_bus: Arc<EventBus>) -> DatasetImplLocalFS {
        DatasetImpl::new(
            event_bus,
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryLocalFSSha3::new(layout.blocks_dir),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(layout.data_dir),
            ObjectRepositoryLocalFS::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
        )
    }

    fn get_http(
        base_url: &Url,
        header_map: http::HeaderMap,
        event_bus: Arc<EventBus>,
    ) -> impl Dataset {
        let client = reqwest::Client::new();

        DatasetImpl::new(
            event_bus,
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryHttp::new(
                        client.clone(),
                        base_url.join("blocks/").unwrap(),
                        header_map.clone(),
                    ),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryHttp::new(
                    client.clone(),
                    base_url.join("refs/").unwrap(),
                    header_map.clone(),
                )),
            ),
            ObjectRepositoryHttp::new(
                client.clone(),
                base_url.join("data/").unwrap(),
                header_map.clone(),
            ),
            ObjectRepositoryHttp::new(
                client.clone(),
                base_url.join("checkpoints/").unwrap(),
                header_map.clone(),
            ),
            NamedObjectRepositoryHttp::new(
                client.clone(),
                base_url.join("info/").unwrap(),
                header_map,
            ),
        )
    }

    /// Creates new dataset proxy for an S3 URL
    ///
    /// WARNING: This function will create a new [S3Context] that will do
    /// credential resolution from scratch which can be very expensive. If you
    /// already have an established [S3Context] use [get_s3_with_context]
    /// function instead.
    pub async fn get_s3_from_url(
        base_url: Url,
        event_bus: Arc<EventBus>,
    ) -> Result<impl Dataset, InternalError> {
        // TODO: We should ensure optimal credential reuse. Perhaps in future we should
        // create a cache of S3Contexts keyed by an endpoint.
        let s3_context = S3Context::from_url(&base_url).await;
        Self::get_s3_from_context(s3_context, event_bus)
    }

    pub fn get_s3_from_context(
        s3_context: S3Context,
        event_bus: Arc<EventBus>,
    ) -> Result<impl Dataset, InternalError> {
        let client = s3_context.client;
        let endpoint = s3_context.endpoint;
        let bucket = s3_context.bucket;
        let key_prefix = s3_context.key_prefix;

        Ok(DatasetImpl::new(
            event_bus,
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryS3Sha3::new(S3Context::new(
                        client.clone(),
                        endpoint.clone(),
                        bucket.clone(),
                        format!("{key_prefix}blocks/"),
                    )),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryS3::new(S3Context::new(
                    client.clone(),
                    endpoint.clone(),
                    bucket.clone(),
                    format!("{key_prefix}refs/"),
                ))),
            ),
            ObjectRepositoryS3Sha3::new(S3Context::new(
                client.clone(),
                endpoint.clone(),
                bucket.clone(),
                format!("{key_prefix}data/"),
            )),
            ObjectRepositoryS3Sha3::new(S3Context::new(
                client.clone(),
                endpoint.clone(),
                bucket.clone(),
                format!("{key_prefix}checkpoints/"),
            )),
            NamedObjectRepositoryS3::new(S3Context::new(
                client.clone(),
                endpoint.clone(),
                bucket.clone(),
                format!("{key_prefix}info/"),
            )),
        ))
    }

    async fn get_ipfs_http(
        &self,
        base_url: Url,
        event_bus: Arc<EventBus>,
    ) -> Result<impl Dataset, InternalError> {
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
            event_bus,
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryHttp::new(
                        client.clone(),
                        dataset_url.join("blocks/").unwrap(),
                        Default::default(),
                    ),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryIpfsHttp::new(
                    client.clone(),
                    dataset_url.join("refs/").unwrap(),
                )),
            ),
            ObjectRepositoryHttp::new(
                client.clone(),
                dataset_url.join("data/").unwrap(),
                Default::default(),
            ),
            ObjectRepositoryHttp::new(
                client.clone(),
                dataset_url.join("checkpoints/").unwrap(),
                Default::default(),
            ),
            NamedObjectRepositoryIpfsHttp::new(client.clone(), dataset_url.join("info/").unwrap()),
        ))
    }

    async fn resolve_ipns_dnslink(&self, domain: &str) -> Result<String, InternalError> {
        let r = trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf().int_err()?;
        let query = format!("_dnslink.{domain}");
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

    fn build_header_map(&self, http_dataset_url: &Url) -> http::HeaderMap {
        let maybe_access_token = self
            .access_token_resolver
            .resolve_odf_dataset_access_token(http_dataset_url);

        match maybe_access_token {
            Some(access_token) => {
                let mut header_map = http::HeaderMap::new();
                header_map.append(
                    http::header::AUTHORIZATION,
                    http::HeaderValue::from_str(format!("Bearer {access_token}").as_str()).unwrap(),
                );
                header_map
            }
            None => Default::default(),
        }
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
                let ds = Self::get_local_fs(layout, self.event_bus.clone());
                Ok(Arc::new(ds) as Arc<dyn Dataset>)
            }
            "http" | "https" => {
                let ds = Self::get_http(url, self.build_header_map(url), self.event_bus.clone());
                Ok(Arc::new(ds))
            }
            "odf+http" | "odf+https" => {
                // TODO: PERF: Consider what speedups are possible in smart protocol
                let http_url = Url::parse(url.as_str().strip_prefix("odf+").unwrap()).unwrap();
                let ds = Self::get_http(
                    &http_url,
                    self.build_header_map(&http_url),
                    self.event_bus.clone(),
                );
                Ok(Arc::new(ds))
            }
            "ipfs" | "ipns" | "ipfs+http" | "ipfs+https" | "ipns+http" | "ipns+https" => {
                let ds = self
                    .get_ipfs_http(url.clone(), self.event_bus.clone())
                    .await?;
                Ok(Arc::new(ds))
            }
            "s3" | "s3+http" | "s3+https" => {
                let ds = Self::get_s3_from_url(url.clone(), self.event_bus.clone()).await?;
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
