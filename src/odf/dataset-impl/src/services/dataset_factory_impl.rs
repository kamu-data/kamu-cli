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
#[cfg(any(feature = "http", feature = "s3"))]
use internal_error::InternalError;
#[cfg(any(feature = "http", feature = "lfs", feature = "s3"))]
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_dataset::*;
#[cfg(any(feature = "http", feature = "lfs", feature = "s3"))]
use odf_storage::{
    MetadataBlockRepositoryCachingInMem,
    MetadataBlockRepositoryImpl,
    ReferenceRepositoryImpl,
};
#[cfg(feature = "http")]
use odf_storage_http::*;
#[cfg(feature = "lfs")]
use odf_storage_lfs::*;
#[cfg(feature = "s3")]
use odf_storage_s3::*;
#[cfg(feature = "s3")]
use s3_utils::{S3Context, S3Metrics};
use url::Url;

#[cfg(feature = "lfs")]
use crate::DatasetLayout;
#[cfg(any(feature = "http", feature = "lfs", feature = "s3"))]
use crate::{DatasetImpl, MetadataChainImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFactoryImpl {
    #[cfg(feature = "http")]
    ipfs_gateway: IpfsGateway,
    #[cfg(feature = "http")]
    access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
    #[cfg(feature = "s3")]
    maybe_s3_metrics: Option<Arc<S3Metrics>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Make digest configurable
#[cfg(feature = "lfs")]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "s3")]
#[component(pub)]
#[interface(dyn DatasetFactory)]
impl DatasetFactoryImpl {
    #[allow(clippy::needless_pass_by_value)]
    #[allow(unused_variables)]
    pub fn new(
        ipfs_gateway: IpfsGateway,
        access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
        maybe_s3_metrics: Option<Arc<s3_utils::S3Metrics>>,
    ) -> Self {
        Self {
            #[cfg(feature = "http")]
            ipfs_gateway,
            #[cfg(feature = "http")]
            access_token_resolver,
            maybe_s3_metrics,
        }
    }
}

#[cfg(not(feature = "s3"))]
#[component(pub)]
#[interface(dyn DatasetFactory)]
impl DatasetFactoryImpl {
    #[allow(clippy::needless_pass_by_value)]
    #[allow(unused_variables)]
    pub fn new(
        ipfs_gateway: IpfsGateway,
        access_token_resolver: Arc<dyn OdfServerAccessTokenResolver>,
    ) -> Self {
        Self {
            #[cfg(feature = "http")]
            ipfs_gateway,
            #[cfg(feature = "http")]
            access_token_resolver,
        }
    }
}

impl DatasetFactoryImpl {
    #[cfg(feature = "lfs")]
    pub fn get_local_fs(layout: DatasetLayout) -> DatasetImplLocalFS {
        DatasetImpl::new(
            MetadataChainImpl::new(
                MetadataBlockRepositoryCachingInMem::new(MetadataBlockRepositoryImpl::new(
                    ObjectRepositoryLocalFSSha3::new(layout.blocks_dir),
                )),
                ReferenceRepositoryImpl::new(NamedObjectRepositoryLocalFS::new(layout.refs_dir)),
            ),
            ObjectRepositoryLocalFS::new(layout.data_dir),
            ObjectRepositoryLocalFS::new(layout.checkpoints_dir),
            NamedObjectRepositoryLocalFS::new(layout.info_dir),
            Url::from_directory_path(&layout.root_dir).unwrap(),
        )
    }

    #[cfg(feature = "http")]
    fn get_http(base_url: &Url, header_map: http::HeaderMap) -> impl Dataset {
        // When joining url without trailing '/', last path part is being dropped
        let mut base_url = base_url.clone();
        if !base_url.path().ends_with('/') {
            base_url.set_path(&format!("{}/", base_url.path()));
        }

        let client = reqwest::Client::new();

        DatasetImpl::new(
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
            base_url.clone(),
        )
    }

    /// Creates new dataset proxy for an S3 URL
    ///
    /// WARNING: This function will create a new [`S3Context`] that will do
    /// credential resolution from scratch which can be very expensive.
    #[cfg(feature = "s3")]
    pub async fn get_s3_from_url(
        base_url: Url,
        maybe_s3_metrics: Option<Arc<S3Metrics>>,
    ) -> impl Dataset {
        // TODO: PERF: We should ensure optimal credential reuse.
        //             Perhaps in future we should create a cache of S3Contexts keyed
        //             by an endpoint.
        let mut s3_context = S3Context::from_url(&base_url).await;
        if let Some(metrics) = maybe_s3_metrics {
            s3_context = s3_context.with_metrics(metrics);
        }

        DatasetImpl::new(
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
            base_url,
        )
    }

    #[cfg(feature = "http")]
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
            dataset_url.clone(),
        ))
    }

    #[cfg(feature = "http")]
    async fn resolve_ipns_dnslink(&self, domain: &str) -> Result<String, InternalError> {
        let r = hickory_resolver::TokioAsyncResolver::tokio_from_system_conf().int_err()?;
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

    #[cfg(feature = "http")]
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Refactor to multiple factory providers that get dispatched based on URL
// schema
#[async_trait::async_trait]
impl DatasetFactory for DatasetFactoryImpl {
    #[allow(unused_variables)]
    async fn get_dataset(
        &self,
        url: &Url,
        create_if_not_exists: bool,
    ) -> Result<Arc<dyn Dataset>, BuildDatasetError> {
        match url.scheme() {
            #[cfg(feature = "lfs")]
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
            #[cfg(feature = "http")]
            "http" | "https" => {
                let ds = Self::get_http(url, self.build_header_map(url));
                Ok(Arc::new(ds))
            }
            #[cfg(feature = "http")]
            "odf+http" | "odf+https" => {
                // TODO: PERF: Consider what speedups are possible in smart protocol
                let http_url = Url::parse(url.as_str().strip_prefix("odf+").unwrap()).unwrap();
                let ds = Self::get_http(&http_url, self.build_header_map(&http_url));
                Ok(Arc::new(ds))
            }
            #[cfg(feature = "http")]
            "ipfs" | "ipns" | "ipfs+http" | "ipfs+https" | "ipns+http" | "ipns+https" => {
                let ds = self.get_ipfs_http(url.clone()).await?;
                Ok(Arc::new(ds))
            }
            #[cfg(feature = "s3")]
            "s3" | "s3+http" | "s3+https" => {
                let ds = Self::get_s3_from_url(url.clone(), self.maybe_s3_metrics.clone()).await;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "http")]
#[derive(thiserror::Error, Debug)]
#[error("Failed to resolve DNSLink record: {record}")]
struct DnsLinkResolutionError {
    pub record: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
