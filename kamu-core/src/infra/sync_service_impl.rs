// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::sync_service::DatasetNotFoundError;
use crate::domain::*;
use crate::infra::utils::ipfs_wrapper::*;
use crate::infra::utils::simple_transfer_protocol::SimpleTransferProtocol;
use opendatafabric::*;

use dill::*;
use std::sync::Arc;
use thiserror::Error;
use tracing::*;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SyncServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    local_repo: Arc<dyn LocalDatasetRepository>,
    dataset_factory: Arc<dyn DatasetFactory>,
    ipfs_client: Arc<IpfsClient>,
    ipfs_gateway: IpfsGateway,
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

#[component(pub)]
impl SyncServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        local_repo: Arc<dyn LocalDatasetRepository>,
        dataset_factory: Arc<dyn DatasetFactory>,
        ipfs_client: Arc<IpfsClient>,
        ipfs_gateway: IpfsGateway,
    ) -> Self {
        Self {
            remote_repo_reg,
            local_repo,
            dataset_factory,
            ipfs_client,
            ipfs_gateway,
        }
    }

    async fn resolve_remote_dataset_url(
        &self,
        remote_ref: &DatasetRefRemote,
    ) -> Result<Url, SyncError> {
        // TODO: REMOTE ID
        let dataset_url = match remote_ref {
            DatasetRefRemote::ID(_) => {
                unimplemented!("Syncing remote dataset by ID is not yet supported")
            }
            DatasetRefRemote::RemoteName(name)
            | DatasetRefRemote::RemoteHandle(RemoteDatasetHandle { name, .. }) => {
                let mut repo = self.remote_repo_reg.get_repository(name.repository())?;

                repo.url.ensure_trailing_slash();
                repo.url
                    .join(&format!("{}/", name.as_name_with_owner()))
                    .unwrap()
            }
            DatasetRefRemote::Url(url) => {
                let mut dataset_url = url.as_ref().clone();
                dataset_url.ensure_trailing_slash();
                dataset_url
            }
        };

        // Resolve IPNS DNSLink names if configured
        let dataset_url = match dataset_url.scheme() {
            "ipns" if self.ipfs_gateway.pre_resolve_dnslink => {
                let key = match dataset_url.host() {
                    Some(url::Host::Domain(k)) => Ok(k),
                    _ => Err("Malformed IPNS URL").int_err(),
                }?;

                if !key.contains('.') {
                    dataset_url
                } else {
                    info!(ipns_url = %dataset_url, "Resolving DNSLink name");
                    let cid = self.resolve_ipns_dnslink(key).await?;
                    let mut ipfs_url =
                        Url::parse(&format!("ipfs://{}{}", cid, dataset_url.path())).unwrap();
                    ipfs_url.ensure_trailing_slash();
                    info!(ipns_url = %dataset_url, %ipfs_url, "Resolved DNSLink name");
                    ipfs_url
                }
            }
            _ => dataset_url,
        };

        // Re-map IPFS/IPNS urls to HTTP gateway URLs
        // Note: This is for read path only, write path is handled separately
        let dataset_url = match dataset_url.scheme() {
            "ipfs" | "ipns" => {
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

                info!(url = %dataset_url, gateway_url = %gw_url, "Mapping IPFS URL to the configured HTTP gateway");
                gw_url
            }
            _ => dataset_url,
        };

        Ok(dataset_url)
    }

    async fn resolve_ipns_dnslink(&self, domain: &str) -> Result<String, SyncError> {
        let r = trust_dns_resolver::TokioAsyncResolver::tokio_from_system_conf().int_err()?;
        let query = format!("_dnslink.{}", domain);
        let result = r.txt_lookup(&query).await.int_err()?;

        let dnslink_re = regex::Regex::new(r"_?dnslink=/ipfs/(.*)").unwrap();

        for record in result {
            let data = record.to_string();
            debug!(%data, "Observed TXT record");

            if let Some(c) = dnslink_re.captures(&data) {
                return Ok(c.get(1).unwrap().as_str().to_owned());
            }
        }
        Err(DnsLinkResolutionError { record: query }.int_err().into())
    }

    async fn get_dataset_reader(
        &self,
        dataset_ref: &DatasetRefAny,
    ) -> Result<Arc<dyn Dataset>, SyncError> {
        let dataset = if let Some(local_ref) = dataset_ref.as_local_ref() {
            self.local_repo.get_dataset(&local_ref).await?
        } else {
            let remote_ref = dataset_ref.as_remote_ref().unwrap();
            let url = self.resolve_remote_dataset_url(&remote_ref).await?;
            self.dataset_factory.get_dataset(&url, false)?
        };

        match dataset.as_metadata_chain().get_ref(&BlockRef::Head).await {
            Ok(_) => Ok(dataset),
            Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                dataset_ref: dataset_ref.clone(),
            }
            .into()),
            Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    async fn get_dataset_writer(
        &self,
        dataset_ref: &DatasetRefAny,
        create_if_not_exists: bool,
    ) -> Result<Box<dyn DatasetBuilder>, SyncError> {
        if let Some(local_ref) = dataset_ref.as_local_ref() {
            if create_if_not_exists {
                Ok(Box::new(WrapperDatasetBuilder::new(
                    self.local_repo.get_or_create_dataset(&local_ref).await?,
                )))
            } else {
                Ok(Box::new(NullDatasetBuilder::new(
                    self.local_repo.get_dataset(&local_ref).await?,
                )))
            }
        } else {
            let remote_ref = dataset_ref.as_remote_ref().unwrap();
            let url = self.resolve_remote_dataset_url(&remote_ref).await?;
            let dataset = self
                .dataset_factory
                .get_dataset(&url, create_if_not_exists)?;

            if !create_if_not_exists {
                match dataset.as_metadata_chain().get_ref(&BlockRef::Head).await {
                    Ok(_) => Ok(()),
                    Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                        dataset_ref: dataset_ref.clone(),
                    }
                    .into()),
                    Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                    Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
                }?;
            }

            Ok(Box::new(NullDatasetBuilder::new(dataset)))
        }
    }

    async fn sync_generic(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        opts: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let src_dataset = self.get_dataset_reader(src).await?;
        let src_is_local = src.as_local_ref().is_some();
        let dst_dataset_builder = self
            .get_dataset_writer(dst, opts.create_if_not_exists)
            .await?;

        let validation = if opts.trust_source.unwrap_or(src_is_local) {
            AppendValidation::None
        } else {
            AppendValidation::Full
        };

        info!("Starting sync using Simple Transfer Protocol");
        match SimpleTransferProtocol
            .sync(
                src_dataset.as_ref(),
                src,
                dst_dataset_builder.as_dataset(),
                dst,
                validation,
                opts.trust_source.unwrap_or(src_is_local),
                opts.force,
            )
            .await
        {
            Ok(result) => {
                info!(?result, "Sync completed");
                dst_dataset_builder.finish().await?;
                Ok(result)
            }
            Err(error) => {
                info!(?error, "Sync failed");
                dst_dataset_builder.discard().await?;
                Err(error)
            }
        }
    }

    async fn sync_to_ipfs(
        &self,
        src: &DatasetRefLocal,
        dst_url: &Url,
        opts: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        // Resolve key
        let key_id = match (dst_url.host_str(), dst_url.path()) {
            (Some(h), "" | "/") => Ok(h),
            _ => Err("Malformed IPFS URL".int_err()),
        }?;

        let keys = self.ipfs_client.key_list().await.int_err()?;

        let key = if let Some(key) = keys.into_iter().find(|k| k.id == key_id) {
            Ok(key)
        } else {
            Err(format!("IPFS does not have a key with ID {}", key_id).int_err())
        }?;

        info!(key_name = %key.name, key_id = %key.id, "Resolved the key to use for IPNS publishing");

        // Resolve and compare heads
        let src_dataset = self.local_repo.get_dataset(src).await?;
        let src_head = src_dataset
            .as_metadata_chain()
            .get_ref(&BlockRef::Head)
            .await
            .int_err()?;

        // If we try to access the IPNS key via HTTP gateway rigt away this may take a very long time
        // if the key does not exist, as IPFS will be reaching out to remote nodes. To avoid long wait
        // times on first push we make an assumption that this key is owned by the local IPFS node and
        // try resolving it with a short timeout. If resolution fails - we assume that the key was not published yet.
        let (old_cid, dst_head) = match self.ipfs_client.name_resolve_local(&key.id).await? {
            None => {
                info!("Key does not resolve locally - asumming it's unpublished");
                Ok((None, None))
            }
            Some(old_cid) => {
                info!(%old_cid, "Attempting to read remote head");
                let dst_http_url = self
                    .resolve_remote_dataset_url(&DatasetRefRemote::from(dst_url))
                    .await?;
                let dst_dataset = self.dataset_factory.get_dataset(&dst_http_url, false)?;
                match dst_dataset
                    .as_metadata_chain()
                    .get_ref(&BlockRef::Head)
                    .await
                {
                    Ok(head) => Ok((Some(old_cid), Some(head))),
                    Err(GetRefError::NotFound(_)) => Ok((None, None)),
                    Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                    Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
                }
            }
        }?;

        info!(?src_head, ?dst_head, "Resolved heads");

        if Some(&src_head) == dst_head.as_ref() {
            // IPNS entries have a limited lifetime
            // so even if data is up-to-date we re-publish to keep the entry alive.
            let cid = old_cid.unwrap();
            info!(%cid, "Refreshing IPNS entry");
            let _id = self
                .ipfs_client
                .name_publish(
                    &cid,
                    PublishOptions {
                        key: Some(&key.name),
                        ..Default::default()
                    },
                )
                .await?;

            return Ok(SyncResult::UpToDate);
        }

        if !opts.create_if_not_exists && dst_head.is_none() {
            return Err(DatasetNotFoundError::new(dst_url).into());
        }

        // Analyze the block interval
        let num_blocks = match src_dataset
            .as_metadata_chain()
            .iter_blocks_interval(&src_head, dst_head.as_ref(), opts.force)
            .try_count()
            .await
        {
            Ok(v) => Ok(v),
            Err(IterBlocksError::RefNotFound(e)) => Err(SyncError::Internal(e.int_err())),
            Err(IterBlocksError::BlockNotFound(e)) => Err(CorruptedSourceError {
                message: "Source metadata chain is broken".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(IterBlocksError::InvalidInterval(e)) => Err(DatasetsDivergedError {
                src_head: e.head,
                dst_head: e.tail,
            }
            .into()),
            Err(IterBlocksError::Access(e)) => Err(SyncError::Access(e)),
            Err(IterBlocksError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        // Add files to IPFS
        info!("Adding files to IPFS");
        let cid = self.add_to_ipfs(src).await?;

        // Publish to IPNS
        info!(%cid, "Publishing to IPNS");
        let _id = self
            .ipfs_client
            .name_publish(
                &cid,
                PublishOptions {
                    key: Some(&key.name),
                    resolve: Some(false),
                    ..Default::default()
                },
            )
            .await?;

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks,
        })
    }

    async fn add_to_ipfs(&self, src: &DatasetRefLocal) -> Result<String, SyncError> {
        let source_url = self.local_repo.get_dataset_url(src).await.int_err()?;
        let source_path = source_url.to_file_path().unwrap();

        let cid = self
            .ipfs_client
            .add_path(
                source_path,
                AddOptions {
                    // TODO: We are currently including the "/cache" directory when pushing to remotes
                    // this is to allow ingest tasks to resume gracefully with minimal work.
                    // But this does not follow the ODF spec and should be revisited.
                    ignore: Some(&["/config", "/info"]),
                },
            )
            .await?;

        Ok(cid)
    }

    async fn sync_impl(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        opts: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let span = info_span!("Dataset sync", %src, %dst);
        let _span_guard = span.enter();

        match (src, dst) {
            (_, DatasetRefAny::Url(dst_url)) if dst_url.scheme() == "ipfs" => {
                Err(UnsupportedProtocolError {
                    url: dst_url.as_ref().clone(),
                    message: Some(
                        concat!(
                            "Cannot sync to ipfs://{CID} URLs since IPFS ",
                            "is a content-addressable system ",
                            "and the CID changes with every update ",
                            "to the data. Consider using IPNS instead.",
                        )
                        .to_owned(),
                    ),
                }
                .into())
            }
            (_, DatasetRefAny::Url(dst_url)) if dst_url.scheme() == "ipns" => {
                if let Some(src) = src.as_local_ref() {
                    match dst_url.path() {
                        "" | "/" => self.sync_to_ipfs(&src, dst_url, opts).await,
                        _ => Err(UnsupportedProtocolError {
                            url: dst_url.as_ref().clone(),
                            message: Some(
                                concat!(
                                    "Cannot use a sub-path when syncing to ipns:// URL. ",
                                    "Only a single dataset per IPNS key is currently supported.",
                                )
                                .to_owned(),
                            ),
                        }
                        .into()),
                    }
                } else {
                    Err(UnsupportedProtocolError {
                        url: dst_url.as_ref().clone(),
                        message: Some(
                            concat!(
                                "Syncing from a remote repository directly to IPFS ",
                                "is not currently supported. Consider pulling the dataset ",
                                "locally and then pushing to IPFS.",
                            )
                            .to_owned(),
                        ),
                    }
                    .into())
                }
            }
            (_, _) => self.sync_generic(src, dst, opts).await,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
impl SyncService for SyncServiceImpl {
    async fn sync(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        opts: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.sync_impl(src, dst, opts).await {
            Ok(result) => {
                listener.success(&result);
                Ok(result)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    // TODO: Parallelism
    async fn sync_multi(
        &self,
        src_dst: &mut dyn Iterator<Item = (DatasetRefAny, DatasetRefAny)>,
        opts: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<SyncResultMulti> {
        let mut results = Vec::new();

        for (src, dst) in src_dst {
            let listener = listener.as_ref().and_then(|l| l.begin_sync(&src, &dst));
            let result = self.sync(&src, &dst, opts.clone(), listener).await;
            results.push(SyncResultMulti { src, dst, result });
        }

        results
    }

    async fn ipfs_add(&self, src: &DatasetRefLocal) -> Result<String, SyncError> {
        self.add_to_ipfs(src).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Adapter for dataset builder that does not return a handle
/// and treats local and remote datasets the same way.
#[async_trait::async_trait]
trait DatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset;
    async fn finish(&self) -> Result<(), CreateDatasetError>;
    async fn discard(&self) -> Result<(), InternalError>;
}

struct NullDatasetBuilder {
    dataset: Arc<dyn Dataset>,
}

impl NullDatasetBuilder {
    pub fn new(dataset: Arc<dyn Dataset>) -> Self {
        Self { dataset }
    }
}

#[async_trait::async_trait]
impl DatasetBuilder for NullDatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset {
        self.dataset.as_ref()
    }

    async fn finish(&self) -> Result<(), CreateDatasetError> {
        Ok(())
    }

    async fn discard(&self) -> Result<(), InternalError> {
        Ok(())
    }
}

struct WrapperDatasetBuilder {
    builder: Box<dyn crate::domain::DatasetBuilder>,
}

impl WrapperDatasetBuilder {
    fn new(builder: Box<dyn crate::domain::DatasetBuilder>) -> Self {
        Self { builder }
    }
}

#[async_trait::async_trait]
impl DatasetBuilder for WrapperDatasetBuilder {
    fn as_dataset(&self) -> &dyn Dataset {
        self.builder.as_dataset()
    }

    async fn finish(&self) -> Result<(), CreateDatasetError> {
        self.builder.finish().await?;
        Ok(())
    }

    async fn discard(&self) -> Result<(), InternalError> {
        self.builder.discard().await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

trait UrlExt {
    fn ensure_trailing_slash(&mut self);
}

impl UrlExt for Url {
    fn ensure_trailing_slash(&mut self) {
        if !self.path().ends_with('/') {
            self.set_path(&format!("{}/", self.path()));
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Failed to resolve DNSLink record: {record}")]
struct DnsLinkResolutionError {
    pub record: String,
}
