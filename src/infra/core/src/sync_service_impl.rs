// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::services::sync_service::DatasetNotFoundError;
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use super::utils::smart_transfer_protocol::SmartTransferProtocolClient;
use crate::resolve_remote_dataset_url;
use crate::utils::ipfs_wrapper::*;
use crate::utils::simple_transfer_protocol::SimpleTransferProtocol;
use crate::utils::smart_transfer_protocol::TransferOptions;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SyncServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    dataset_factory: Arc<dyn DatasetFactory>,
    smart_transfer_protocol: Arc<dyn SmartTransferProtocolClient>,
    ipfs_client: Arc<IpfsClient>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SyncService)]
impl SyncServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        dataset_factory: Arc<dyn DatasetFactory>,
        smart_transfer_protocol: Arc<dyn SmartTransferProtocolClient>,
        ipfs_client: Arc<IpfsClient>,
    ) -> Self {
        Self {
            remote_repo_reg,
            dataset_factory,
            smart_transfer_protocol,
            ipfs_client,
        }
    }

    async fn sync_generic(
        &self,
        src: SyncRequestSource,
        dst: SyncRequestDestination,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<(SyncResult, Arc<dyn Dataset>), SyncError> {
        let src_is_local = src.sync_ref.is_local();

        let validation = if opts.trust_source.unwrap_or(src_is_local) {
            AppendValidation::None
        } else {
            AppendValidation::Full
        };

        let trust_source_hashes = opts.trust_source.unwrap_or(src_is_local);

        tracing::info!("Starting sync using Simple Transfer Protocol");

        SimpleTransferProtocol
            .sync(
                &src.src_ref,
                src.dataset,
                dst.maybe_dataset,
                dst.maybe_dataset_factory,
                validation,
                trust_source_hashes,
                opts.force,
                listener,
            )
            .await
    }

    async fn sync_smart_pull_transfer_protocol(
        &self,
        src_url: &Url,
        dst: SyncRequestDestination,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<(SyncResult, Arc<dyn Dataset>), SyncError> {
        let http_src_url = src_url.odf_to_transport_protocol()?;

        tracing::info!("Starting sync using Smart Transfer Protocol (Pull flow)");

        self.smart_transfer_protocol
            .pull_protocol_client_flow(
                &http_src_url,
                dst.maybe_dataset,
                dst.maybe_dataset_factory,
                listener,
                TransferOptions {
                    force_update_if_diverged: opts.force,
                    ..Default::default()
                },
            )
            .await
    }

    async fn sync_smart_push_transfer_protocol<'a>(
        &'a self,
        src: SyncRequestSource,
        dst_url: &Url,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<(SyncResult, Arc<dyn Dataset>), SyncError> {
        let http_dst_url = dst_url.odf_to_transport_protocol()?;

        // TODO: move head check into the protocol
        let maybe_dst_head = match self.dataset_factory.get_dataset(&http_dst_url, false).await {
            Ok(http_dst_dataset_view) => match http_dst_dataset_view
                .as_metadata_chain()
                .resolve_ref(&BlockRef::Head)
                .await
            {
                Ok(head) => Ok(Some(head)),
                Err(GetRefError::NotFound(_)) => Ok(None),
                Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
            },
            Err(e) => Err(e.into()),
        }?;

        tracing::info!("Starting sync using Smart Transfer Protocol (Push flow)");
        self.smart_transfer_protocol
            .push_protocol_client_flow(
                src.dataset,
                &http_dst_url,
                maybe_dst_head.as_ref(),
                listener,
                TransferOptions {
                    force_update_if_diverged: opts.force,
                    visibility_for_created_dataset: opts.dataset_visibility,
                    ..Default::default()
                },
            )
            .await
    }

    async fn sync_to_ipfs(
        &self,
        src_dataset: Arc<dyn Dataset>,
        dst_url: &Url,
        opts: SyncOptions,
    ) -> Result<(SyncResult, Arc<dyn Dataset>), SyncError> {
        // Resolve key
        let key_id = match (dst_url.host_str(), dst_url.path()) {
            (Some(h), "" | "/") => Ok(h),
            _ => Err("Malformed IPFS URL".int_err()),
        }?;

        let keys = self.ipfs_client.key_list().await.int_err()?;

        let key = if let Some(key) = keys.into_iter().find(|k| k.id == key_id) {
            Ok(key)
        } else {
            Err(format!("IPFS does not have a key with ID {key_id}").int_err())
        }?;

        tracing::info!(key_name = %key.name, key_id = %key.id, "Resolved the key to use for IPNS publishing");

        // Resolve and compare heads
        let src_head = src_dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
            .int_err()?;

        // If we try to access the IPNS key via HTTP gateway right away this may take a
        // very long time if the key does not exist, as IPFS will be reaching
        // out to remote nodes. To avoid long wait times on first push we make
        // an assumption that this key is owned by the local IPFS node and
        // try resolving it with a short timeout. If resolution fails - we assume that
        // the key was not published yet.
        let (old_cid, dst_head, chains_comparison) =
            match self.ipfs_client.name_resolve_local(&key.id).await? {
                None => {
                    tracing::info!("Key does not resolve locally - assuming it's unpublished");
                    Ok((None, None, None))
                }
                Some(old_cid) => {
                    tracing::info!(%old_cid, "Attempting to read remote head");
                    let dst_http_url = resolve_remote_dataset_url(
                        self.remote_repo_reg.as_ref(),
                        &DatasetRefRemote::from(dst_url),
                    )?;
                    let dst_dataset = self
                        .dataset_factory
                        .get_dataset(&dst_http_url, false)
                        .await?;
                    match dst_dataset
                        .as_metadata_chain()
                        .resolve_ref(&BlockRef::Head)
                        .await
                    {
                        Ok(dst_head) => {
                            let chains_comparison = MetadataChainComparator::compare_chains(
                                src_dataset.as_metadata_chain(),
                                &src_head,
                                dst_dataset.as_metadata_chain(),
                                Some(&dst_head),
                                &NullCompareChainsListener,
                            )
                            .await?;

                            Ok((Some(old_cid), Some(dst_head), Some(chains_comparison)))
                        }
                        Err(GetRefError::NotFound(_)) => Ok((None, None, None)),
                        Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                        Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
                    }
                }
            }?;

        tracing::info!(?src_head, ?dst_head, "Resolved heads");

        if !opts.create_if_not_exists && dst_head.is_none() {
            return Err(DatasetNotFoundError::new(dst_url).into());
        }

        match chains_comparison {
            Some(CompareChainsResult::Equal) => {
                // IPNS entries have a limited lifetime
                // so even if data is up-to-date we re-publish to keep the entry alive.
                let cid = old_cid.unwrap();
                tracing::info!(%cid, "Refreshing IPNS entry");
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

                return Ok((SyncResult::UpToDate, src_dataset));
            }
            Some(CompareChainsResult::LhsAhead { .. }) | None => { /* Skip */ }
            Some(CompareChainsResult::LhsBehind {
                ref rhs_ahead_blocks,
            }) => {
                if !opts.force {
                    return Err(SyncError::DestinationAhead(DestinationAheadError {
                        src_head,
                        dst_head: dst_head.unwrap(),
                        dst_ahead_size: rhs_ahead_blocks.len(),
                    }));
                }
            }
            Some(CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs,
                uncommon_blocks_in_rhs,
            }) => {
                if !opts.force {
                    return Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
                        src_head,
                        dst_head: dst_head.unwrap(),
                        detail: Some(DatasetsDivergedErrorDetail {
                            uncommon_blocks_in_dst: uncommon_blocks_in_rhs,
                            uncommon_blocks_in_src: uncommon_blocks_in_lhs,
                        }),
                    }));
                }
            }
        }

        let num_blocks = match chains_comparison {
            Some(CompareChainsResult::Equal) => unreachable!(),
            Some(CompareChainsResult::LhsAhead { lhs_ahead_blocks }) => lhs_ahead_blocks.len(),
            None
            | Some(
                CompareChainsResult::LhsBehind { .. } | CompareChainsResult::Divergence { .. },
            ) => {
                let mut num_blocks = 0;

                use futures::TryStreamExt;
                let mut block_stream = src_dataset
                    .as_metadata_chain()
                    .iter_blocks_interval(&src_head, None, false);

                while let Some((_, _)) = block_stream.try_next().await.map_err(|e| match e {
                    IterBlocksError::RefNotFound(e) => SyncError::Internal(e.int_err()),
                    IterBlocksError::BlockNotFound(e) => CorruptedSourceError {
                        message: "Source metadata chain is broken".to_owned(),
                        source: Some(e.into()),
                    }
                    .into(),
                    IterBlocksError::BlockVersion(e) => CorruptedSourceError {
                        message: "Source metadata chain is broken".to_owned(),
                        source: Some(e.into()),
                    }
                    .into(),
                    IterBlocksError::BlockMalformed(e) => CorruptedSourceError {
                        message: "Source metadata chain is broken".to_owned(),
                        source: Some(e.into()),
                    }
                    .into(),
                    IterBlocksError::InvalidInterval(_) => unreachable!(),
                    IterBlocksError::Access(e) => SyncError::Access(e),
                    IterBlocksError::Internal(e) => SyncError::Internal(e),
                })? {
                    num_blocks += 1;
                }

                num_blocks
            }
        };

        // Add files to IPFS
        tracing::info!("Adding files to IPFS");
        let cid = self.add_to_ipfs(src_dataset.as_ref()).await?;

        // Publish to IPNS
        tracing::info!(%cid, "Publishing to IPNS");
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

        Ok((
            SyncResult::Updated {
                old_head: dst_head,
                new_head: src_head,
                num_blocks: num_blocks as u64,
            },
            src_dataset,
        ))
    }

    async fn add_to_ipfs(&self, src_dataset: &dyn Dataset) -> Result<String, InternalError> {
        let source_url = src_dataset
            .try_get_ipfs_compatible_url()
            .expect("Dataset must provide IPFS-compatible URL");
        let source_path = source_url.to_file_path().unwrap();

        let cid = self
            .ipfs_client
            .add_path(
                source_path,
                AddOptions {
                    ignore: Some(&["/config", "/info"]),
                },
            )
            .await?;

        Ok(cid)
    }

    #[tracing::instrument(level = "info", name = "sync", skip_all, fields(src=%src.src_ref, dst=%dst.dst_ref))]
    async fn sync_impl(
        &self,
        src: SyncRequestSource,
        dst: SyncRequestDestination,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<(SyncResult, Arc<dyn Dataset>), SyncError> {
        match (&src.sync_ref, &dst.sync_ref) {
            // * -> ipfs
            (_, SyncRef::Remote(dst_url)) if dst_url.scheme() == "ipfs" => {
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
            // <remote> -> ipns
            (SyncRef::Remote(_), SyncRef::Remote(dst_url)) if dst_url.scheme() == "ipns" => {
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
            // <local> -> ipns
            (SyncRef::Local(_), SyncRef::Remote(dst_url)) if dst_url.scheme() == "ipns" => {
                match dst_url.path() {
                    "" | "/" => self.sync_to_ipfs(src.dataset, dst_url, opts).await,
                    _ => Err(UnsupportedProtocolError {
                        url: dst_url.as_ref().clone(),
                        message: Some(
                            concat!(
                                "Cannot use a sub-path when syncing to ipns:// URL. ",
                                "Only a single dataset per IPNS key is supported.",
                            )
                            .to_owned(),
                        ),
                    }
                    .into()),
                }
            }
            // odf -> odf
            (SyncRef::Remote(src_url), SyncRef::Remote(dst_url))
                if src_url.is_odf_protocol() && dst_url.is_odf_protocol() =>
            {
                Err(UnsupportedProtocolError {
                    url: dst_url.as_ref().clone(),
                    message: Some(
                        concat!(
                            "Syncing from a remote ODF repository directly to remote ODF ",
                            "repository is not currently supported. Consider pulling the ",
                            "dataset locally and then pushing to ODF repository.",
                        )
                        .to_owned(),
                    ),
                }
                .into())
            }
            // odf -> *
            (SyncRef::Remote(src_url), _) if src_url.is_odf_protocol() => {
                self.sync_smart_pull_transfer_protocol(src_url.as_ref(), dst, opts, listener)
                    .await
            }
            // * -> odf
            (_, SyncRef::Remote(dst_url)) if dst_url.is_odf_protocol() => {
                self.sync_smart_push_transfer_protocol(src, dst_url.as_ref(), opts, listener)
                    .await
            }
            // * -> *
            (_, _) => self.sync_generic(src, dst, opts, listener).await,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SyncService for SyncServiceImpl {
    async fn sync(
        &self,
        request: SyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<(SyncResult, Arc<dyn Dataset>), SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();
        match self
            .sync_impl(request.src, request.dst, options, listener.clone())
            .await
        {
            Ok((result, dataset)) => {
                listener.success(&result);
                Ok((result, dataset))
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }

    async fn ipfs_add(&self, src: Arc<dyn Dataset>) -> Result<String, InternalError> {
        self.add_to_ipfs(src.as_ref()).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait UrlExt {
    fn ensure_trailing_slash(&mut self);
    fn is_odf_protocol(&self) -> bool;
    fn as_odf_protocol(&self) -> Result<Url, InternalError>;

    /// Converts from odf+http(s) scheme to plain http(s)
    fn odf_to_transport_protocol(&self) -> Result<Url, InternalError>;
}

impl UrlExt for Url {
    fn ensure_trailing_slash(&mut self) {
        if !self.path().ends_with('/') {
            self.set_path(&format!("{}/", self.path()));
        }
    }

    fn as_odf_protocol(&self) -> Result<Url, InternalError> {
        let url_string = self.as_str().replace("http", "odf+http");
        Url::from_str(&url_string).int_err()
    }

    fn is_odf_protocol(&self) -> bool {
        self.scheme().starts_with("odf+")
    }

    fn odf_to_transport_protocol(&self) -> Result<Url, InternalError> {
        let s = self
            .as_str()
            .strip_prefix("odf+")
            .ok_or_else(|| format!("Expected odf+http(s) URL but got: {self}").int_err())?;
        Url::parse(s).int_err()
    }
}
