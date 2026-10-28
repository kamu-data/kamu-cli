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
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;
use kamu_datasets::ResolvedDataset;
use url::Url;

use crate::utils::ipfs_wrapper::*;
use crate::utils::simple_transfer_protocol::{
    SimpleProtocolTransferOptions,
    SimpleTransferProtocol,
};
use crate::utils::smart_transfer_protocol::{
    SmartTransferProtocolClient,
    TransferOptions as SmartTransferOptions,
};
use crate::utils::sync_increment::sync_increment;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SyncServiceImpl {
    dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
    smart_transfer_protocol: Arc<dyn SmartTransferProtocolClient>,
    simple_transfer_protocol: Arc<SimpleTransferProtocol>,
    ipfs_client: Arc<IpfsClient>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SyncService)]
impl SyncServiceImpl {
    pub fn new(
        dataset_factory: Arc<dyn odf::dataset::DatasetFactory>,
        smart_transfer_protocol: Arc<dyn SmartTransferProtocolClient>,
        simple_transfer_protocol: Arc<SimpleTransferProtocol>,
        ipfs_client: Arc<IpfsClient>,
    ) -> Self {
        Self {
            dataset_factory,
            smart_transfer_protocol,
            simple_transfer_protocol,
            ipfs_client,
        }
    }

    /// Whether a source's blocks and objects are validated and re-hashed
    fn source_trust(src: &SyncRef, opts: SyncOptions) -> (odf::dataset::AppendValidation, bool) {
        let trust_source = opts.trust_source.unwrap_or(src.is_local());
        let validation = if trust_source {
            odf::dataset::AppendValidation::None
        } else {
            odf::dataset::AppendValidation::Full
        };
        (validation, trust_source)
    }

    fn source_dataset(src: &SyncRef) -> Arc<dyn odf::Dataset> {
        match src {
            SyncRef::Local(resolved_dataset) => (**resolved_dataset).clone(),
            SyncRef::Remote(src_remote) => src_remote.dataset.clone(),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?src, ?dst, ?opts))]
    async fn push_generic(
        &self,
        src: SyncRef,
        dst: SyncRefRemote,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        let (validation, trust_source_hashes) = Self::source_trust(&src, opts);

        tracing::info!("Starting sync using Simple Transfer Protocol");

        self.simple_transfer_protocol
            .push(
                &src.as_internal_any_ref(),
                Self::source_dataset(&src),
                dst.dataset,
                validation,
                trust_source_hashes,
                opts.force,
                SimpleProtocolTransferOptions::default(),
                listener,
            )
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?src, ?dst, ?opts))]
    async fn transfer_generic(
        &self,
        src: SyncRef,
        dst: &LocalSyncTarget,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncTransfer, SyncError> {
        let (validation, trust_source_hashes) = Self::source_trust(&src, opts);

        tracing::info!("Starting transfer using Simple Transfer Protocol");

        self.simple_transfer_protocol
            .transfer_into_local(
                &src.as_internal_any_ref(),
                Self::source_dataset(&src),
                dst,
                validation,
                trust_source_hashes,
                opts.force,
                SimpleProtocolTransferOptions {
                    visibility_for_created_dataset: opts.dataset_visibility,
                    ..Default::default()
                },
                listener,
            )
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%src_url, ?dst, ?opts))]
    async fn transfer_smart_pull(
        &self,
        src_url: &Url,
        dst: &LocalSyncTarget,
        opts: SyncOptions,
    ) -> Result<SyncTransfer, SyncError> {
        let http_src_url = src_url.odf_to_transport_protocol()?;

        tracing::info!("Starting transfer using Smart Transfer Protocol (Pull flow)");

        self.smart_transfer_protocol
            .pull_protocol_client_flow(
                &http_src_url,
                dst,
                SmartTransferOptions {
                    force_update_if_diverged: opts.force,
                    visibility_for_created_dataset: opts.dataset_visibility,
                    ..Default::default()
                },
            )
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?src, %dst_url, ?opts))]
    async fn sync_smart_push_transfer_protocol(
        &self,
        src: SyncRef,
        dst_url: &Url,
        opts: SyncOptions,
    ) -> Result<SyncResult, SyncError> {
        let http_dst_url = dst_url.odf_to_transport_protocol()?;

        // TODO: move head check into the protocol
        let http_dst_dataset_view = self
            .dataset_factory
            .get_dataset(&http_dst_url, false)
            .await?;
        let maybe_dst_head = try_resolve_head(http_dst_dataset_view.as_metadata_chain()).await?;

        let src_dataset = Self::source_dataset(&src);

        tracing::info!("Starting sync using Smart Transfer Protocol (Push flow)");
        self.smart_transfer_protocol
            .push_protocol_client_flow(
                src_dataset,
                &http_dst_url,
                maybe_dst_head.as_ref(),
                SmartTransferOptions {
                    force_update_if_diverged: opts.force,
                    visibility_for_created_dataset: opts.dataset_visibility,
                    ..Default::default()
                },
            )
            .await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dst_url, ?opts))]
    async fn sync_to_ipfs(
        &self,
        src_dataset: Arc<dyn odf::Dataset>,
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
            Err(format!("IPFS does not have a key with ID {key_id}").int_err())
        }?;

        tracing::info!(key_name = %key.name, key_id = %key.id, "Resolved the key to use for IPNS publishing");

        // Resolve and compare heads
        let src_head = src_dataset
            .as_metadata_chain()
            .resolve_ref(&odf::BlockRef::Head)
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
                    (None, None, None)
                }
                Some(old_cid) => {
                    tracing::info!(%old_cid, "Attempting to read remote head");
                    let mut dst_http_url = dst_url.clone();
                    dst_http_url.ensure_trailing_slash();
                    let dst_dataset = self
                        .dataset_factory
                        .get_dataset(&dst_http_url, false)
                        .await?;
                    match try_resolve_head(dst_dataset.as_metadata_chain()).await? {
                        Some(dst_head) => {
                            let chains_comparison = MetadataChainComparator::compare_chains(
                                src_dataset.as_metadata_chain(),
                                &src_head,
                                dst_dataset.as_metadata_chain(),
                                Some(&dst_head),
                                &NullCompareChainsListener,
                            )
                            .await?;

                            (Some(old_cid), Some(dst_head), Some(chains_comparison))
                        }
                        None => (None, None, None),
                    }
                }
            };

        tracing::info!(?src_head, ?dst_head, "Resolved heads");

        if !opts.create_if_not_exists && dst_head.is_none() {
            return Err(DatasetAnyRefUnresolvedError::new(dst_url).into());
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

                return Ok(SyncResult::UpToDate);
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
            Some(CompareChainsResult::DifferentDatasets {
                lhs_dataset_id: src_dataset_id,
                rhs_dataset_id: dst_dataset_id,
            }) => {
                // Even a forced sync must not replace a dataset with another one
                return Err(SyncError::DatasetIdMismatch(DatasetIdMismatchError {
                    src_dataset_id,
                    dst_dataset_id,
                }));
            }
        }

        let num_blocks = match chains_comparison {
            Some(CompareChainsResult::Equal | CompareChainsResult::DifferentDatasets { .. }) => {
                unreachable!()
            }
            Some(CompareChainsResult::LhsAhead { lhs_ahead_blocks }) => lhs_ahead_blocks.len(),
            None
            | Some(
                CompareChainsResult::LhsBehind { .. } | CompareChainsResult::Divergence { .. },
            ) => {
                let mut num_blocks = 0;

                use futures::TryStreamExt;
                let mut block_stream = src_dataset.as_metadata_chain().iter_blocks_interval(
                    (&src_head).into(),
                    None,
                    false,
                );

                while let Some((_, _)) = block_stream
                    .try_next()
                    .await
                    .map_err(SimpleTransferProtocol::map_block_iteration_error)?
                {
                    num_blocks += 1;
                }

                num_blocks
            }
        };

        // Read before publishing, so a failure here aborts the push
        let increment = sync_increment(
            src_dataset.as_metadata_chain(),
            dst_head.as_ref(),
            &src_head,
        )
        .await?;

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

        Ok(SyncResult::Updated {
            old_head: dst_head,
            new_head: src_head,
            num_blocks: u64::try_from(num_blocks).unwrap(),
            increment,
        })
    }

    async fn add_to_ipfs(&self, src_dataset: &dyn odf::Dataset) -> Result<String, IpfsAddError> {
        let source_url = src_dataset.get_storage_internal_url();
        let source_path = source_url.to_file_path().map_err(|_| {
            IpfsAddError::UnsupportedIpfsStorageType({
                UnsupportedIpfsStorageTypeError {
                    url: source_url.clone(),
                }
            })
        })?;

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

    #[tracing::instrument(level = "info", name = "sync", skip_all, fields(src=?src, dst=?dst))]
    async fn sync_impl(
        &self,
        src: SyncRef,
        dst: SyncRefRemote,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        match (&src, dst.url.scheme()) {
            // * -> ipfs
            (_, "ipfs") => Err(odf::dataset::UnsupportedProtocolError {
                url: dst.url.as_ref().clone(),
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
            .into()),
            // <remote> -> ipns
            (SyncRef::Remote(_), "ipns") => Err(odf::dataset::UnsupportedProtocolError {
                url: dst.url.as_ref().clone(),
                message: Some(
                    concat!(
                        "Syncing from a remote repository directly to IPFS ",
                        "is not currently supported. Consider pulling the dataset ",
                        "locally and then pushing to IPFS.",
                    )
                    .to_owned(),
                ),
            }
            .into()),
            // <local> -> ipns
            (SyncRef::Local(src_dataset), "ipns") => match dst.url.path() {
                "" | "/" => {
                    self.sync_to_ipfs((**src_dataset).clone(), dst.url.as_ref(), opts)
                        .await
                }
                _ => Err(odf::dataset::UnsupportedProtocolError {
                    url: dst.url.as_ref().clone(),
                    message: Some(
                        concat!(
                            "Cannot use a sub-path when syncing to ipns:// URL. ",
                            "Only a single dataset per IPNS key is supported.",
                        )
                        .to_owned(),
                    ),
                }
                .into()),
            },
            // odf -> odf
            (SyncRef::Remote(src_remote), _)
                if src_remote.url.is_odf_protocol() && dst.url.is_odf_protocol() =>
            {
                Err(odf::dataset::UnsupportedProtocolError {
                    url: dst.url.as_ref().clone(),
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
            // * -> odf
            (_, _) if dst.url.is_odf_protocol() => {
                let dst_url = dst.url.clone();
                self.sync_smart_push_transfer_protocol(src, dst_url.as_ref(), opts)
                    .await
            }
            // * -> *
            (_, _) => self.push_generic(src, dst, opts, listener).await,
        }
    }

    #[tracing::instrument(level = "info", name = "transfer_into_local", skip_all, fields(src=?src, dst=?dst))]
    async fn transfer_into_local_impl(
        &self,
        src: SyncRef,
        dst: &LocalSyncTarget,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncTransfer, SyncError> {
        match &src {
            SyncRef::Remote(src_remote) if src_remote.url.is_odf_protocol() => {
                let src_url = src_remote.url.clone();
                self.transfer_smart_pull(src_url.as_ref(), dst, opts).await
            }
            SyncRef::Remote(_) | SyncRef::Local(_) => {
                self.transfer_generic(src, dst, opts, listener).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SyncService for SyncServiceImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?request, ?options))]
    async fn sync(
        &self,
        request: SyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();
        match self
            .sync_impl(request.src, request.dst, options, listener.clone())
            .await
        {
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

    #[tracing::instrument(level = "debug", skip_all, fields(?request, ?options))]
    async fn transfer_into_local(
        &self,
        request: LocalSyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncTransfer, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();
        self.transfer_into_local_impl(request.src, &request.dst, options, listener)
            .await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ipfs_add(&self, src: ResolvedDataset) -> Result<String, IpfsAddError> {
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
