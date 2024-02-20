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
use kamu_core::services::sync_service::DatasetNotFoundError;
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;
use opendatafabric::*;
use url::Url;

use super::utils::smart_transfer_protocol::SmartTransferProtocolClient;
use crate::utils::ipfs_wrapper::*;
use crate::utils::simple_transfer_protocol::{DatasetFactoryFn, SimpleTransferProtocol};
use crate::utils::smart_transfer_protocol::ObjectTransferOptions;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SyncServiceImpl {
    remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    dataset_factory: Arc<dyn DatasetFactory>,
    smart_transfer_protocol: Arc<dyn SmartTransferProtocolClient>,
    ipfs_client: Arc<IpfsClient>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SyncService)]
impl SyncServiceImpl {
    pub fn new(
        remote_repo_reg: Arc<dyn RemoteRepositoryRegistry>,
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
        dataset_factory: Arc<dyn DatasetFactory>,
        smart_transfer_protocol: Arc<dyn SmartTransferProtocolClient>,
        ipfs_client: Arc<IpfsClient>,
    ) -> Self {
        Self {
            remote_repo_reg,
            dataset_repo,
            dataset_action_authorizer,
            dataset_factory,
            smart_transfer_protocol,
            ipfs_client,
        }
    }

    fn resolve_remote_dataset_url(&self, remote_ref: &DatasetRefRemote) -> Result<Url, SyncError> {
        // TODO: REMOTE ID
        match remote_ref {
            DatasetRefRemote::ID(_, _) => {
                unimplemented!("Syncing remote dataset by ID is not yet supported")
            }
            DatasetRefRemote::Alias(alias)
            | DatasetRefRemote::Handle(DatasetHandleRemote { alias, .. }) => {
                let mut repo = self.remote_repo_reg.get_repository(&alias.repo_name)?;

                repo.url.ensure_trailing_slash();
                Ok(repo.url.join(&format!("{}/", alias.local_alias())).unwrap())
            }
            DatasetRefRemote::Url(url) => {
                let mut dataset_url = url.as_ref().clone();
                dataset_url.ensure_trailing_slash();
                Ok(dataset_url)
            }
        }
    }

    async fn get_dataset_reader(
        &self,
        dataset_ref: &DatasetRefAny,
    ) -> Result<Arc<dyn Dataset>, SyncError> {
        let dataset = match dataset_ref.as_local_ref(|_| !self.dataset_repo.is_multi_tenant()) {
            Ok(local_ref) => {
                let dataset_handle = self.dataset_repo.resolve_dataset_ref(&local_ref).await?;
                self.dataset_action_authorizer
                    .check_action_allowed(&dataset_handle, auth::DatasetAction::Read)
                    .await?;

                self.dataset_repo.get_dataset(&local_ref).await?
            }
            Err(remote_ref) => {
                // TODO: implement authorization checks somehow
                let url = self.resolve_remote_dataset_url(&remote_ref)?;
                self.dataset_factory.get_dataset(&url, false).await?
            }
        };

        match dataset
            .as_metadata_chain()
            .resolve_ref(&BlockRef::Head)
            .await
        {
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
    ) -> Result<(Option<Arc<dyn Dataset>>, Option<DatasetFactoryFn>), SyncError> {
        match dataset_ref.as_local_ref(|_| !self.dataset_repo.is_multi_tenant()) {
            Ok(local_ref) => match self.dataset_repo.get_dataset(&local_ref).await {
                Ok(dataset) => {
                    let dataset_handle = self.dataset_repo.resolve_dataset_ref(&local_ref).await?;
                    self.dataset_action_authorizer
                        .check_action_allowed(&dataset_handle, auth::DatasetAction::Write)
                        .await?;

                    Ok((Some(dataset), None))
                }
                Err(GetDatasetError::NotFound(_)) if create_if_not_exists => {
                    let alias = local_ref.alias().unwrap().clone();
                    let repo = self.dataset_repo.clone();
                    Ok((
                        None,
                        Some(Box::new(move |seed_block| {
                            Box::pin(async move { repo.create_dataset(&alias, seed_block).await })
                        })),
                    ))
                }
                Err(err) => Err(err.into()),
            },
            Err(remote_ref) => {
                // TODO: implement authorization checks somehow
                let url = self.resolve_remote_dataset_url(&remote_ref)?;
                let dataset = self
                    .dataset_factory
                    .get_dataset(&url, create_if_not_exists)
                    .await?;

                if !create_if_not_exists {
                    match dataset
                        .as_metadata_chain()
                        .resolve_ref(&BlockRef::Head)
                        .await
                    {
                        Ok(_) => Ok(()),
                        Err(GetRefError::NotFound(_)) => Err(DatasetNotFoundError {
                            dataset_ref: dataset_ref.clone(),
                        }
                        .into()),
                        Err(GetRefError::Access(e)) => Err(SyncError::Access(e)),
                        Err(GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
                    }?;
                }

                Ok((Some(dataset), None))
            }
        }
    }

    async fn sync_generic(
        &self,
        src_ref: &DatasetRefAny,
        dst_ref: &DatasetRefAny,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        let src_is_local = src_ref
            .as_local_ref(|_| !self.dataset_repo.is_multi_tenant())
            .is_ok();

        let src_dataset = self.get_dataset_reader(src_ref).await?;
        let (dst_dataset, dst_factory) = self
            .get_dataset_writer(dst_ref, opts.create_if_not_exists)
            .await?;

        let validation = if opts.trust_source.unwrap_or(src_is_local) {
            AppendValidation::None
        } else {
            AppendValidation::Full
        };

        let trust_source_hashes = opts.trust_source.unwrap_or(src_is_local);

        tracing::info!("Starting sync using Simple Transfer Protocol");

        SimpleTransferProtocol
            .sync(
                src_ref,
                src_dataset,
                dst_dataset,
                dst_factory,
                validation,
                trust_source_hashes,
                opts.force,
                listener,
            )
            .await
    }

    async fn sync_smart_pull_transfer_protocol(
        &self,
        src_ref: &DatasetRefRemote,
        dst_ref: &DatasetRefAny,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        let odf_src_url = self.resolve_remote_dataset_url(src_ref)?;
        let http_src_url = Url::parse(&odf_src_url.as_str()["odf+".len()..]).unwrap(); // odf+http, odf+https - cut odf+

        let (dst_dataset, dst_factory) = self
            .get_dataset_writer(dst_ref, opts.create_if_not_exists)
            .await?;

        tracing::info!("Starting sync using Smart Transfer Protocol (Pull flow)");

        self.smart_transfer_protocol
            .pull_protocol_client_flow(
                &http_src_url,
                dst_dataset,
                dst_factory,
                listener,
                ObjectTransferOptions::default(),
            )
            .await
    }

    async fn sync_smart_push_transfer_protocol<'a>(
        &'a self,
        src: &DatasetRefAny,
        odf_dst: &DatasetRefRemote,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
        let src_dataset = self.get_dataset_reader(src).await?;

        let odf_dst_url = self.resolve_remote_dataset_url(odf_dst)?;
        let http_dst_url = Url::parse(&odf_dst_url.as_str()[4..]).unwrap(); // odf+http, odf+https - cut odf+

        let http_dst_ref = DatasetRefAny::Url(http_dst_url.clone().into());
        let maybe_dst_head = match self.get_dataset_reader(&http_dst_ref).await {
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
            Err(SyncError::DatasetNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }?;

        tracing::info!("Starting sync using Smart Transfer Protocol (Push flow)");
        self.smart_transfer_protocol
            .push_protocol_client_flow(
                src_dataset,
                &http_dst_url,
                maybe_dst_head.as_ref(),
                listener,
                ObjectTransferOptions::default(),
            )
            .await
    }

    async fn sync_to_ipfs(
        &self,
        src: &DatasetRef,
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

        let src_dataset_handle = self.dataset_repo.resolve_dataset_ref(src).await?;
        self.dataset_action_authorizer
            .check_action_allowed(&src_dataset_handle, auth::DatasetAction::Read)
            .await?;

        // Resolve and compare heads
        let src_dataset = self.dataset_repo.get_dataset(src).await?;
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
                    let dst_http_url =
                        self.resolve_remote_dataset_url(&DatasetRefRemote::from(dst_url))?;
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
                        uncommon_blocks_in_dst: uncommon_blocks_in_rhs,
                        uncommon_blocks_in_src: uncommon_blocks_in_lhs,
                    }));
                }
            }
        }

        let (num_blocks, num_records) = match chains_comparison {
            Some(CompareChainsResult::Equal) => unreachable!(),
            Some(CompareChainsResult::LhsAhead { lhs_ahead_blocks }) => {
                let num_records = lhs_ahead_blocks
                    .iter()
                    .map(|(_, block)| Self::num_data_records_in_block(block))
                    .sum();
                (lhs_ahead_blocks.len(), num_records)
            }
            None
            | Some(
                CompareChainsResult::LhsBehind { .. } | CompareChainsResult::Divergence { .. },
            ) => {
                let mut num_blocks = 0;
                let mut num_records = 0;

                use futures::TryStreamExt;
                let mut block_stream = src_dataset
                    .as_metadata_chain()
                    .iter_blocks_interval(&src_head, None, false);

                while let Some((_, block)) = block_stream.try_next().await.map_err(|e| match e {
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
                    num_records += Self::num_data_records_in_block(&block);
                }

                (num_blocks, num_records)
            }
        };

        // Add files to IPFS
        tracing::info!("Adding files to IPFS");
        let cid = self.add_to_ipfs(src).await?;

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
            num_blocks: num_blocks as u64,
            num_records,
        })
    }

    fn num_data_records_in_block(block: &MetadataBlock) -> u64 {
        let maybe_new_slice = match &block.event {
            MetadataEvent::AddData(add_data) => add_data.new_data.as_ref(),
            MetadataEvent::ExecuteTransform(transform) => transform.new_data.as_ref(),
            _ => None,
        };
        if let Some(new_slice) = maybe_new_slice {
            new_slice.num_records()
        } else {
            0
        }
    }

    async fn add_to_ipfs(&self, src: &DatasetRef) -> Result<String, SyncError> {
        let source_url = self.dataset_repo.get_dataset_url(src).await.int_err()?;
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

    #[tracing::instrument(level = "info", name = "sync", skip_all, fields(%src, %dst))]
    async fn sync_impl(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        opts: SyncOptions,
        listener: Arc<dyn SyncListener>,
    ) -> Result<SyncResult, SyncError> {
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
                match src.as_local_ref(|_| !self.dataset_repo.is_multi_tenant()) {
                    Ok(src) => match dst_url.path() {
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
                    },
                    Err(_) => Err(UnsupportedProtocolError {
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
                    .into()),
                }
            }
            (DatasetRefAny::Url(src_url), DatasetRefAny::Url(dst_url))
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
            (DatasetRefAny::Url(src_url), _) if src_url.is_odf_protocol() => {
                self.sync_smart_pull_transfer_protocol(
                    &DatasetRefRemote::Url(src_url.clone()),
                    dst,
                    opts,
                    listener,
                )
                .await
            }
            (_, DatasetRefAny::Url(dst_url)) if dst_url.is_odf_protocol() => {
                self.sync_smart_push_transfer_protocol(
                    src,
                    &DatasetRefRemote::Url(dst_url.clone()),
                    listener,
                )
                .await
            }
            (_, _) => self.sync_generic(src, dst, opts, listener).await,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SyncService for SyncServiceImpl {
    async fn sync(
        &self,
        src: &DatasetRefAny,
        dst: &DatasetRefAny,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        let listener = listener.unwrap_or(Arc::new(NullSyncListener));
        listener.begin();

        match self.sync_impl(src, dst, options, listener.clone()).await {
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
        requests: Vec<SyncRequest>,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<SyncResultMulti> {
        let mut results = Vec::new();

        for SyncRequest { src, dst } in requests {
            let listener = listener.as_ref().and_then(|l| l.begin_sync(&src, &dst));
            let result = self.sync(&src, &dst, options.clone(), listener).await;
            results.push(SyncResultMulti { src, dst, result });
        }

        results
    }

    async fn ipfs_add(&self, src: &DatasetRef) -> Result<String, SyncError> {
        self.add_to_ipfs(src).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

trait UrlExt {
    fn ensure_trailing_slash(&mut self);
    fn is_odf_protocol(&self) -> bool;
}

impl UrlExt for Url {
    fn ensure_trailing_slash(&mut self) {
        if !self.path().ends_with('/') {
            self.set_path(&format!("{}/", self.path()));
        }
    }

    fn is_odf_protocol(&self) -> bool {
        self.scheme().starts_with("odf+")
    }
}
