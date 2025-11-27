// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use database_common_macros::{transactional_method1, transactional_method2};
use dill::{Catalog, component};
use futures::{Future, StreamExt, TryStreamExt, stream};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::utils::metadata_chain_comparator::*;
use kamu_core::*;
use kamu_datasets::{
    AppendDatasetMetadataBatchUseCase,
    AppendDatasetMetadataBatchUseCaseBlockAppendError,
    AppendDatasetMetadataBatchUseCaseError,
    AppendDatasetMetadataBatchUseCaseOptions,
    CreateDatasetResult,
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetRegistry,
    DatasetRegistryExt,
    SetRefCheckRefMode,
};
use odf::dataset::{MetadataChainImpl, MetadataChainReferenceRepositoryImpl};
use odf::storage::inmem::{NamedObjectRepositoryInMemory, ObjectRepositoryInMemory};
use odf::storage::{MetadataBlockRepositoryImpl, ReferenceRepositoryImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: &str =
    "SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS";
const DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS: usize = 10;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Eq, PartialEq)]
pub struct SimpleProtocolTransferOptions {
    pub max_parallel_transfers: usize,
    pub visibility_for_created_dataset: odf::DatasetVisibility,
}

impl Default for SimpleProtocolTransferOptions {
    fn default() -> Self {
        let max_parallel_transfers =
            match std::env::var(ENV_VAR_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS) {
                Ok(string_value) => string_value
                    .parse::<usize>()
                    .unwrap_or(DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS),
                _ => DEFAULT_SIMPLE_PROTOCOL_MAX_PARALLEL_TRANSFERS,
            };
        Self {
            max_parallel_transfers,
            visibility_for_created_dataset: odf::DatasetVisibility::Private,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implements "Simple Transfer Protocol" as described in ODF spec
pub struct SimpleTransferProtocol {
    catalog: Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl SimpleTransferProtocol {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    pub async fn sync(
        &self,
        src_ref: &odf::DatasetRefAny,
        src: Arc<dyn odf::Dataset>,
        maybe_dst: Option<Arc<dyn odf::Dataset>>,
        dst_alias: Option<&odf::DatasetAlias>,
        validation: odf::dataset::AppendValidation,
        trust_source_hashes: bool,
        force: bool,
        transfer_options: SimpleProtocolTransferOptions,
        listener: Arc<dyn SyncListener + 'static>,
    ) -> Result<SyncResult, SyncError> {
        listener.begin();

        let empty_chain = MetadataChainImpl::new(
            MetadataBlockRepositoryImpl::new(ObjectRepositoryInMemory::new()),
            MetadataChainReferenceRepositoryImpl::new(ReferenceRepositoryImpl::new(
                NamedObjectRepositoryInMemory::new(),
            )),
        );

        let src_chain = src.as_metadata_chain();
        let src_head = self.get_src_head(src_ref, src_chain).await?;

        let (dst_chain, dst_head) = if let Some(dst) = &maybe_dst {
            let dst_chain = dst.as_metadata_chain();
            let dst_head = self.get_dest_head(dst_chain).await?;
            (dst_chain, dst_head)
        } else {
            (&empty_chain as &dyn odf::MetadataChain, None)
        };

        tracing::info!(?src_head, ?dst_head, "Resolved heads");

        let listener_adapter = CompareChainsListenerAdapter::new(listener.clone());

        let chains_comparison = MetadataChainComparator::compare_chains(
            src_chain,
            &src_head,
            dst_chain,
            dst_head.as_ref(),
            &listener_adapter,
        )
        .await?;

        match chains_comparison {
            CompareChainsResult::Equal => return Ok(SyncResult::UpToDate),
            CompareChainsResult::LhsAhead { .. } => { /* Skip */ }
            CompareChainsResult::LhsBehind {
                ref rhs_ahead_blocks,
            } => {
                if !force {
                    return Err(SyncError::DestinationAhead(DestinationAheadError {
                        src_head,
                        dst_head: dst_head.unwrap(),
                        dst_ahead_size: rhs_ahead_blocks.len(),
                    }));
                }
            }
            CompareChainsResult::Divergence {
                uncommon_blocks_in_lhs: uncommon_blocks_in_src,
                uncommon_blocks_in_rhs: uncommon_blocks_in_dst,
            } => {
                if !force {
                    return Err(SyncError::DatasetsDiverged(DatasetsDivergedError {
                        src_head,
                        dst_head: dst_head.unwrap(),
                        detail: Some(DatasetsDivergedErrorDetail {
                            uncommon_blocks_in_src,
                            uncommon_blocks_in_dst,
                        }),
                    }));
                }
            }
        }

        let mut blocks_desc_ordered = match chains_comparison {
            CompareChainsResult::Equal => unreachable!(),
            CompareChainsResult::LhsAhead {
                lhs_ahead_blocks: src_ahead_blocks,
            } => src_ahead_blocks,
            CompareChainsResult::LhsBehind { .. } | CompareChainsResult::Divergence { .. } => {
                // Load all source blocks from head to tail
                assert!(force);
                use odf::dataset::MetadataChainExt;
                src_chain
                    .iter_blocks()
                    .try_collect()
                    .await
                    .map_err(Self::map_block_iteration_error)?
            }
        };

        let old_head = dst_head.clone();
        let num_blocks = blocks_desc_ordered.len();

        // Create dataset if necessary using the source Seed block
        let (dst, dst_head) = if let Some(dst) = maybe_dst {
            (dst, dst_head)
        } else {
            use odf::metadata::AsTypedBlock;

            // Safety: there are blocks
            let (first_hash, first_block) = blocks_desc_ordered.pop().unwrap();
            let seed_block = first_block
                .into_typed()
                .ok_or_else(|| CorruptedSourceError {
                    message: "First metadata block is not Seed".to_owned(),
                    source: None,
                })?;

            let alias =
                dst_alias.ok_or_else(|| "Destination dataset alias is unknown".int_err())?;
            let create_result = self
                .create_dataset_transactional(
                    alias,
                    seed_block,
                    transfer_options.visibility_for_created_dataset,
                )
                .await?;

            assert_eq!(first_hash, create_result.head);
            (create_result.dataset, Some(create_result.head))
        };

        self.synchronize_blocks(
            blocks_desc_ordered,
            src.as_ref(),
            dst.as_ref(),
            dst_alias,
            dst_head,
            validation,
            trust_source_hashes,
            listener,
            transfer_options,
            listener_adapter.into_status(),
        )
        .await?;

        Ok(SyncResult::Updated {
            old_head,
            new_head: src_head,
            num_blocks: num_blocks as u64,
        })
    }

    async fn get_src_head(
        &self,
        src_ref: &odf::DatasetRefAny,
        src_chain: &dyn odf::MetadataChain,
    ) -> Result<odf::Multihash, SyncError> {
        match src_chain.resolve_ref(&odf::BlockRef::Head).await {
            Ok(head) => Ok(head),
            Err(odf::GetRefError::NotFound(_)) => Err(DatasetAnyRefUnresolvedError {
                dataset_ref: src_ref.clone(),
            }
            .into()),
            Err(odf::GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    async fn get_dest_head(
        &self,
        dst_chain: &dyn odf::MetadataChain,
    ) -> Result<Option<odf::Multihash>, SyncError> {
        match dst_chain.resolve_ref(&odf::BlockRef::Head).await {
            Ok(h) => Ok(Some(h)),
            Err(odf::GetRefError::NotFound(_)) => Ok(None),
            Err(odf::GetRefError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::GetRefError::Internal(e)) => Err(SyncError::Internal(e)),
        }
    }

    fn map_block_iteration_error(e: odf::IterBlocksError) -> SyncError {
        const METADATA_CHAIN_BROKEN_MESSAGE: &str = "Source metadata chain is broken";

        match e {
            odf::IterBlocksError::RefNotFound(e) => SyncError::Internal(e.int_err()),
            odf::IterBlocksError::BlockNotFound(e) => CorruptedSourceError {
                message: METADATA_CHAIN_BROKEN_MESSAGE.to_owned(),
                source: Some(e.into()),
            }
            .into(),
            odf::IterBlocksError::BlockVersion(e) => CorruptedSourceError {
                message: METADATA_CHAIN_BROKEN_MESSAGE.to_owned(),
                source: Some(e.into()),
            }
            .into(),
            odf::IterBlocksError::BlockMalformed(e) => CorruptedSourceError {
                message: METADATA_CHAIN_BROKEN_MESSAGE.to_owned(),
                source: Some(e.into()),
            }
            .into(),
            odf::IterBlocksError::InvalidInterval(_) => unreachable!(),
            odf::IterBlocksError::Access(e) => SyncError::Access(e),
            e @ odf::IterBlocksError::Internal(_) => SyncError::Internal(e.int_err()),
        }
    }

    #[transactional_method1(create_dataset_use_case: Arc<dyn CreateDatasetUseCase>)]
    async fn create_dataset_transactional(
        &self,
        dataset_alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
        dataset_visibility: odf::DatasetVisibility,
    ) -> Result<CreateDatasetResult, InternalError> {
        let result = create_dataset_use_case
            .execute(
                dataset_alias,
                seed_block,
                CreateDatasetUseCaseOptions { dataset_visibility },
            )
            .await
            .int_err()?;

        result.dataset.detach_from_transaction();

        Ok(result)
    }

    #[transactional_method2(append_dataset_metadata_batch_use_case: Arc<dyn AppendDatasetMetadataBatchUseCase>, dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn append_metadata_transactional(
        &self,
        dst: &dyn odf::Dataset,
        dst_alias: Option<&odf::DatasetAlias>,
        new_blocks: Box<dyn Iterator<Item = odf::dataset::HashedMetadataBlock> + Send>,
        options: AppendDatasetMetadataBatchUseCaseOptions,
    ) -> Result<Option<odf::Multihash>, AppendDatasetMetadataBatchUseCaseError> {
        if let Some(alias) = dst_alias {
            let res = dataset_registry
                .get_dataset_by_ref(&alias.clone().into())
                .await
                .int_err()?;

            append_dataset_metadata_batch_use_case
                .execute(res.as_ref(), new_blocks, options)
                .await
        } else {
            append_dataset_metadata_batch_use_case
                .execute(dst, new_blocks, options)
                .await
        }
    }

    async fn download_block_data<'a>(
        &'a self,
        src: &'a dyn odf::Dataset,
        dst: &'a dyn odf::Dataset,
        data_slice: &odf::DataSlice,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        arc_stats: Arc<Mutex<SyncStats>>,
    ) -> Result<(), SyncError> {
        tracing::info!(hash = ?data_slice.physical_hash, "Transferring data file");

        let stream = match src
            .as_data_repo()
            .get_stream(&data_slice.physical_hash)
            .await
        {
            Ok(s) => Ok(s),
            Err(odf::storage::GetError::NotFound(e)) => Err(CorruptedSourceError {
                message: "Source data file is missing".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(odf::storage::GetError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::GetError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        match dst
            .as_data_repo()
            .insert_stream(
                stream,
                odf::storage::InsertOpts {
                    precomputed_hash: if !trust_source_hashes {
                        None
                    } else {
                        Some(&data_slice.physical_hash)
                    },
                    expected_hash: Some(&data_slice.physical_hash),
                    size_hint: Some(data_slice.size),
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(odf::storage::InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                message: concat!(
                    "Data file hash declared by the source didn't match ",
                    "the computed - this may be an indication of hashing ",
                    "algorithm mismatch or an attempted tampering",
                )
                .to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(odf::storage::InsertError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::InsertError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let mut stats = arc_stats.lock().unwrap();

        stats.src.data_slices_read += 1;
        stats.dst.data_slices_written += 1;
        stats.src.data_records_read += data_slice.num_records();
        stats.dst.data_records_written += data_slice.num_records();
        stats.src.bytes_read += data_slice.size;
        stats.dst.bytes_written += data_slice.size;

        listener.on_status(SyncStage::TransferData, &stats);

        Ok(())
    }

    async fn download_block_checkpoint<'a>(
        &'a self,
        src: &'a dyn odf::Dataset,
        dst: &'a dyn odf::Dataset,
        checkpoint: &odf::Checkpoint,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        arc_stats: Arc<Mutex<SyncStats>>,
    ) -> Result<(), SyncError> {
        tracing::info!(hash = ?checkpoint.physical_hash, "Transferring checkpoint file");

        let stream = match src
            .as_checkpoint_repo()
            .get_stream(&checkpoint.physical_hash)
            .await
        {
            Ok(s) => Ok(s),
            Err(odf::storage::GetError::NotFound(e)) => Err(CorruptedSourceError {
                message: "Source checkpoint file is missing".to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(odf::storage::GetError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::GetError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        match dst
            .as_checkpoint_repo()
            .insert_stream(
                stream,
                odf::storage::InsertOpts {
                    precomputed_hash: if !trust_source_hashes {
                        None
                    } else {
                        Some(&checkpoint.physical_hash)
                    },
                    expected_hash: Some(&checkpoint.physical_hash),
                    // This hint is necessary only for S3 implementation that does not
                    // currently support streaming uploads
                    // without knowing Content-Length. We should remove it in the future.
                    size_hint: Some(checkpoint.size),
                },
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(odf::storage::InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
                message: concat!(
                    "Checkpoint file hash declared by the source didn't ",
                    "match the computed - this may be an indication of hashing ",
                    "algorithm mismatch or an attempted tampering",
                )
                .to_owned(),
                source: Some(e.into()),
            }
            .into()),
            Err(odf::storage::InsertError::Access(e)) => Err(SyncError::Access(e)),
            Err(odf::storage::InsertError::Internal(e)) => Err(SyncError::Internal(e)),
        }?;

        let mut stats = arc_stats.lock().unwrap();

        stats.src.checkpoints_read += 1;
        stats.dst.checkpoints_written += 1;
        stats.src.bytes_read += checkpoint.size;
        stats.dst.bytes_written += checkpoint.size;

        listener.on_status(SyncStage::TransferData, &stats);

        Ok(())
    }

    async fn synchronize_blocks<'a>(
        &'a self,
        blocks_desc_ordered: Vec<odf::dataset::HashedMetadataBlock>,
        src: &'a dyn odf::Dataset,
        dst: &'a dyn odf::Dataset,
        dst_alias: Option<&odf::DatasetAlias>,
        dst_head: Option<odf::Multihash>,
        validation: odf::dataset::AppendValidation,
        trust_source_hashes: bool,
        listener: Arc<dyn SyncListener>,
        transfer_options: SimpleProtocolTransferOptions,
        mut stats: SyncStats,
    ) -> Result<(), SyncError> {
        // Update stats estimates based on metadata
        stats.dst_estimated.metadata_blocks_written += blocks_desc_ordered.len() as u64;

        use odf::metadata::IntoDataStreamBlock;
        for block in blocks_desc_ordered
            .iter()
            .filter_map(|(_, b)| b.as_data_stream_block())
        {
            if let Some(data_slice) = block.event.new_data {
                stats.src_estimated.data_slices_read += 1;
                stats.src_estimated.bytes_read += data_slice.size;
                stats.src_estimated.data_records_read += data_slice.num_records();

                stats.dst_estimated.data_slices_written += 1;
                stats.dst_estimated.bytes_written += data_slice.size;
                stats.dst_estimated.data_records_written += data_slice.num_records();
            }
            if let Some(checkpoint) = block.event.new_checkpoint {
                stats.src_estimated.checkpoints_read += 1;
                stats.src_estimated.bytes_read += checkpoint.size;

                stats.dst_estimated.checkpoints_written += 1;
                stats.dst_estimated.bytes_written += checkpoint.size;
            }
        }

        tracing::info!(
            ?stats,
            "Considering {} new blocks",
            blocks_desc_ordered.len()
        );
        listener.on_status(SyncStage::TransferData, &stats);

        // Download data and checkpoints
        let arc_stats = Arc::new(Mutex::new(stats));
        let mut block_download_tasks = vec![];
        blocks_desc_ordered.iter().rev().for_each(|(_, b)| {
            if let Some(block_stream) = b.as_data_stream_block() {
                if let Some(data_slice) = block_stream.event.new_data {
                    // Each function return unique future
                    // cast future to next type to allow storing them in vector
                    block_download_tasks.push(Box::pin(self.download_block_data(
                        src,
                        dst,
                        data_slice,
                        trust_source_hashes,
                        listener.clone(),
                        arc_stats.clone(),
                    ))
                        as Pin<Box<dyn Future<Output = Result<(), SyncError>> + Send>>);
                }
                if let Some(checkpoint) = block_stream.event.new_checkpoint {
                    block_download_tasks.push(Box::pin(self.download_block_checkpoint(
                        src,
                        dst,
                        checkpoint,
                        trust_source_hashes,
                        listener.clone(),
                        arc_stats.clone(),
                    )));
                }
            }
        });

        stream::iter(block_download_tasks)
            .map(Ok)
            .try_for_each_concurrent(transfer_options.max_parallel_transfers, |future| future)
            .await?;

        let on_append_metadata_block_callback = {
            let boxed_closure: Box<dyn Fn() + Send> = Box::new(move || {
                let mut stats = arc_stats.lock().unwrap();
                stats.dst.metadata_blocks_written += 1;
                listener.on_status(SyncStage::CommitBlocks, &stats);
            });
            Some(boxed_closure)
        };

        self.append_metadata_transactional(
            dst,
            dst_alias,
            Box::new(blocks_desc_ordered.into_iter().rev()),
            AppendDatasetMetadataBatchUseCaseOptions {
                set_ref_check_ref_mode: Some(SetRefCheckRefMode::Explicit(dst_head)),
                trust_source_hashes: Some(trust_source_hashes),
                append_validation: Some(validation),
                on_append_metadata_block_callback,
            },
        )
        .await
        .map_err(|use_case_error| {
            use odf::dataset::{AppendError, SetChainRefError};

            match use_case_error {
                AppendDatasetMetadataBatchUseCaseError::BlockAppendError(
                    AppendDatasetMetadataBatchUseCaseBlockAppendError {
                        append_error,
                        block_sequence_number,
                        block_hash,
                    },
                ) => match append_error {
                    AppendError::InvalidBlock(append_validation_error) => {
                        let message = match &append_validation_error {
                            odf::dataset::AppendValidationError::HashMismatch(e) => {
                                format!(
                                    "Block hash declared by the source {} didn't match the \
                                     computed {} at block {} - this may be an indication of \
                                     hashing algorithm mismatch or an attempt to tamper data",
                                    e.actual, e.expected, block_sequence_number
                                )
                            }
                            _ => format!(
                                "Source metadata chain is logically inconsistent at block \
                                 {block_hash}[{block_sequence_number}]"
                            ),
                        };

                        CorruptedSourceError {
                            message,
                            source: Some(append_validation_error.into()),
                        }
                        .into()
                    }
                    AppendError::RefNotFound(_) | AppendError::RefCASFailed(_) => {
                        unreachable!()
                    }
                    AppendError::Access(e) => SyncError::Access(e),
                    e @ AppendError::Internal(_) => SyncError::Internal(e.int_err()),
                },
                AppendDatasetMetadataBatchUseCaseError::SetRefError(set_ref_error) => {
                    match set_ref_error {
                        SetChainRefError::CASFailed(e) => SyncError::UpdatedConcurrently(e.into()),
                        SetChainRefError::Access(e) => SyncError::Access(e),
                        e
                        @ (SetChainRefError::Internal(_) | SetChainRefError::BlockNotFound(_)) => {
                            SyncError::Internal(e.int_err())
                        }
                    }
                }
                e @ AppendDatasetMetadataBatchUseCaseError::Internal(_) => {
                    SyncError::Internal(e.int_err())
                }
            }
        })?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct CompareChainsListenerAdapter {
    l: Arc<dyn SyncListener>,
    stats: Mutex<SyncStats>,
}

impl CompareChainsListenerAdapter {
    fn new(l: Arc<dyn SyncListener>) -> Self {
        Self {
            l,
            stats: Mutex::new(SyncStats::default()),
        }
    }

    fn into_status(self) -> SyncStats {
        self.stats.into_inner().unwrap()
    }
}

impl CompareChainsListener for CompareChainsListenerAdapter {
    fn on_lhs_expected_reads(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.src_estimated.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_lhs_read(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.src.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_rhs_expected_reads(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.dst_estimated.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }

    fn on_rhs_read(&self, num_blocks: u64) {
        let mut s = self.stats.lock().unwrap();
        s.dst.metadata_blocks_read += num_blocks;
        self.l.on_status(SyncStage::ReadMetadata, &s);
    }
}
