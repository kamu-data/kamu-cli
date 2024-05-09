// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion::prelude::*;
use dill::{component, interface};
use domain::{
    CompactionError,
    CompactionListener,
    CompactionMultiListener,
    CompactionOptions,
    CompactionPhase,
    CompactionResult,
    CompactionService,
    InvalidDatasetKindError,
    NullCompactionListener,
    DEFAULT_MAX_SLICE_RECORDS,
    DEFAULT_MAX_SLICE_SIZE,
};
use futures::stream::TryStreamExt;
use kamu_core::*;
use opendatafabric::{
    Checkpoint,
    DatasetHandle,
    DatasetKind,
    DatasetVocabulary,
    MetadataEvent,
    Multihash,
    OffsetInterval,
    SetVocab,
    SourceState,
};
use random_names::get_random_name;
use url::Url;

use crate::*;

pub struct CompactionServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
    run_info_dir: PathBuf,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
enum DataSliceBatch {
    CompactedBatch(DataSliceBatchInfo),
    // Hash of block will not be None value in case
    // when we will get only one block in batch
    // and will be used tp not rewriting such blocks
    SingleBlock(Multihash),
}

#[derive(Debug, Default, Clone)]
struct DataSliceBatchUpperBound {
    pub new_source_state: Option<SourceState>,
    pub new_watermark: Option<DateTime<Utc>>,
    pub new_checkpoint: Option<Checkpoint>,
    pub end_offset: u64,
}

#[derive(Debug, Default, Clone)]
struct DataSliceBatchLowerBound {
    pub prev_offset: Option<u64>,
    pub prev_checkpoint: Option<Multihash>,
    pub start_offset: u64,
}

#[derive(Debug, Default, Clone)]
struct DataSliceBatchInfo {
    pub data_slices_batch: Vec<Url>,
    pub upper_bound: DataSliceBatchUpperBound,
    pub lower_bound: DataSliceBatchLowerBound,
    pub new_file_path: Option<PathBuf>,
}

struct ChainFilesInfo {
    old_head: Multihash,
    old_num_blocks: usize,
    offset_column: String,
    data_slice_batches: Vec<DataSliceBatch>,
}

#[component(pub)]
#[interface(dyn CompactionService)]
impl CompactionServiceImpl {
    pub fn new(
        dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
        dataset_repo: Arc<dyn DatasetRepository>,
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        time_source: Arc<dyn SystemTimeSource>,
        run_info_dir: PathBuf,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_authorizer,
            object_store_registry,
            time_source,
            run_info_dir,
        }
    }

    async fn gather_chain_info(
        &self,
        dataset: Arc<dyn Dataset>,
        max_slice_size: u64,
        max_slice_records: u64,
    ) -> Result<ChainFilesInfo, CompactionError> {
        // Declare mut values for result

        let mut old_num_blocks: usize = 0;
        let mut old_head: Option<Multihash> = None;
        let mut current_hash: Option<Multihash> = None;
        let mut vocab_event: Option<SetVocab> = None;
        let mut data_slice_batch_info: DataSliceBatchInfo = DataSliceBatchInfo::default();
        let mut data_slice_batches: Vec<DataSliceBatch> = vec![];
        let (mut batch_size, mut batch_records) = (0u64, 0u64);

        ////////////////////////////////////////////////////////////////////////////////

        let chain = dataset.as_metadata_chain();
        let head = chain.resolve_ref(&BlockRef::Head).await?;
        let mut block_stream = chain.iter_blocks_interval(&head, None, false);
        let object_data_repo = dataset.as_data_repo();

        while let Some((block_hash, block)) = block_stream.try_next().await? {
            old_num_blocks += 1;
            match block.event {
                MetadataEvent::AddData(add_data_event) => {
                    if let Some(output_slice) = &add_data_event.new_data {
                        let data_slice_url = object_data_repo
                            .get_internal_url(&output_slice.physical_hash)
                            .await;

                        // Setting the end offset interval needs to be here because we
                        // have to get it at the beginning of iteration unlike
                        // other values which will be set at the end of iteration
                        if data_slice_batch_info.data_slices_batch.is_empty() {
                            data_slice_batch_info.upper_bound.end_offset =
                                output_slice.offset_interval.end;
                        }

                        let current_records = output_slice.num_records();

                        if batch_size + output_slice.size > max_slice_size
                            || batch_records + current_records > max_slice_records
                        {
                            let is_appended =
                                CompactionServiceImpl::append_add_data_batch_to_chain_info(
                                    &mut data_slice_batches,
                                    &current_hash,
                                    &mut data_slice_batch_info,
                                );
                            if is_appended {
                                // Reset values for next batch
                                data_slice_batch_info = DataSliceBatchInfo::default();
                                data_slice_batch_info.upper_bound.end_offset =
                                    output_slice.offset_interval.end;
                            }

                            data_slice_batch_info.data_slices_batch = vec![data_slice_url];
                            batch_size = output_slice.size;
                            batch_records = current_records;
                        } else {
                            data_slice_batch_info.data_slices_batch.push(data_slice_url);
                            batch_size += output_slice.size;
                            batch_records += current_records;
                        }

                        // Set lower bound values
                        data_slice_batch_info.lower_bound.prev_checkpoint =
                            add_data_event.prev_checkpoint;
                        data_slice_batch_info.lower_bound.prev_offset = add_data_event.prev_offset;
                        data_slice_batch_info.lower_bound.start_offset =
                            output_slice.offset_interval.start;
                        current_hash = Some(block_hash);
                    }
                    // Set upper bound values
                    if data_slice_batch_info.upper_bound.new_checkpoint.is_none() {
                        data_slice_batch_info.upper_bound.new_checkpoint =
                            add_data_event.new_checkpoint;
                    }
                    if data_slice_batch_info.upper_bound.new_source_state.is_none() {
                        data_slice_batch_info.upper_bound.new_source_state =
                            add_data_event.new_source_state;
                    }
                    if data_slice_batch_info.upper_bound.new_watermark.is_none() {
                        data_slice_batch_info.upper_bound.new_watermark =
                            add_data_event.new_watermark;
                    }
                }
                MetadataEvent::Seed(_) => old_head = Some(block_hash),
                event => {
                    if let MetadataEvent::SetVocab(set_vocab_event) = event {
                        vocab_event = Some(set_vocab_event);
                    }
                    let is_appended = CompactionServiceImpl::append_add_data_batch_to_chain_info(
                        &mut data_slice_batches,
                        &current_hash,
                        &mut data_slice_batch_info,
                    );
                    data_slice_batches.push(DataSliceBatch::SingleBlock(block_hash.clone()));
                    if is_appended {
                        data_slice_batch_info = DataSliceBatchInfo::default();
                    }
                }
            }
        }

        let vocab: DatasetVocabulary = vocab_event.unwrap_or_default().into();

        Ok(ChainFilesInfo {
            data_slice_batches,
            offset_column: vocab.offset_column,
            old_head: old_head.unwrap(),
            old_num_blocks,
        })
    }

    fn append_add_data_batch_to_chain_info(
        data_slice_batches: &mut Vec<DataSliceBatch>,
        hash: &Option<Multihash>,
        data_slice_batch_info: &mut DataSliceBatchInfo,
    ) -> bool {
        match data_slice_batch_info.data_slices_batch.len().cmp(&1) {
            Ordering::Equal => {
                data_slice_batches
                    .push(DataSliceBatch::SingleBlock(hash.as_ref().unwrap().clone()));
            }
            Ordering::Greater => {
                data_slice_batches.push(DataSliceBatch::CompactedBatch(
                    data_slice_batch_info.clone(),
                ));
            }
            _ => return false,
        }
        true
    }

    async fn merge_files(
        &self,
        data_slice_batches: &mut [DataSliceBatch],
        offset_column: &str,
        compaction_dir_path: &Path,
    ) -> Result<(), CompactionError> {
        let ctx = new_session_context(self.object_store_registry.clone());

        for (index, data_slice_batch) in data_slice_batches.iter_mut().enumerate() {
            if let DataSliceBatch::CompactedBatch(data_slice_batch_info) = data_slice_batch {
                let data_frame = ctx
                    .read_parquet(
                        data_slice_batch_info.data_slices_batch.clone(),
                        datafusion::execution::options::ParquetReadOptions {
                            file_extension: "",
                            ..Default::default()
                        },
                    )
                    .await
                    .int_err()?
                    // TODO: PERF: Consider passing sort order hint to `read_parquet` to let DF now
                    // that the data is already pre-sorted
                    .sort(vec![col(offset_column).sort(true, false)])
                    .int_err()?;

                let new_file_path =
                    compaction_dir_path.join(format!("merge-slice-{index}").as_str());

                data_frame
                    .write_parquet(
                        new_file_path.to_str().unwrap(),
                        datafusion::dataframe::DataFrameWriteOptions::new()
                            .with_single_file_output(true),
                        None,
                    )
                    .await
                    .int_err()?;
                data_slice_batch_info.new_file_path = Some(new_file_path);
            }
        }

        Ok(())
    }

    fn create_run_compaction_dir(&self) -> Result<PathBuf, CompactionError> {
        let compaction_dir_path = self
            .run_info_dir
            .join(get_random_name(Some("compaction-"), 10));
        fs::create_dir_all(&compaction_dir_path).int_err()?;
        Ok(compaction_dir_path)
    }

    async fn commit_new_blocks(
        &self,
        dataset: Arc<dyn Dataset>,
        chain_files_info: &ChainFilesInfo,
    ) -> Result<(Vec<Url>, Multihash, usize), CompactionError> {
        let chain = dataset.as_metadata_chain();
        let mut current_head = chain_files_info.old_head.clone();
        let mut old_data_slices: Vec<Url> = vec![];
        // set it to 1 to include seed block
        let mut new_num_blocks: usize = 1;

        for data_slice_batch in chain_files_info.data_slice_batches.iter().rev() {
            match data_slice_batch {
                DataSliceBatch::SingleBlock(block_hash) => {
                    let block = chain.get_block(block_hash).await.int_err()?;

                    let commit_result = dataset
                        .commit_event(
                            block.event,
                            CommitOpts {
                                block_ref: &BlockRef::Head,
                                system_time: Some(self.time_source.now()),
                                prev_block_hash: Some(Some(&current_head)),
                                check_object_refs: false,
                                update_block_ref: false,
                            },
                        )
                        .await
                        .int_err()?;
                    current_head = commit_result.new_head;
                }
                DataSliceBatch::CompactedBatch(data_slice_batch_info) => {
                    let new_offset_interval = OffsetInterval {
                        start: data_slice_batch_info.lower_bound.start_offset,
                        end: data_slice_batch_info.upper_bound.end_offset,
                    };

                    let add_data_params = AddDataParams {
                        prev_checkpoint: data_slice_batch_info.lower_bound.prev_checkpoint.clone(),
                        prev_offset: data_slice_batch_info.lower_bound.prev_offset,
                        new_offset_interval: Some(new_offset_interval),
                        new_source_state: data_slice_batch_info
                            .upper_bound
                            .new_source_state
                            .clone(),
                        new_watermark: data_slice_batch_info.upper_bound.new_watermark,
                    };
                    let new_checkpoint_ref = data_slice_batch_info
                        .upper_bound
                        .new_checkpoint
                        .clone()
                        .map(|r| CheckpointRef::Existed(r.physical_hash));

                    let commit_result = dataset
                        .commit_add_data(
                            add_data_params,
                            Some(OwnedFile::new(
                                data_slice_batch_info.new_file_path.as_ref().unwrap(),
                            )),
                            new_checkpoint_ref,
                            CommitOpts {
                                block_ref: &BlockRef::Head,
                                system_time: Some(self.time_source.now()),
                                prev_block_hash: Some(Some(&current_head)),
                                check_object_refs: false,
                                update_block_ref: false,
                            },
                        )
                        .await
                        .int_err()?;

                    current_head = commit_result.new_head;
                    old_data_slices.extend(data_slice_batch_info.data_slices_batch.clone());
                }
            }
            new_num_blocks += 1;
        }

        Ok((old_data_slices, current_head, new_num_blocks))
    }

    #[tracing::instrument(level = "info", skip_all)]
    async fn compact_dataset_impl(
        &self,
        dataset: Arc<dyn Dataset>,
        max_slice_size: u64,
        max_slice_records: u64,
        listener: Arc<dyn CompactionListener>,
    ) -> Result<CompactionResult, CompactionError> {
        let compaction_dir_path = self.create_run_compaction_dir()?;

        listener.begin_phase(CompactionPhase::GatherChainInfo);
        let mut chain_files_info = self
            .gather_chain_info(dataset.clone(), max_slice_size, max_slice_records)
            .await?;

        // if slices amount +1(seed block) eq to amount of blocks we will not perform
        // compaction
        if chain_files_info.data_slice_batches.len() + 1 == chain_files_info.old_num_blocks {
            return Ok(CompactionResult::NothingToDo);
        }

        listener.begin_phase(CompactionPhase::MergeDataslices);
        self.merge_files(
            &mut chain_files_info.data_slice_batches,
            chain_files_info.offset_column.as_str(),
            &compaction_dir_path,
        )
        .await?;

        listener.begin_phase(CompactionPhase::CommitNewBlocks);
        let (_old_data_slices, new_head, new_num_blocks) = self
            .commit_new_blocks(dataset.clone(), &chain_files_info)
            .await?;

        dataset
            .as_metadata_chain()
            .set_ref(
                &BlockRef::Head,
                &new_head,
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await?;

        let res = CompactionResult::Success {
            old_head: chain_files_info.old_head,
            new_head,
            old_num_blocks: chain_files_info.old_num_blocks,
            new_num_blocks,
        };

        listener.success(&res);

        Ok(res)
    }
}

#[async_trait::async_trait]
impl CompactionService for CompactionServiceImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        options: CompactionOptions,
        multi_listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Result<CompactionResult, CompactionError> {
        self.dataset_authorizer
            .check_action_allowed(dataset_handle, domain::auth::DatasetAction::Write)
            .await?;

        let dataset = self
            .dataset_repo
            .get_dataset(&dataset_handle.as_local_ref())
            .await?;

        let dataset_kind = dataset
            .get_summary(GetSummaryOpts::default())
            .await
            .int_err()?
            .kind;

        if dataset_kind != DatasetKind::Root {
            return Err(CompactionError::InvalidDatasetKind(
                InvalidDatasetKindError {
                    dataset_name: dataset_handle.alias.dataset_name.clone(),
                },
            ));
        }

        let listener = multi_listener
            .and_then(|l| l.begin_compact(dataset_handle))
            .unwrap_or(Arc::new(NullCompactionListener {}));

        let max_slice_size = options.max_slice_size.unwrap_or(DEFAULT_MAX_SLICE_SIZE);
        let max_slice_records = options
            .max_slice_records
            .unwrap_or(DEFAULT_MAX_SLICE_RECORDS);

        match self
            .compact_dataset_impl(dataset, max_slice_size, max_slice_records, listener.clone())
            .await
        {
            Ok(res) => {
                listener.success(&res);
                Ok(res)
            }
            Err(err) => {
                listener.error(&err);
                Err(err)
            }
        }
    }
}
