// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::prelude::*;
use dill::{component, interface};
use file_utils::OwnedFile;
use internal_error::ResultIntoInternal;
use kamu_core::*;
use random_names::get_random_name;
use time_source::SystemTimeSource;
use url::Url;

use crate::new_session_context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CompactionExecutorImpl {
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
    run_info_dir: Arc<RunInfoDir>,
}

#[component(pub)]
#[interface(dyn CompactionExecutor)]
impl CompactionExecutorImpl {
    pub fn new(
        object_store_registry: Arc<dyn ObjectStoreRegistry>,
        time_source: Arc<dyn SystemTimeSource>,
        run_info_dir: Arc<RunInfoDir>,
    ) -> Self {
        Self {
            object_store_registry,
            time_source,
            run_info_dir,
        }
    }

    fn create_run_compaction_dir(&self) -> Result<PathBuf, CompactionExecutionError> {
        let compaction_dir_path = self
            .run_info_dir
            .join(get_random_name(Some("compaction-"), 10));
        std::fs::create_dir_all(&compaction_dir_path).int_err()?;
        Ok(compaction_dir_path)
    }

    async fn merge_files(
        &self,
        plan: &CompactionPlan,
        compaction_dir_path: &Path,
    ) -> Result<HashMap<usize, PathBuf>, CompactionExecutionError> {
        let ctx = new_session_context(self.object_store_registry.clone());

        let mut new_file_paths = HashMap::new();

        for (index, data_slice_batch) in plan.data_slice_batches.iter().enumerate() {
            if let CompactionDataSliceBatch::CompactedBatch(data_slice_batch_info) =
                data_slice_batch
            {
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
                    .sort(vec![
                        col(Column::from_name(&plan.offset_column_name)).sort(true, false)
                    ])
                    .int_err()?;

                // FIXME: The .parquet extension is currently necessary for DataFusion to
                // respect the single-file output
                // See: https://github.com/apache/datafusion/issues/13323
                let new_file_path =
                    compaction_dir_path.join(format!("merge-slice-{index}.parquet").as_str());

                data_frame
                    .write_parquet(
                        new_file_path.to_str().unwrap(),
                        datafusion::dataframe::DataFrameWriteOptions::new()
                            .with_single_file_output(true),
                        None,
                    )
                    .await
                    .int_err()?;
                new_file_paths.insert(index, new_file_path);
            }
        }

        Ok(new_file_paths)
    }

    async fn commit_new_blocks(
        &self,
        target: &ResolvedDataset,
        plan: &CompactionPlan,
        new_file_paths: HashMap<usize, PathBuf>,
    ) -> Result<(Vec<Url>, odf::Multihash, usize), CompactionExecutionError> {
        let chain = target.as_metadata_chain();
        let mut current_head = plan.seed.clone();
        let mut old_data_slices: Vec<Url> = vec![];
        // set it to 1 to include seed block
        let mut new_num_blocks: usize = 1;

        for (index, data_slice_batch) in plan.data_slice_batches.iter().enumerate().rev() {
            match data_slice_batch {
                CompactionDataSliceBatch::SingleBlock(block_hash) => {
                    let block = chain.get_block(block_hash).await.int_err()?;

                    let commit_result = target
                        .commit_event(
                            block.event,
                            odf::dataset::CommitOpts {
                                block_ref: &odf::BlockRef::Head,
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
                CompactionDataSliceBatch::CompactedBatch(data_slice_batch_info) => {
                    let new_offset_interval = odf::metadata::OffsetInterval {
                        start: data_slice_batch_info.lower_bound.start_offset,
                        end: data_slice_batch_info.upper_bound.end_offset,
                    };

                    let add_data_params = odf::dataset::AddDataParams {
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
                        .map(|r| odf::dataset::CheckpointRef::Existed(r.physical_hash));

                    let commit_result = target
                        .commit_add_data(
                            add_data_params,
                            Some(OwnedFile::new(new_file_paths.get(&index).expect(
                                "File path for the compacted chunk should be defined",
                            ))),
                            new_checkpoint_ref,
                            odf::dataset::CommitOpts {
                                block_ref: &odf::BlockRef::Head,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl CompactionExecutor for CompactionExecutorImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(target=%target.get_handle()))]
    async fn execute(
        &self,
        target: ResolvedDataset,
        plan: CompactionPlan,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionExecutionError> {
        // if slices amount +1(seed block) eq to amount of blocks we will not perform
        // compaction
        if plan.data_slice_batches.len() + 1 == plan.old_num_blocks {
            return Ok(CompactionResult::NothingToDo);
        }

        let listener = maybe_listener.unwrap_or(Arc::new(NullCompactionListener {}));

        let compaction_dir_path = self.create_run_compaction_dir()?;

        tracing::debug!("Merging data slices");
        listener.begin_phase(CompactionPhase::MergeDataslices);
        let new_file_paths = self.merge_files(&plan, &compaction_dir_path).await?;

        tracing::debug!("Committing new compacted blocks");
        listener.begin_phase(CompactionPhase::CommitNewBlocks);
        let (_old_data_slices, new_head, new_num_blocks) = self
            .commit_new_blocks(&target, &plan, new_file_paths)
            .await?;

        let res = CompactionResult::Success {
            old_head: plan.old_head,
            new_head,
            old_num_blocks: plan.old_num_blocks,
            new_num_blocks,
        };

        listener.execute_success(&res);

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
