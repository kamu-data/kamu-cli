// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::prelude::*;
use dill::{component, interface};
use internal_error::ResultIntoInternal;
use kamu_core::*;
use opendatafabric as odf;
use random_names::get_random_name;
use time_source::SystemTimeSource;
use url::Url;

use crate::new_session_context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CompactionExecutionServiceImpl {
    object_store_registry: Arc<dyn ObjectStoreRegistry>,
    time_source: Arc<dyn SystemTimeSource>,
    run_info_dir: Arc<RunInfoDir>,
}

#[component(pub)]
#[interface(dyn CompactionExecutionService)]
impl CompactionExecutionServiceImpl {
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
        data_slice_batches: &mut [CompactionDataSliceBatch],
        offset_column: &str,
        compaction_dir_path: &Path,
    ) -> Result<(), CompactionExecutionError> {
        let ctx = new_session_context(self.object_store_registry.clone());

        for (index, data_slice_batch) in data_slice_batches.iter_mut().enumerate() {
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
                    .sort(vec![col(Column::from_name(offset_column)).sort(true, false)])
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
                data_slice_batch_info.new_file_path = Some(new_file_path);
            }
        }

        Ok(())
    }

    async fn commit_new_blocks(
        &self,
        target: &ResolvedDataset,
        plan: &CompactionPlan,
    ) -> Result<(Vec<Url>, odf::Multihash, usize), CompactionExecutionError> {
        let chain = target.as_metadata_chain();
        let mut current_head = plan.old_seed.clone();
        let mut old_data_slices: Vec<Url> = vec![];
        // set it to 1 to include seed block
        let mut new_num_blocks: usize = 1;

        for data_slice_batch in plan.data_slice_batches.iter().rev() {
            match data_slice_batch {
                CompactionDataSliceBatch::SingleBlock(block_hash) => {
                    let block = chain.get_block(block_hash).await.int_err()?;

                    let commit_result = target
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
                CompactionDataSliceBatch::CompactedBatch(data_slice_batch_info) => {
                    let new_offset_interval = odf::OffsetInterval {
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

                    let commit_result = target
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl CompactionExecutionService for CompactionExecutionServiceImpl {
    async fn execute_compaction(
        &self,
        target: ResolvedDataset,
        mut plan: CompactionPlan,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionExecutionError> {
        // if slices amount +1(seed block) eq to amount of blocks we will not perform
        // compaction
        if plan.data_slice_batches.len() + 1 == plan.old_num_blocks {
            return Ok(CompactionResult::NothingToDo);
        }

        let listener = maybe_listener.unwrap_or(Arc::new(NullCompactionListener {}));

        let compaction_dir_path = self.create_run_compaction_dir()?;

        listener.begin_phase(CompactionPhase::MergeDataslices);
        self.merge_files(
            &mut plan.data_slice_batches,
            plan.offset_column_name.as_str(),
            &compaction_dir_path,
        )
        .await?;

        listener.begin_phase(CompactionPhase::CommitNewBlocks);
        let (_old_data_slices, new_head, new_num_blocks) =
            self.commit_new_blocks(&target, &plan).await?;

        target
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
            old_head: plan.old_seed, // TODO: is this intented?
            new_head,
            old_num_blocks: plan.old_num_blocks,
            new_num_blocks,
        };

        listener.execute_success(&res);

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
