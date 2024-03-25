// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::Utc;
use datafusion::prelude::*;
use dill::{component, interface};
use domain::compact_service::{
    CompactError,
    CompactionMultiListener,
    CompactionPhase,
    InvalidDatasetKindError,
    NullCompactionListener,
};
use futures::stream::TryStreamExt;
use kamu_core::compact_service::CompactService;
use kamu_core::*;
use opendatafabric::{
    AddData,
    DatasetHandle,
    DatasetKind,
    DatasetName,
    MetadataEvent,
    Multihash,
    OffsetInterval,
};
use url::Url;

use crate::utils::random_names_helper::get_random_operation_name_with_prefix;
use crate::*;

pub struct CompactServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
    run_info_dir: PathBuf,
}

#[derive(Debug, Default, Clone)]
struct DataSliceBatchInfo {
    pub data_slices_batch: Vec<Url>,
    pub prev_offset: Option<u64>,
    pub new_offset_interval: Option<OffsetInterval>,
    pub new_file_path: Option<PathBuf>,
    // Hash of block will not be None value in case
    // when data_slices_batch.len() == 1
    // and will be used tp not rewriting such blocks
    pub hash: Option<Multihash>,
}

struct ChainFilesInfo {
    _block_file_urls: Vec<Url>,
    old_head: Multihash,
    data_slice_batches: Vec<DataSliceBatchInfo>,
}

#[component(pub)]
#[interface(dyn CompactService)]
impl CompactServiceImpl {
    pub fn new(
        dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
        dataset_repo: Arc<dyn DatasetRepository>,
        run_info_dir: PathBuf,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_authorizer,
            run_info_dir,
        }
    }

    async fn gather_chain_info(
        &self,
        dataset: Arc<dyn Dataset>,
        max_slice_size: u64,
        max_slice_records: u64,
    ) -> Result<ChainFilesInfo, CompactError> {
        // Declare mut values for result

        let mut old_head: Option<Multihash> = None;
        let mut block_file_urls: Vec<Url> = vec![];
        let mut data_slice_batch_info: DataSliceBatchInfo = DataSliceBatchInfo::default();
        let mut data_slice_batches: Vec<DataSliceBatchInfo> = vec![];
        let (mut batch_size, mut new_end_offset_interval, mut batch_records): (u64, u64, u64) =
            (0, 0, 0);

        ////////////////////////////////////////////////////////////////////////////////

        let chain = dataset.as_metadata_chain();
        let head = chain.resolve_ref(&BlockRef::Head).await?;
        let mut block_stream = chain.iter_blocks_interval(&head, None, false);
        let object_data_repo = dataset.as_data_repo();
        let object_block_repo = chain.as_metadata_block_repository();

        while let Some((block_hash, block)) = block_stream.try_next().await? {
            block_file_urls.push(object_block_repo.get_internal_url(&block_hash).await);

            match block.event {
                MetadataEvent::AddData(add_data_event) => {
                    if let Some(output_slice) = &add_data_event.new_data {
                        old_head = block.prev_block_hash.clone();

                        let data_slice_url = object_data_repo
                            .get_internal_url(&output_slice.physical_hash)
                            .await;
                        if data_slice_batch_info.data_slices_batch.is_empty() {
                            new_end_offset_interval = output_slice.offset_interval.end;
                        }

                        let current_records = output_slice.num_records();

                        if batch_size + output_slice.size >= max_slice_size
                            || batch_records + current_records >= max_slice_records
                        {
                            if !data_slice_batch_info.data_slices_batch.is_empty() {
                                data_slice_batches.push(data_slice_batch_info.clone());
                                // Reset values for next batch
                                data_slice_batch_info.hash = None;
                                data_slice_batch_info.new_offset_interval = None;
                                new_end_offset_interval = output_slice.offset_interval.end;
                            }

                            data_slice_batch_info.data_slices_batch = vec![data_slice_url];
                            batch_size = output_slice.size;
                            batch_records = current_records;
                        } else {
                            data_slice_batch_info.data_slices_batch.push(data_slice_url);
                            batch_size += output_slice.size;
                            batch_records += current_records;
                            if data_slice_batch_info.data_slices_batch.len() > 1 {
                                data_slice_batch_info.hash = None;
                            }
                        }

                        if data_slice_batch_info.hash.is_none()
                            && data_slice_batch_info.data_slices_batch.len() <= 1
                        {
                            data_slice_batch_info.hash = Some(block_hash.clone());
                        }
                        data_slice_batch_info.prev_offset = add_data_event.prev_offset;
                        data_slice_batch_info.new_offset_interval = Some(OffsetInterval {
                            start: output_slice.offset_interval.start,
                            end: new_end_offset_interval,
                        });
                    }
                }
                _ => continue,
            }
        }

        if !data_slice_batch_info.data_slices_batch.is_empty() {
            data_slice_batches.push(data_slice_batch_info);
        }

        Ok(ChainFilesInfo {
            data_slice_batches,
            _block_file_urls: block_file_urls,
            old_head: old_head.unwrap(),
        })
    }

    async fn merge_files(
        &self,
        data_slice_batches: &mut Vec<DataSliceBatchInfo>,
        dataset_name: &DatasetName,
        compact_dir_path: &Path,
    ) -> Result<(), CompactError> {
        let ctx = SessionContext::new();

        for data_slice_batch_info in data_slice_batches {
            if data_slice_batch_info.data_slices_batch.len() == 1 {
                continue;
            }
            let data_frame = ctx
                .read_parquet(
                    data_slice_batch_info.data_slices_batch.clone(),
                    datafusion::execution::options::ParquetReadOptions {
                        file_extension: "",
                        ..Default::default()
                    },
                )
                .await
                .int_err()?;

            data_slice_batch_info.new_file_path = Some(compact_dir_path.join(
                get_random_operation_name_with_prefix(format!("{dataset_name}-").as_str()),
            ));

            data_frame
                .write_parquet(
                    data_slice_batch_info
                        .new_file_path
                        .as_ref()
                        .unwrap()
                        .to_str()
                        .unwrap(),
                    datafusion::dataframe::DataFrameWriteOptions::new()
                        .with_single_file_output(true),
                    None,
                )
                .await
                .int_err()?;
        }

        Ok(())
    }

    fn create_run_compact_dir(&self) -> Result<PathBuf, CompactError> {
        let compact_dir_path = self.run_info_dir.join("compact");
        fs::create_dir_all(&compact_dir_path).int_err()?;
        Ok(compact_dir_path)
    }

    async fn commit_new_blocks(
        &self,
        dataset: Arc<dyn Dataset>,
        chain_files_info: &ChainFilesInfo,
    ) -> Result<(Vec<Url>, Multihash), CompactError> {
        let chain = dataset.as_metadata_chain();

        let mut current_head = chain_files_info.old_head.clone();
        let mut old_data_slices: Vec<Url> = vec![];

        for data_slice_batch_info in chain_files_info.data_slice_batches.iter().rev() {
            if let Some(block_hash) = data_slice_batch_info.hash.as_ref() {
                let block = chain.get_block(block_hash).await.int_err()?;

                if let MetadataEvent::AddData(add_data_event) = block.event {
                    let block_event = MetadataEvent::AddData(AddData {
                        prev_checkpoint: None,
                        prev_offset: add_data_event.prev_offset,
                        new_checkpoint: None,
                        new_data: add_data_event.new_data,
                        new_source_state: None,
                        new_watermark: None,
                    });
                    let commit_result = dataset
                        .commit_event(
                            block_event,
                            CommitOpts {
                                block_ref: &BlockRef::Head,
                                system_time: Some(Utc::now()),
                                prev_block_hash: Some(Some(&current_head)),
                                check_object_refs: false,
                                update_head: false,
                            },
                        )
                        .await
                        .int_err()?;
                    current_head = commit_result.new_head;
                    continue;
                }
            }

            let add_data_params = AddDataParams {
                prev_checkpoint: None,
                prev_offset: data_slice_batch_info.prev_offset,
                new_offset_interval: data_slice_batch_info.new_offset_interval.clone(),
                new_watermark: None,
                new_source_state: None,
            };

            let commit_result = dataset
                .commit_add_data(
                    add_data_params,
                    Some(OwnedFile::new(
                        data_slice_batch_info.new_file_path.as_ref().unwrap(),
                    )),
                    None,
                    CommitOpts {
                        block_ref: &BlockRef::Head,
                        system_time: Some(Utc::now()),
                        prev_block_hash: Some(Some(&current_head)),
                        check_object_refs: false,
                        update_head: false,
                    },
                )
                .await
                .int_err()?;
            current_head = commit_result.new_head;

            old_data_slices.extend(data_slice_batch_info.data_slices_batch.clone());
        }

        Ok((old_data_slices, current_head))
    }

    // fn _remove_old_files(
    //     &self,
    //     chain_files_info: &ChainFilesInfo,
    //     old_data_slices: &Vec<Url>,
    // ) -> Result<(), CompactError> {
    //     for file_url in old_data_slices {
    //         fs::remove_file(file_url.to_file_path().unwrap()).int_err()?;
    //     }

    //     let old_head_string =
    // chain_files_info.old_head.as_multibase().to_string();
    //     for block_file_url in &chain_files_info._block_file_urls {
    //         // Stop cleaning up once we reach block where new chain started
    //         if block_file_url.as_str().contains(&old_head_string) {
    //             break;
    //         }
    //         fs::remove_file(block_file_url.to_file_path().unwrap()).int_err()?;
    //     }
    //     Ok(())
    // }
}

#[async_trait::async_trait]
impl CompactService for CompactServiceImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        max_slice_size: u64,
        max_slice_records: u64,
        multi_listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Result<(), CompactError> {
        let listener = multi_listener
            .and_then(|l| l.begin_compact(dataset_handle))
            .unwrap_or(Arc::new(NullCompactionListener {}));
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
            return Err(CompactError::InvalidDatasetKind(InvalidDatasetKindError {
                dataset_name: dataset_handle.alias.dataset_name.clone(),
            }));
        }

        let compact_dir_path = self.create_run_compact_dir()?;

        listener.begin_phase(CompactionPhase::GatherChainInfo);
        let mut chain_files_info = self
            .gather_chain_info(dataset.clone(), max_slice_size, max_slice_records)
            .await?;

        listener.begin_phase(CompactionPhase::MergeDataslices);
        self.merge_files(
            &mut chain_files_info.data_slice_batches,
            &dataset_handle.alias.dataset_name,
            &compact_dir_path,
        )
        .await?;

        listener.begin_phase(CompactionPhase::CommitNewBlocks);
        let (_old_data_slices, new_head) = self
            .commit_new_blocks(dataset.clone(), &chain_files_info)
            .await?;

        listener.begin_phase(CompactionPhase::CleanOldFiles);
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
        // self.remove_old_files(&chain_files_info, &old_data_slices)?;
        listener.success();

        Ok(())
    }
}
