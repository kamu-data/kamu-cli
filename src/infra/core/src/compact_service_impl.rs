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
    DatasetHandle,
    DatasetKind,
    DatasetName,
    MetadataEvent,
    Multihash,
    OffsetInterval,
};
use url::Url;

use crate::*;

pub struct CompactServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
}

#[derive(Debug, Clone)]
struct DataSliceBatchInfo {
    pub data_slices_batch: Vec<Url>,
    pub prev_offset: Option<u64>,
    pub new_offset_interval: Option<OffsetInterval>,
    pub new_file_path: Option<PathBuf>,
}

struct ChainFilesInfo {
    block_file_urls: Vec<Url>,
    block_hash_to_reset: Multihash,
    data_slice_batches: Vec<DataSliceBatchInfo>,
}

#[component(pub)]
#[interface(dyn CompactService)]
impl CompactServiceImpl {
    pub fn new(
        dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
        dataset_repo: Arc<dyn DatasetRepository>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_authorizer,
        }
    }

    async fn get_chain_files_info(
        &self,
        dataset: Arc<dyn Dataset>,
        dataset_name: &DatasetName,
        compact_dir_path: &str,
        max_slice_size: u64,
    ) -> Result<ChainFilesInfo, CompactError> {
        // Declare mut values for result

        let mut block_hash_to_reset: Option<Multihash> = None;
        let mut block_file_urls: Vec<Url> = vec![];
        let mut data_slice_batch_info: DataSliceBatchInfo = DataSliceBatchInfo {
            prev_offset: None,
            new_offset_interval: None,
            data_slices_batch: vec![],
            new_file_path: None,
        };
        let mut data_slice_batches: Vec<DataSliceBatchInfo> = vec![];
        let (mut batch_size, mut new_end_offset_interval): (u64, u64) = (0, 0);

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
                        block_hash_to_reset = block.prev_block_hash.clone();

                        let data_slice_url = object_data_repo
                            .get_internal_url(&output_slice.physical_hash)
                            .await;
                        if data_slice_batch_info.data_slices_batch.is_empty() {
                            new_end_offset_interval = output_slice.offset_interval.end;
                        }

                        if batch_size + output_slice.size > max_slice_size {
                            if !data_slice_batch_info.data_slices_batch.is_empty() {
                                data_slice_batches.push(data_slice_batch_info.clone());
                                data_slice_batch_info.new_file_path = None;
                                data_slice_batch_info.new_offset_interval = None;
                                new_end_offset_interval = output_slice.offset_interval.end;
                            }

                            data_slice_batch_info.data_slices_batch = vec![data_slice_url];
                            batch_size = output_slice.size;
                        } else {
                            data_slice_batch_info.data_slices_batch.push(data_slice_url);
                            batch_size += output_slice.size;
                        }

                        if data_slice_batch_info.new_file_path.is_none() {
                            data_slice_batch_info.new_file_path =
                                Some(Path::new(compact_dir_path).join(
                                    Self::get_random_operation_name_with_prefix(
                                        format!("{dataset_name}-").as_str(),
                                    ),
                                ));
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
            block_file_urls,
            block_hash_to_reset: block_hash_to_reset.unwrap(),
        })
    }

    async fn write_file(
        &self,
        data_slice_batches: &Vec<DataSliceBatchInfo>,
    ) -> Result<(), CompactError> {
        let ctx = SessionContext::new();

        for data_slice_batch_info in data_slice_batches {
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

    fn create_compact_dir(&self, dataset_dir_path: &Path) -> Result<String, CompactError> {
        let compact_dir_path = format!("{}-compact", dataset_dir_path.display());
        fs::create_dir_all(&compact_dir_path).int_err()?;
        Ok(compact_dir_path)
    }

    async fn replace_data_files(
        &self,
        dataset: Arc<dyn Dataset>,
        chain_files_info: &ChainFilesInfo,
    ) -> Result<Vec<Url>, CompactError> {
        let chain = dataset.as_metadata_chain();
        chain
            .set_ref(
                &BlockRef::Head,
                &chain_files_info.block_hash_to_reset,
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await?;

        let mut new_head = chain_files_info.block_hash_to_reset.clone();
        let mut old_data_slices: Vec<Url> = vec![];

        for data_slice_batch_info in chain_files_info.data_slice_batches.iter().rev() {
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
                        prev_block_hash: Some(Some(&new_head)),
                        check_object_refs: false,
                    },
                )
                .await?;
            new_head = commit_result.new_head;
            if data_slice_batch_info.data_slices_batch.len() == 1 {
                let new_block = chain.get_block(&new_head).await.int_err()?;
                if let MetadataEvent::AddData(add_event) = new_block.event {
                    if let Some(data_slice) = add_event.new_data {
                        if data_slice_batch_info
                            .data_slices_batch
                            .first()
                            .unwrap()
                            .to_string()
                            .contains(&data_slice.physical_hash.as_multibase().to_string())
                        {
                            continue;
                        }
                    }
                }
            }
            old_data_slices.extend(data_slice_batch_info.data_slices_batch.clone());
        }

        Ok(old_data_slices)
    }

    fn remove_old_files(
        &self,
        chain_files_info: &ChainFilesInfo,
        old_data_slices: &Vec<Url>,
    ) -> Result<(), CompactError> {
        for file_url in old_data_slices {
            fs::remove_file(file_url.to_file_path().unwrap()).int_err()?;
        }

        let block_hash_to_reset_string = chain_files_info
            .block_hash_to_reset
            .as_multibase()
            .to_string();
        for block_file_url in &chain_files_info.block_file_urls {
            if block_file_url
                .as_str()
                .contains(&block_hash_to_reset_string)
            {
                break;
            }
            fs::remove_file(block_file_url.to_file_path().unwrap()).int_err()?;
        }
        Ok(())
    }

    fn get_random_operation_name_with_prefix(prefix: &str) -> String {
        use rand::distributions::Alphanumeric;
        use rand::Rng;

        let mut name = String::with_capacity(10 + prefix.len());
        name.push_str(prefix);
        name.extend(
            rand::thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from),
        );
        name
    }
}

#[async_trait::async_trait]
impl CompactService for CompactServiceImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        dataset_dir_path: &Path,
        max_slice_size: u64,
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

        let compact_dir_path = self.create_compact_dir(dataset_dir_path)?;

        listener.begin_phase(CompactionPhase::GatherChainInfo);
        let chain_files_info = self
            .get_chain_files_info(
                dataset.clone(),
                &dataset_handle.alias.dataset_name,
                &compact_dir_path,
                max_slice_size,
            )
            .await?;

        listener.begin_phase(CompactionPhase::MergeDataslices);
        self.write_file(&chain_files_info.data_slice_batches)
            .await?;

        listener.begin_phase(CompactionPhase::ReplaceChainHead);
        let old_data_slices = self
            .replace_data_files(dataset.clone(), &chain_files_info)
            .await?;

        listener.begin_phase(CompactionPhase::CleanOldFiles);
        self.remove_old_files(&chain_files_info, &old_data_slices)?;
        listener.success();

        Ok(())
    }
}
