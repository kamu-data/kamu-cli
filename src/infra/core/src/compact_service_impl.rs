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
    CompactionListener,
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

use crate::*;

pub struct CompactServiceImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_authorizer: Arc<dyn domain::auth::DatasetActionAuthorizer>,
}

struct LastDatasliceInfo {
    prev_offset: Option<u64>,
    new_offset_interval: Option<OffsetInterval>,
}

struct ChainFilesInfo {
    data_slice_file_urls: Vec<String>,
    block_file_urls: Vec<String>,
    block_hash_to_reset: Multihash,
    last_dataslice_info: LastDatasliceInfo,
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
    ) -> Result<ChainFilesInfo, CompactError> {
        let chain = dataset.as_metadata_chain();
        let head = chain.resolve_ref(&BlockRef::Head).await?;
        let mut block_hash_to_reset: Option<Multihash> = None;
        let mut data_slice_file_urls: Vec<String> = vec![];
        let mut block_file_urls: Vec<String> = vec![];

        let mut block_stream = chain.iter_blocks_interval(&head, None, false);
        let object_data_repo = dataset.as_data_repo();
        let object_block_repo = chain.as_metadata_block_repository();
        let mut last_dataslice_info: Option<LastDatasliceInfo> = None;

        while let Some((block_hash, block)) = block_stream.try_next().await? {
            block_file_urls.push(object_block_repo.get_internal_url(&block_hash).await.into());

            match block.event {
                MetadataEvent::AddData(add_data_event) => {
                    if let Some(output_slice) = &add_data_event.new_data {
                        if last_dataslice_info.is_none() {
                            last_dataslice_info = Some(LastDatasliceInfo {
                                prev_offset: add_data_event.prev_offset,
                                new_offset_interval: Some(output_slice.offset_interval.clone()),
                            });
                        }
                        block_hash_to_reset = block.prev_block_hash.clone();

                        data_slice_file_urls.push(
                            object_data_repo
                                .get_internal_url(&output_slice.physical_hash)
                                .await
                                .into(),
                        );
                    }
                }
                _ => continue,
            }
        }

        Ok(ChainFilesInfo {
            data_slice_file_urls,
            block_file_urls,
            block_hash_to_reset: block_hash_to_reset.unwrap(),
            last_dataslice_info: last_dataslice_info.unwrap(),
        })
    }

    async fn write_file(
        &self,
        data_slice_file_urls: Vec<String>,
        dataset_name: &DatasetName,
        compact_dir_path: &str,
        listener: Arc<dyn CompactionListener>,
    ) -> Result<PathBuf, CompactError> {
        let ctx = SessionContext::new();
        listener.begin_phase(CompactionPhase::ReadDatasliceFiles);
        let data_frame = ctx
            .read_parquet(
                data_slice_file_urls,
                datafusion::execution::options::ParquetReadOptions {
                    file_extension: "",
                    ..Default::default()
                },
            )
            .await
            .int_err()?;

        let file_path = Path::new(compact_dir_path).join(dataset_name);

        listener.begin_phase(CompactionPhase::WriteDataasliceFiles);
        data_frame
            .write_parquet(
                file_path.to_str().unwrap(),
                datafusion::dataframe::DataFrameWriteOptions::new().with_single_file_output(true),
                None,
            )
            .await
            .int_err()?;

        Ok(file_path)
    }

    fn create_compact_dir(&self, dataset_dir_path: &Path) -> Result<String, CompactError> {
        let compact_dir_path = format!("{}-compact", dataset_dir_path.display());
        fs::create_dir_all(&compact_dir_path).int_err()?;
        Ok(compact_dir_path)
    }

    async fn replace_data_files(
        &self,
        dataset: Arc<dyn Dataset>,
        new_file_path: &Path,
        block_hash_to_reset: &Multihash,
        last_dataslice_info: &LastDatasliceInfo,
    ) -> Result<(), CompactError> {
        let chain = dataset.as_metadata_chain();
        chain
            .set_ref(
                &BlockRef::Head,
                block_hash_to_reset,
                SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: None,
                },
            )
            .await?;

        let add_data_params = AddDataParams {
            prev_checkpoint: None,
            prev_offset: last_dataslice_info.prev_offset,
            new_offset_interval: last_dataslice_info.new_offset_interval.clone(),
            new_watermark: None,
            new_source_state: None,
        };

        dataset
            .commit_add_data(
                add_data_params,
                Some(OwnedFile::new(new_file_path)),
                None,
                CommitOpts {
                    block_ref: &BlockRef::Head,
                    system_time: Some(Utc::now()),
                    prev_block_hash: Some(Some(block_hash_to_reset)),
                    check_object_refs: false,
                },
            )
            .await?;

        Ok(())
    }

    fn remove_old_files(
        &self,
        chain_files_info: &ChainFilesInfo,
        listener: &Arc<dyn CompactionListener>,
    ) -> Result<(), CompactError> {
        listener.begin_phase(CompactionPhase::CleanDatasliceFiles);
        for file_url in &chain_files_info.data_slice_file_urls {
            fs::remove_file(file_url.replace("file://", "")).int_err()?;
        }

        listener.begin_phase(CompactionPhase::CleanBlockFiles);
        let block_hash_to_reset_string = chain_files_info
            .block_hash_to_reset
            .as_multibase()
            .to_string();
        for block_file_url in &chain_files_info.block_file_urls {
            if block_file_url.contains(&block_hash_to_reset_string) {
                break;
            }
            fs::remove_file(block_file_url.replace("file://", "")).int_err()?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl CompactService for CompactServiceImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn compact_dataset(
        &self,
        dataset_handle: &DatasetHandle,
        dataset_dir_path: &Path,
        multi_listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Result<(), CompactError> {
        let listener = multi_listener
            .and_then(|l| l.begin_compact(dataset_handle))
            .unwrap_or(Arc::new(NullCompactionListener {}));
        self.dataset_authorizer
            .check_action_allowed(dataset_handle, domain::auth::DatasetAction::Read)
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

        let chain_files_info = self.get_chain_files_info(dataset.clone()).await?;
        let new_file_path = self
            .write_file(
                chain_files_info.data_slice_file_urls.clone(),
                &dataset_handle.alias.dataset_name,
                &compact_dir_path,
                listener.clone(),
            )
            .await?;

        listener.begin_phase(CompactionPhase::ReplaceChainHead);
        self.replace_data_files(
            dataset.clone(),
            &new_file_path,
            &chain_files_info.block_hash_to_reset,
            &chain_files_info.last_dataslice_info,
        )
        .await?;

        self.remove_old_files(&chain_files_info, &listener)?;
        listener.success();

        Ok(())
    }
}
