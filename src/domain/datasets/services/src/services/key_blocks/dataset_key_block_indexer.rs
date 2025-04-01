// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method2;
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::DatasetRegistry;
use kamu_datasets::{DatasetKeyBlock, DatasetKeyBlockRepository, MetadataEventType};
use odf::dataset::MetadataChainExt;

use crate::{
    JOB_KAMU_DATASETS_DATASET_KEY_BLOCK_INDEXER,
    JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetKeyBlockIndexer {
    catalog: dill::Catalog,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_DATASETS_DATASET_KEY_BLOCK_INDEXER,
    depends_on: &[
        JOB_KAMU_DATASETS_DATASET_REFERENCE_INDEXER,
    ],
    requires_transaction: false,
})]
impl DatasetKeyBlockIndexer {
    pub fn new(catalog: dill::Catalog) -> Self {
        Self { catalog }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    #[transactional_method2(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>
    )]
    async fn collect_datasets_to_index(&self) -> Result<Vec<odf::DatasetHandle>, InternalError> {
        let mut dataset_handles_stream = dataset_registry.all_dataset_handles();

        let mut datasets_to_index = Vec::new();
        let mut included_count = 0;
        let mut skipped_count = 0;

        use tokio_stream::StreamExt;
        while let Some(dataset_handle) = dataset_handles_stream.try_next().await? {
            included_count += 1;
            tracing::debug!(%dataset_handle, "Checking if dataset index exists");
            let has_blocks = dataset_key_block_repo
                .has_blocks(&dataset_handle.id, &odf::BlockRef::Head)
                .await?;

            if !has_blocks {
                tracing::debug!(%dataset_handle, "Dataset key block index does not exist");
                datasets_to_index.push(dataset_handle);
            } else {
                skipped_count += 1;
                tracing::debug!(%dataset_handle, "Dataset key block index already exists");
            }
        }

        tracing::debug!(
            included_count,
            skipped_count,
            collected_count = datasets_to_index.len(),
            "Finished collecting datasets to index"
        );

        Ok(datasets_to_index)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for DatasetKeyBlockIndexer {
    #[tracing::instrument(
        level = "info",
        skip_all,
        name = "DatasetKeyBlocksIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Collect dataset handles that have no block index built yet
        let datasets_to_index = self.collect_datasets_to_index().await?;
        if datasets_to_index.is_empty() {
            tracing::debug!("No datasets to index. All datasets have been indexed.");
            return Ok(());
        }

        // Convert handles into jobs wrapped into tokio tasks
        let mut job_results = tokio::task::JoinSet::new();
        for dataset_handle in datasets_to_index {
            let job = DatasetKeyBlockIndexingJob::new(&self.catalog, dataset_handle.clone());
            job_results.spawn(async move { (dataset_handle, job.run().await) });
        }

        // Execute jobs in parallel
        let results = job_results.join_all().await;

        // Report errors, if any
        for (dataset_handle, result) in results {
            if let Err(err) = result {
                tracing::error!(%dataset_handle, err = ?err, "Failed to index dataset key blocks");
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetKeyBlockIndexingJob {
    catalog: dill::Catalog,
    hdl_to_index: odf::DatasetHandle,
}

impl DatasetKeyBlockIndexingJob {
    fn new(catalog: &dill::Catalog, hdl_to_index: odf::DatasetHandle) -> Self {
        Self {
            catalog: catalog.clone(),
            hdl_to_index,
        }
    }

    #[transactional_method2(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>
    )]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(%hdl_to_index)
    )]
    async fn run(&self) -> Result<(), InternalError> {
        // Resolve dataset
        let target = dataset_registry
            .get_dataset_by_handle(&self.hdl_to_index)
            .await;

        use futures::stream::TryStreamExt;
        use odf::metadata::MetadataEventExt;

        // Iterate over blocks in the dataset
        // Collect key metadata blocks and save them in chunks.
        let mut current_chunk = Vec::new();
        let mut total_blocks = 0;
        let mut chunks_saved = 0;

        let mut blocks_stream = target.as_metadata_chain().iter_blocks();
        while let Some((block_hash, block)) = blocks_stream.try_next().await.int_err()? {
            // Ignore non-key events, such as `AddData` and `ExecuteTransform`
            if block.event.is_key_event() {
                // Serialize the event and form a key block value
                current_chunk.push(DatasetKeyBlock {
                    event_kind: MetadataEventType::from_metadata_event(&block.event),
                    sequence_number: block.sequence_number,
                    block_hash,
                    event_payload: serde_json::to_value(&block.event).int_err()?,
                    created_at: block.system_time,
                });

                total_blocks += 1;

                // Save in chunks of 100
                if current_chunk.len() >= 100 {
                    dataset_key_block_repo
                        .save_blocks_batch(target.get_id(), &odf::BlockRef::Head, &current_chunk)
                        .await
                        .int_err()?;

                    tracing::debug!(
                        dataset_id = %target.get_id(),
                        chunk_size = current_chunk.len(),
                        last_block_hash = %current_chunk.last().unwrap().block_hash,
                        last_block_sequence_number = current_chunk.last().unwrap().sequence_number,
                        "Chunk of key blocks collected and saved"
                    );

                    // Clear the chunk for the next iteration
                    current_chunk.clear();
                    chunks_saved += 1;
                }
            }
        }

        // Save remaining blocks
        if !current_chunk.is_empty() {
            dataset_key_block_repo
                .save_blocks_batch(target.get_id(), &odf::BlockRef::Head, &current_chunk)
                .await
                .int_err()?;

            tracing::debug!(
                dataset_id = %target.get_id(),
                chunk_size = current_chunk.len(),
                last_block_hash = %current_chunk.last().unwrap().block_hash,
                last_block_sequence_number = current_chunk.last().unwrap().sequence_number,
                "Tail chunk of key blocks saved"
            );
            chunks_saved += 1;
        }

        // Report indexing metrics for this dataset
        tracing::debug!(
            dataset_id = %target.get_id(),
            total_blocks,
            chunks_saved,
            "Finished indexing dataset key blocks",
        );

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
