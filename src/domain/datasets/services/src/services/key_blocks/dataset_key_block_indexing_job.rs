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
use dill::Catalog;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::{DatasetRegistry, ResolvedDataset};
use kamu_datasets::{DatasetKeyBlock, DatasetKeyBlockRepository, MetadataEventType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DatasetKeyBlockIndexingJob {
    catalog: Catalog,
    hdl_to_index: odf::DatasetHandle,
}

impl DatasetKeyBlockIndexingJob {
    pub(crate) fn new(catalog: &Catalog, hdl_to_index: odf::DatasetHandle) -> Self {
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
    pub(crate) async fn run(self) -> Result<(), InternalError> {
        // Resolve dataset
        let target = dataset_registry
            .get_dataset_by_handle(&self.hdl_to_index)
            .await;

        // Run the indexing job for entire dataset
        // Note: we don't need to handle invalid interval error,
        //  as we are scanning the entire dataset from HEAD to SEED
        index_dataset_key_blocks_entirely(dataset_key_block_repo.as_ref(), target)
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_dataset_key_blocks_entirely(
    dataset_key_block_repo: &dyn DatasetKeyBlockRepository,
    target: ResolvedDataset,
) -> Result<(), DatasetKeyBlockIndexingError> {
    // Read HEAD ref
    let head = target
        .as_metadata_chain()
        .resolve_ref(&odf::BlockRef::Head)
        .await
        .int_err()?;

    // Run the indexing job for entire dataset
    index_dataset_key_blocks_in_range(dataset_key_block_repo, target, &head, None).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn index_dataset_key_blocks_in_range(
    dataset_key_block_repo: &dyn DatasetKeyBlockRepository,
    target: ResolvedDataset,
    head: &odf::Multihash,
    tail: Option<&odf::Multihash>,
) -> Result<(), DatasetKeyBlockIndexingError> {
    use futures::stream::TryStreamExt;
    use odf::dataset::MetadataChainExt;
    use odf::metadata::MetadataEventExt;

    // Collect key metadata blocks and save them in chunks.
    let mut current_chunk = Vec::new();
    let mut total_blocks = 0;
    let mut chunks_saved = 0;

    // Iterate over blocks in the dataset in the specified range
    let mut blocks_stream = target
        .as_metadata_chain()
        .iter_blocks_interval(head, tail, false);

    while let Some((block_hash, block)) = blocks_stream.try_next().await? {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub(crate) enum DatasetKeyBlockIndexingError {
    #[error(transparent)]
    InvalidInterval(odf::dataset::InvalidIntervalError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::dataset::IterBlocksError> for DatasetKeyBlockIndexingError {
    fn from(e: odf::dataset::IterBlocksError) -> Self {
        match e {
            odf::dataset::IterBlocksError::InvalidInterval(e) => Self::InvalidInterval(e),
            _ => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
