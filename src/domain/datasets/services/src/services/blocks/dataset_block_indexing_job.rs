// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method3;
use dill::Catalog;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetBlock,
    DatasetDataBlockRepository,
    DatasetKeyBlockRepository,
    DatasetRegistry,
    MetadataEventType,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BLOCK_CHUNK_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DatasetBlockIndexingJob {
    catalog: Catalog,
    hdl_to_index: odf::DatasetHandle,
    block_ref: odf::BlockRef,
}

impl DatasetBlockIndexingJob {
    pub(crate) fn new(
        catalog: Catalog,
        hdl_to_index: odf::DatasetHandle,
        block_ref: odf::BlockRef,
    ) -> Self {
        Self {
            catalog,
            hdl_to_index,
            block_ref,
        }
    }

    #[transactional_method3(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_key_block_repo: Arc<dyn DatasetKeyBlockRepository>,
        dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>
    )]
    #[tracing::instrument(
        level = "debug",
        name = "DatasetBlockIndexingJob::run",
        skip_all,
        fields(dataset_handle=%self.hdl_to_index)
    )]
    pub(crate) async fn run(self) -> Result<(), InternalError> {
        // Resolve dataset
        let target = dataset_registry
            .get_dataset_by_handle(&self.hdl_to_index)
            .await;

        // Run the indexing job for entire dataset with a single scan
        // Note: we don't need to handle invalid interval error,
        //  as we are scanning the entire dataset from HEAD to SEED
        index_dataset_blocks_entirely(
            dataset_key_block_repo.as_ref(),
            dataset_data_block_repo.as_ref(),
            target,
            &self.block_ref,
        )
        .await
        .int_err()?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indexes both dataset key and data blocks from head to seed in a single scan,
/// assuming no blocks are stored yet for the given `block_ref`
pub(crate) async fn index_dataset_blocks_entirely(
    dataset_key_block_repo: &dyn DatasetKeyBlockRepository,
    dataset_data_block_repo: &dyn DatasetDataBlockRepository,
    target: ResolvedDataset,
    block_ref: &odf::BlockRef,
) -> Result<(), InternalError> {
    // Make sure the repositories contain no blocks for this dataset
    assert!(
        !dataset_key_block_repo
            .has_key_blocks_for_ref(target.get_id(), block_ref)
            .await?
    );
    assert!(
        !dataset_data_block_repo
            .has_data_blocks_for_ref(target.get_id(), block_ref)
            .await?
    );

    // Read HEAD ref
    let head = target
        .as_metadata_chain()
        .resolve_ref(block_ref)
        .await
        .int_err()?;

    use futures::stream::TryStreamExt;

    // Separate chunks for key and data blocks
    let mut key_chunk = Vec::new();
    let mut data_chunk = Vec::new();
    let mut total_key_blocks = 0;
    let mut total_data_blocks = 0;
    let mut key_chunks_saved = 0;
    let mut data_chunks_saved = 0;

    // Iterate over blocks of the entire dataset in a single pass
    let mut blocks_stream = target
        .as_metadata_chain()
        .as_uncached_chain()
        .iter_blocks_interval((&head).into(), None, true);

    while let Some((block_hash, block)) = blocks_stream.try_next().await.int_err()? {
        let event_flags = odf::metadata::MetadataEventTypeFlags::from(&block.event);
        let block_entity = make_dataset_block(block_hash, &block);

        if event_flags.has_data_flags() {
            // This is a data block (AddData, ExecuteTransform)
            data_chunk.push(block_entity);
            total_data_blocks += 1;

            // Save data blocks in chunks
            if data_chunk.len() >= BLOCK_CHUNK_SIZE {
                dataset_data_block_repo
                    .save_data_blocks_batch(target.get_id(), block_ref, &data_chunk)
                    .await
                    .int_err()?;

                tracing::debug!(
                    dataset_id = %target.get_id(),
                    chunk_size = data_chunk.len(),
                    last_block_hash = %data_chunk.last().unwrap().block_hash,
                    last_block_sequence_number = data_chunk.last().unwrap().sequence_number,
                    "Chunk of data blocks collected and saved"
                );

                data_chunk.clear();
                data_chunks_saved += 1;
            }
        } else {
            // This is a key block (everything else)
            key_chunk.push(block_entity);
            total_key_blocks += 1;

            // Save key blocks in chunks
            if key_chunk.len() >= BLOCK_CHUNK_SIZE {
                dataset_key_block_repo
                    .save_key_blocks_batch(target.get_id(), block_ref, &key_chunk)
                    .await
                    .int_err()?;

                tracing::debug!(
                    dataset_id = %target.get_id(),
                    chunk_size = key_chunk.len(),
                    last_block_hash = %key_chunk.last().unwrap().block_hash,
                    last_block_sequence_number = key_chunk.last().unwrap().sequence_number,
                    "Chunk of key blocks collected and saved"
                );

                key_chunk.clear();
                key_chunks_saved += 1;
            }
        }
    }

    // Save remaining key blocks
    if !key_chunk.is_empty() {
        dataset_key_block_repo
            .save_key_blocks_batch(target.get_id(), block_ref, &key_chunk)
            .await
            .int_err()?;

        tracing::debug!(
            dataset_id = %target.get_id(),
            chunk_size = key_chunk.len(),
            last_block_hash = %key_chunk.last().unwrap().block_hash,
            last_block_sequence_number = key_chunk.last().unwrap().sequence_number,
            "Tail chunk of key blocks saved"
        );
        key_chunks_saved += 1;
    }

    // Save remaining data blocks
    if !data_chunk.is_empty() {
        dataset_data_block_repo
            .save_data_blocks_batch(target.get_id(), block_ref, &data_chunk)
            .await
            .int_err()?;

        tracing::debug!(
            dataset_id = %target.get_id(),
            chunk_size = data_chunk.len(),
            last_block_hash = %data_chunk.last().unwrap().block_hash,
            last_block_sequence_number = data_chunk.last().unwrap().sequence_number,
            "Tail chunk of data blocks saved"
        );
        data_chunks_saved += 1;
    }

    // Report indexing metrics for this dataset
    tracing::debug!(
        dataset_id = %target.get_id(),
        total_key_blocks,
        key_chunks_saved,
        total_data_blocks,
        data_chunks_saved,
        "Finished indexing dataset blocks in single scan",
    );

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_dataset_block(
    block_hash: odf::Multihash,
    block: &odf::MetadataBlock,
) -> DatasetBlock {
    let block_data = odf::storage::serialize_metadata_block(block).unwrap();

    DatasetBlock {
        event_kind: MetadataEventType::from_metadata_event(&block.event),
        sequence_number: block.sequence_number,
        block_hash,
        block_payload: bytes::Bytes::from(block_data),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
