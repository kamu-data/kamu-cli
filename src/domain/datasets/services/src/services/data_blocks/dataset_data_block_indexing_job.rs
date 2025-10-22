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
use kamu_datasets::{DatasetBlock, DatasetDataBlockRepository, MetadataEventType};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DatasetDataBlockIndexingJob {
    catalog: Catalog,
    hdl_to_index: odf::DatasetHandle,
}

impl DatasetDataBlockIndexingJob {
    pub(crate) fn new(catalog: &Catalog, hdl_to_index: odf::DatasetHandle) -> Self {
        Self {
            catalog: catalog.clone(),
            hdl_to_index,
        }
    }

    #[transactional_method2(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_data_block_repo: Arc<dyn DatasetDataBlockRepository>
    )]
    #[tracing::instrument(
        level = "debug",
        name = "DatasetDataBlockIndexingJob::run",
        skip_all,
        fields(dataset_handle=%self.hdl_to_index)
    )]
    pub(crate) async fn run(self) -> Result<(), InternalError> {
        // Resolve dataset
        let target = dataset_registry
            .get_dataset_by_handle(&self.hdl_to_index)
            .await;

        // Run the indexing job for entire dataset
        // Note: we don't need to handle invalid interval error,
        //  as we are scanning the entire dataset from HEAD to SEED
        index_dataset_data_blocks_entirely(
            dataset_data_block_repo.as_ref(),
            target,
            &odf::BlockRef::Head,
        )
        .await
        .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Indexes dataset data blocks from head to seed,
///  assuming no data blocks are stored yet
pub(crate) async fn index_dataset_data_blocks_entirely(
    dataset_data_block_repo: &dyn DatasetDataBlockRepository,
    target: ResolvedDataset,
    block_ref: &odf::BlockRef,
) -> Result<(), InternalError> {
    // Make sure the repository contains no data blocks for this dataset
    assert!(
        !dataset_data_block_repo
            .has_blocks(target.get_id(), block_ref)
            .await?
    );

    // Read HEAD ref
    let head = target
        .as_metadata_chain()
        .resolve_ref(block_ref)
        .await
        .int_err()?;

    use futures::stream::TryStreamExt;
    use odf::dataset::MetadataChainExt;

    // Collect data blocks and save them in chunks.
    let mut current_chunk = Vec::new();
    let mut total_blocks = 0;
    let mut chunks_saved = 0;

    // Iterate over blocks of the entire dataset.
    let mut blocks_stream = target
        .as_metadata_chain()
        .iter_blocks_interval(&head, None, true);

    while let Some((block_hash, block)) = blocks_stream.try_next().await.int_err()? {
        // Collect only data events, such as `AddData` and `ExecuteTransform`
        let event_flags = odf::metadata::MetadataEventTypeFlags::from(&block.event);
        if event_flags.has_data_flags() {
            // Create a data block entity and collect it in the current chunk
            current_chunk.push(make_data_block(block_hash, &block));
            total_blocks += 1;

            // Save in chunks of 100
            if current_chunk.len() >= 100 {
                dataset_data_block_repo
                    .save_blocks_batch(target.get_id(), block_ref, &current_chunk)
                    .await
                    .int_err()?;

                tracing::debug!(
                    dataset_id = %target.get_id(),
                    chunk_size = current_chunk.len(),
                    last_block_hash = %current_chunk.last().unwrap().block_hash,
                    last_block_sequence_number = current_chunk.last().unwrap().sequence_number,
                    "Chunk of data blocks collected and saved"
                );

                // Clear the chunk for the next iteration
                current_chunk.clear();
                chunks_saved += 1;
            }
        }
    }

    // Save remaining blocks
    if !current_chunk.is_empty() {
        dataset_data_block_repo
            .save_blocks_batch(target.get_id(), block_ref, &current_chunk)
            .await
            .int_err()?;

        tracing::debug!(
            dataset_id = %target.get_id(),
            chunk_size = current_chunk.len(),
            last_block_hash = %current_chunk.last().unwrap().block_hash,
            last_block_sequence_number = current_chunk.last().unwrap().sequence_number,
            "Tail chunk of data blocks saved"
        );
        chunks_saved += 1;
    }

    // Report indexing metrics for this dataset
    tracing::debug!(
        dataset_id = %target.get_id(),
        total_blocks,
        chunks_saved,
        "Finished indexing dataset data blocks",
    );

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn collect_dataset_data_blocks_in_range(
    target: ResolvedDataset,
    head: &odf::Multihash,
    tail: Option<&odf::Multihash>,
) -> Result<CollectDataBlockResponse, InternalError> {
    use futures::stream::TryStreamExt;
    use odf::dataset::MetadataChainExt;

    // Resulting blocks and event flags
    let mut data_blocks = Vec::new();

    // Iterate over blocks in the dataset in the specified range.
    // Note: don't ignore missing tail, we want to detect InvalidInterval error.
    //       Therefore, we need to iterate through all blocks, not only data ones,
    //       to perform an accurate tail check.
    let mut blocks_stream = target
        .as_metadata_chain()
        .iter_blocks_interval(head, tail, false);

    loop {
        // Try reading next stream element
        let try_next_result = match blocks_stream.try_next().await {
            // Normal stream element
            Ok(maybe_hashed_block) => maybe_hashed_block,

            // Invalid interval: return so far collected result with divergence marker
            Err(odf::dataset::IterBlocksError::InvalidInterval(_)) => {
                return Ok(CollectDataBlockResponse {
                    data_blocks,
                    divergence_detected: true,
                });
            }

            // Other errors are internal
            Err(odf::IterBlocksError::Internal(e)) => return Err(e),
            Err(e) => return Err(e.int_err()),
        };

        // Check if we've reached the end of stream
        let Some((block_hash, block)) = try_next_result else {
            break;
        };

        // Collect only data events, such as `AddData` and `ExecuteTransform`
        let event_flags = odf::metadata::MetadataEventTypeFlags::from(&block.event);
        if event_flags.has_data_flags() {
            // Create a data block entity
            data_blocks.push(make_data_block(block_hash, &block));
        }
    }

    Ok(CollectDataBlockResponse {
        data_blocks,
        divergence_detected: false,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn make_data_block(
    block_hash: odf::Multihash,
    block: &odf::MetadataBlock,
) -> DatasetBlock {
    use odf::serde::MetadataBlockSerializer;
    use odf::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;

    let block_data = FlatbuffersMetadataBlockSerializer
        .write_manifest(block)
        .unwrap();

    DatasetBlock {
        event_kind: MetadataEventType::from_metadata_event(&block.event),
        sequence_number: block.sequence_number,
        block_hash,
        block_payload: bytes::Bytes::from(block_data.collapse_vec()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct CollectDataBlockResponse {
    pub(crate) data_blocks: Vec<DatasetBlock>,
    pub(crate) divergence_detected: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
