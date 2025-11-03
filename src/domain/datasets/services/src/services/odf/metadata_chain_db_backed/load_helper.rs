// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn load_data_blocks_from_repository(
    data_block_repository: &dyn kamu_datasets::DatasetDataBlockRepository,
    dataset_id: &odf::DatasetID,
    page_size: usize,
    sequence_number: u64,
) -> Result<Vec<(odf::Multihash, bytes::Bytes, odf::MetadataBlock)>, InternalError> {
    // Load data block records from repository
    let data_block_records = data_block_repository
        .get_page_of_data_blocks(dataset_id, &odf::BlockRef::Head, page_size, sequence_number)
        .await
        .int_err()?;

    // Convert to metadata blocks
    let data_blocks = data_block_records
        .into_iter()
        .map(|data_block| {
            odf::storage::deserialize_metadata_block(
                &data_block.block_hash,
                &data_block.block_payload,
            )
            .map(|metadata_block| {
                (
                    data_block.block_hash,
                    data_block.block_payload,
                    metadata_block,
                )
            })
            .int_err()
        })
        .collect::<Result<Vec<_>, InternalError>>()?;

    tracing::debug!(
        %dataset_id,
        page_size,
        sequence_number,
        min_bound = ?data_blocks.first().map(|(_, _, b)| b.sequence_number),
        max_bound = ?data_blocks.last().map(|(_, _, b)| b.sequence_number),
        "Loaded data blocks page",
    );

    Ok(data_blocks)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
