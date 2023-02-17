// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


use flate2::Compression;
use kamu::domain::{MetadataChain};
use opendatafabric::{Multihash, MetadataEvent};
use futures::TryStreamExt;
use tar::Header;
use bytes::Bytes;
use crate::messages::{TransferSizeEstimation, ObjectsBatch, ObjectType};

/////////////////////////////////////////////////////////////////////////////////////////


pub async fn prepare_dataset_transfer_estimaton(
    metadata_chain: &dyn MetadataChain,
    stop_at: Multihash,
    begin_after: Option<Multihash>
) -> TransferSizeEstimation {

    let mut block_stream = metadata_chain
        .iter_blocks_interval(&stop_at, begin_after.as_ref(), false);

    let mut blocks_count: u32 = 0;
    let mut bytes_in_blocks: u64 = 0;

    let mut data_objects_count: u32 = 0;
    let mut checkpoint_objects_count: u32 = 0;
    let mut bytes_in_data_objects: i64 = 0;
    let mut bytes_in_checkpoint_objects: i64 = 0;

    while let Some((hash, block)) = block_stream.try_next().await.unwrap() {
        blocks_count += 1;

        // TODO: error handling of get_block_size
        bytes_in_blocks += metadata_chain.get_block_size(&hash).await.unwrap();

        match block.event {
            MetadataEvent::AddData(add_data) => {
                data_objects_count += 1;
                bytes_in_data_objects += add_data.output_data.size;

                if add_data.output_checkpoint.is_some() {
                    checkpoint_objects_count += 1;
                    bytes_in_checkpoint_objects += add_data.output_checkpoint.unwrap().size;
                }
            }
            MetadataEvent::ExecuteQuery(execute_query) => {
                if execute_query.output_data.is_some() {
                    data_objects_count += 1;
                    bytes_in_data_objects += execute_query.output_data.unwrap().size;
                }
                if execute_query.output_checkpoint.is_some() {
                    checkpoint_objects_count += 1;
                    bytes_in_checkpoint_objects += execute_query.output_checkpoint.unwrap().size;
                }
            }
            _ => ()
        }
    }        

    TransferSizeEstimation { 
        num_blocks: blocks_count, 
        num_objects: data_objects_count + checkpoint_objects_count, 
        bytes_in_raw_blocks: bytes_in_blocks,
        bytes_in_raw_objects: (bytes_in_data_objects + bytes_in_checkpoint_objects) as u64,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_dataset_metadata_batch(
    metadata_chain: &dyn MetadataChain,
    stop_at: Multihash,
    begin_after: Option<Multihash>
) -> ObjectsBatch {

    let mut blocks_count: u32 = 0;
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut tarball_builder = tar::Builder::new(encoder);

    let mut block_stream = 
        metadata_chain
            .iter_blocks_interval(&stop_at, begin_after.as_ref(), false);

    while let Some((hash, _)) = block_stream.try_next().await.unwrap() {
        blocks_count += 1;

        // TODO: error handling of get_block_bytes
        let block_bytes: Bytes = metadata_chain.get_block_bytes(&hash).await.unwrap();
        let block_data: &[u8] = &(*block_bytes);

        let mut header = Header::new_gnu();
        header.set_size(block_bytes.len() as u64);

        tarball_builder.append_data(&mut header, hash.to_multibase_string(), block_data).unwrap();
    }


    let tarball_data = tarball_builder.into_inner().unwrap().finish().unwrap();

    ObjectsBatch {
        objects_count: blocks_count,
        object_type: ObjectType::MetadataBlock,
        media_type: String::from("application/tar+gzip"),
        encoding: String::from("raw"),
        payload: tarball_data,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
