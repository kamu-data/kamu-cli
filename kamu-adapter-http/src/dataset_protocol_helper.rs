// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


use std::io::Read;

use flate2::Compression;
use kamu::domain::{MetadataChain, Dataset};
use opendatafabric::{Multihash, MetadataEvent, MetadataBlock};
use futures::TryStreamExt;
use tar::Header;
use bytes::Bytes;
use url::Url;
use crate::messages::{TransferSizeEstimation, ObjectsBatch, ObjectType, ObjectFileReference, PullObjectTransferStrategy, TransferUrl};

/////////////////////////////////////////////////////////////////////////////////////////

const MEDIA_TAR_GZ: &str = "application/tar+gzip";
const ENCODING_RAW: &str = "raw";

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

    let blocks_for_transfer: Vec<(Multihash, MetadataBlock)> = 
        metadata_chain
            .iter_blocks_interval(&stop_at, begin_after.as_ref(), false)
            .try_collect()
            .await
            .unwrap();

    for (hash, _) in blocks_for_transfer.iter().rev() {
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
        media_type: String::from(MEDIA_TAR_GZ),
        encoding: String::from(ENCODING_RAW),
        payload: tarball_data,
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn unpack_dataset_metadata_batch(
    objects_batch: ObjectsBatch
) -> Vec<(Multihash, Vec<u8>)> {

    if !objects_batch.media_type.eq(MEDIA_TAR_GZ) {
        panic!("Unsupported media type {}", objects_batch.media_type);
    }

    if !objects_batch.encoding.eq(ENCODING_RAW) {
        panic!("Unsupported batch encoding type {}", objects_batch.encoding);
    }

    if objects_batch.object_type != ObjectType::MetadataBlock {
        panic!("Unexpected object type {:?}", objects_batch.object_type);
    }

    let decoder = flate2::read::GzDecoder::new(objects_batch.payload.as_slice());
    let mut archive = tar::Archive::new(decoder);
    let blocks_data: Vec<(Multihash, Vec<u8>)> = archive
        .entries()
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|mut entry| {
            let entry_size = entry.size();
            let mut buf = vec![0 as u8; entry_size as usize];
            entry.read(buf.as_mut_slice()).unwrap();
            
            let path = entry.path().unwrap().to_owned();
            let hash = Multihash::from_multibase_str(path.to_str().unwrap()).unwrap();

            (hash, buf)
        })
        .collect();

    blocks_data
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn collect_missing_object_references_from_metadata(
    dataset: &dyn Dataset,
    blocks_data: Vec<(Multihash, Vec<u8>)>
) -> Vec<ObjectFileReference> {

    let metadata_chain = dataset.as_metadata_chain();
    let data_repo = dataset.as_data_repo();
    let checkpoint_repo = dataset.as_checkpoint_repo();

    let mut object_files: Vec<ObjectFileReference> = Vec::new();
    for (hash, block_data) in blocks_data {
        println!("> {} - {} bytes", hash, block_data.len());
        let block = metadata_chain.construct_block_from_bytes(
            &hash, Bytes::copy_from_slice(block_data.as_slice())
        ).await.unwrap();

        match block.event {
            MetadataEvent::AddData(e) => {
                if !data_repo.contains(&e.output_data.physical_hash).await.unwrap() {
                    object_files.push(
                        ObjectFileReference {
                            object_type: ObjectType::DataSlice,
                            physical_hash: e.output_data.physical_hash.clone(),
                        }
                    );
                }
                if let Some(checkpoint) = e.output_checkpoint {
                    if !checkpoint_repo.contains(&checkpoint.physical_hash).await.unwrap() {
                        object_files.push(
                            ObjectFileReference {
                                object_type: ObjectType::Checkpoint,
                                physical_hash: checkpoint.physical_hash.clone()
                            }
                        );
                    }
                }
            },
            MetadataEvent::ExecuteQuery(e) => {
                if let Some(data_slice) = e.output_data {
                    if !data_repo.contains(&data_slice.physical_hash).await.unwrap() {
                        object_files.push(
                            ObjectFileReference {
                                object_type: ObjectType::DataSlice,
                                physical_hash: data_slice.physical_hash.clone(),
                            }
                        );
                    }
                }
                if let Some(checkpoint) = e.output_checkpoint {
                    if !checkpoint_repo.contains(&checkpoint.physical_hash).await.unwrap() {
                        object_files.push(
                            ObjectFileReference {
                                object_type: ObjectType::Checkpoint,
                                physical_hash: checkpoint.physical_hash.clone()
                            }
                        );
                    }
                }
            },
            _ => (),
        }
    }

    object_files

}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_object_transfer_strategy(
    dataset: &dyn Dataset,
    prefix_url: &Url,
    object_file_ref: &ObjectFileReference
) -> PullObjectTransferStrategy {

    PullObjectTransferStrategy {
        object_file: object_file_ref.clone(),
        pull_strategy: crate::messages::ObjectPullStrategy::HttpDownload,
        download_from: {
            let (url, expires_at) = match object_file_ref.object_type {
                ObjectType::MetadataBlock => {
                    let blocks_url = prefix_url.join("blocks/").unwrap();
                    dataset.as_metadata_chain()
                        .get_block_download_url(&blocks_url, &object_file_ref.physical_hash)
                        .await
                        .unwrap()
                }
                ObjectType::DataSlice => {
                    let data_url = prefix_url.join("data/").unwrap();
                    dataset.as_data_repo()
                        .get_download_url(&data_url, &object_file_ref.physical_hash)
                        .await
                        .unwrap()
                }
                ObjectType::Checkpoint => {
                    let checkpoints_url = prefix_url.join("checkpoints/").unwrap();
                    dataset.as_checkpoint_repo()
                        .get_download_url(&checkpoints_url, &object_file_ref.physical_hash)
                        .await
                        .unwrap()
                }
            };
            TransferUrl { url, expires_at }
        }
        
    }

}

/////////////////////////////////////////////////////////////////////////////////////////