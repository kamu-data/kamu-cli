// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Read;

use crate::messages::{
    ObjectFileReference, ObjectPullStrategy, ObjectType, ObjectsBatch, PullObjectTransferStrategy,
    TransferSizeEstimation, TransferUrl,
};
use bytes::Bytes;
use flate2::Compression;
use futures::{stream, StreamExt, TryStreamExt};
use kamu::domain::{
    AppendOpts, CorruptedSourceError, Dataset, InsertError, InsertOpts, MetadataChain, SyncError,
};
use opendatafabric::{MetadataBlock, MetadataEvent, Multihash};
use tar::Header;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

const MEDIA_TAR_GZ: &str = "application/tar+gzip";
const ENCODING_RAW: &str = "raw";

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_dataset_transfer_estimaton(
    metadata_chain: &dyn MetadataChain,
    stop_at: Multihash,
    begin_after: Option<Multihash>,
) -> TransferSizeEstimation {
    let mut block_stream =
        metadata_chain.iter_blocks_interval(&stop_at, begin_after.as_ref(), false);

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
            _ => (),
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
    begin_after: Option<Multihash>,
) -> ObjectsBatch {
    let mut blocks_count: u32 = 0;
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut tarball_builder = tar::Builder::new(encoder);

    let blocks_for_transfer: Vec<(Multihash, MetadataBlock)> = metadata_chain
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

        tarball_builder
            .append_data(&mut header, hash.to_multibase_string(), block_data)
            .unwrap();
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

pub async fn dataset_import_pulled_metadata(
    dataset: &dyn Dataset,
    objects_batch: ObjectsBatch,
) -> Vec<Vec<ObjectFileReference>> {
    let blocks_data = unpack_dataset_metadata_batch(objects_batch).await;

    let loaded_blocks = load_dataset_blocks(dataset.as_metadata_chain(), blocks_data).await;

    let object_files =
        collect_missing_object_references_from_metadata(dataset, loaded_blocks).await;

    // Future: analyze sizes and split on stages

    vec![object_files]
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn unpack_dataset_metadata_batch(objects_batch: ObjectsBatch) -> Vec<(Multihash, Vec<u8>)> {
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

async fn load_dataset_blocks(
    metadata_chain: &dyn MetadataChain,
    blocks_data: Vec<(Multihash, Vec<u8>)>,
) -> Vec<MetadataBlock> {
    stream::iter(blocks_data)
        .then(|(hash, block_buf)| async move {
            println!("> {} - {} bytes", hash, block_buf.len());
            let block = metadata_chain
                .append_block_from_bytes(
                    &hash,
                    Bytes::copy_from_slice(block_buf.as_slice()),
                    AppendOpts {
                        expected_hash: Some(&hash),
                        ..AppendOpts::default()
                    },
                )
                .await
                .unwrap();
            block
        })
        .collect()
        .await
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn collect_missing_object_references_from_metadata(
    dataset: &dyn Dataset,
    blocks: Vec<MetadataBlock>,
) -> Vec<ObjectFileReference> {
    let data_repo = dataset.as_data_repo();
    let checkpoint_repo = dataset.as_checkpoint_repo();

    let mut object_files: Vec<ObjectFileReference> = Vec::new();
    for block in blocks {
        match block.event {
            MetadataEvent::AddData(e) => {
                if !data_repo
                    .contains(&e.output_data.physical_hash)
                    .await
                    .unwrap()
                {
                    object_files.push(ObjectFileReference {
                        object_type: ObjectType::DataSlice,
                        physical_hash: e.output_data.physical_hash.clone(),
                        size: e.output_data.size,
                    });
                }
                if let Some(checkpoint) = e.output_checkpoint {
                    if !checkpoint_repo
                        .contains(&checkpoint.physical_hash)
                        .await
                        .unwrap()
                    {
                        object_files.push(ObjectFileReference {
                            object_type: ObjectType::Checkpoint,
                            physical_hash: checkpoint.physical_hash.clone(),
                            size: checkpoint.size,
                        });
                    }
                }
            }
            MetadataEvent::ExecuteQuery(e) => {
                if let Some(data_slice) = e.output_data {
                    if !data_repo.contains(&data_slice.physical_hash).await.unwrap() {
                        object_files.push(ObjectFileReference {
                            object_type: ObjectType::DataSlice,
                            physical_hash: data_slice.physical_hash.clone(),
                            size: data_slice.size,
                        });
                    }
                }
                if let Some(checkpoint) = e.output_checkpoint {
                    if !checkpoint_repo
                        .contains(&checkpoint.physical_hash)
                        .await
                        .unwrap()
                    {
                        object_files.push(ObjectFileReference {
                            object_type: ObjectType::Checkpoint,
                            physical_hash: checkpoint.physical_hash.clone(),
                            size: checkpoint.size,
                        });
                    }
                }
            }
            _ => (),
        }
    }

    object_files
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_object_transfer_strategy(
    dataset: &dyn Dataset,
    prefix_url: &Url,
    object_file_ref: &ObjectFileReference,
) -> PullObjectTransferStrategy {
    PullObjectTransferStrategy {
        object_file: object_file_ref.clone(),
        pull_strategy: crate::messages::ObjectPullStrategy::HttpDownload,
        download_from: {
            let (url, expires_at) = match object_file_ref.object_type {
                ObjectType::MetadataBlock => {
                    let blocks_url = prefix_url.join("blocks/").unwrap();
                    dataset
                        .as_metadata_chain()
                        .get_block_download_url(&blocks_url, &object_file_ref.physical_hash)
                        .await
                        .unwrap()
                }
                ObjectType::DataSlice => {
                    let data_url = prefix_url.join("data/").unwrap();
                    dataset
                        .as_data_repo()
                        .get_download_url(&data_url, &object_file_ref.physical_hash)
                        .await
                        .unwrap()
                }
                ObjectType::Checkpoint => {
                    let checkpoints_url = prefix_url.join("checkpoints/").unwrap();
                    dataset
                        .as_checkpoint_repo()
                        .get_download_url(&checkpoints_url, &object_file_ref.physical_hash)
                        .await
                        .unwrap()
                }
            };
            TransferUrl { url, expires_at }
        },
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_import_object_file(
    dataset: &dyn Dataset,
    object_transfer_strategy: &PullObjectTransferStrategy,
) -> Result<(), SyncError> {
    if object_transfer_strategy.pull_strategy != ObjectPullStrategy::HttpDownload {
        panic!(
            "Unsupported pull strategy {:?}",
            object_transfer_strategy.pull_strategy
        );
    }

    let object_file_reference = &object_transfer_strategy.object_file;

    let client = reqwest::Client::new();

    // TODO: error handling
    let response = client
        .get(object_transfer_strategy.download_from.url.clone())
        .send()
        .await
        .unwrap();

    let stream = response.bytes_stream();

    use tokio_util::compat::FuturesAsyncReadCompatExt;
    let reader = stream
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();

    let target_object_repository = match object_file_reference.object_type {
        ObjectType::MetadataBlock => panic!("Metadata block unexpected at objects import stage"),
        ObjectType::DataSlice => dataset.as_data_repo(),
        ObjectType::Checkpoint => dataset.as_checkpoint_repo(),
    };

    let res = target_object_repository
        .insert_stream(
            Box::new(reader),
            InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&object_file_reference.physical_hash),
                size_hint: Some(object_file_reference.size as usize),
            },
        )
        .await;

    match res {
        Ok(_) => Ok(()),
        Err(InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
            message: concat!(
                "Data file hash declared by the source didn't match ",
                "the computed - this may be an indication of hashing ",
                "algorithm mismatch or an attempted tampering",
            )
            .to_owned(),
            source: Some(e.into()),
        }
        .into()),
        Err(InsertError::Access(e)) => Err(SyncError::Access(e)),
        Err(InsertError::Internal(e)) => Err(SyncError::Internal(e)),
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
