// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Read;

use crate::smart_protocol::{errors::ObjectUploadError, messages::*};
use bytes::Bytes;
use flate2::Compression;
use futures::{stream, StreamExt, TryStreamExt};
use kamu::domain::*;
use opendatafabric::{MetadataBlock, MetadataEvent, Multihash};
use tar::Header;
use thiserror::Error;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

const MEDIA_TAR_GZ: &str = "application/tar+gzip";
const ENCODING_RAW: &str = "raw";

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PrepareDatasetTransferEstimateError {
    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        InvalidIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<IterBlocksError> for PrepareDatasetTransferEstimateError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::InvalidInterval(e) => Self::InvalidInterval(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

pub async fn prepare_dataset_transfer_estimate(
    metadata_chain: &dyn MetadataChain,
    stop_at: &Multihash,
    begin_after: Option<&Multihash>,
) -> Result<TransferSizeEstimate, PrepareDatasetTransferEstimateError> {
    let mut block_stream = metadata_chain.iter_blocks_interval(stop_at, begin_after, false);

    let mut blocks_count: u32 = 0;
    let mut bytes_in_blocks: u64 = 0;

    let mut data_objects_count: u32 = 0;
    let mut checkpoint_objects_count: u32 = 0;
    let mut bytes_in_data_objects: i64 = 0;
    let mut bytes_in_checkpoint_objects: i64 = 0;

    while let Some((hash, block)) = block_stream.try_next().await? {
        blocks_count += 1;

        bytes_in_blocks += metadata_chain
            .as_object_repo()
            .get_size(&hash)
            .await
            .int_err()?;

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
            MetadataEvent::Seed(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetWatermark(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_) => (),
        }
    }

    Ok(TransferSizeEstimate {
        num_blocks: blocks_count,
        num_objects: data_objects_count + checkpoint_objects_count,
        bytes_in_raw_blocks: bytes_in_blocks,
        bytes_in_raw_objects: (bytes_in_data_objects + bytes_in_checkpoint_objects) as u64,
    })
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_dataset_metadata_batch(
    metadata_chain: &dyn MetadataChain,
    stop_at: &Multihash,
    begin_after: Option<&Multihash>,
) -> Result<ObjectsBatch, InternalError> {
    let mut blocks_count: u32 = 0;
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut tarball_builder = tar::Builder::new(encoder);

    let blocks_for_transfer: Vec<(Multihash, MetadataBlock)> = metadata_chain
        .iter_blocks_interval(stop_at, begin_after, false)
        .try_collect()
        .await
        .int_err()?;

    for (hash, _) in blocks_for_transfer.iter().rev() {
        blocks_count += 1;

        let block_bytes: Bytes = metadata_chain
            .as_object_repo()
            .get_bytes(&hash)
            .await
            .int_err()?;

        let block_data: &[u8] = &(*block_bytes);

        let mut header = Header::new_gnu();
        header.set_size(block_bytes.len() as u64);

        tarball_builder
            .append_data(&mut header, hash.to_multibase_string(), block_data)
            .int_err()?;
    }

    let tarball_data = tarball_builder.into_inner().int_err()?.finish().int_err()?;

    Ok(ObjectsBatch {
        objects_count: blocks_count,
        object_type: ObjectType::MetadataBlock,
        media_type: String::from(MEDIA_TAR_GZ),
        encoding: String::from(ENCODING_RAW),
        payload: tarball_data,
    })
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_load_metadata(
    dataset: &dyn Dataset,
    objects_batch: ObjectsBatch,
) -> Vec<(Multihash, MetadataBlock)> {
    let blocks_data = unpack_dataset_metadata_batch(objects_batch).await;
    load_dataset_blocks(dataset.as_metadata_chain(), blocks_data).await
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_append_metadata(
    dataset: &dyn Dataset,
    metadata: Vec<(Multihash, MetadataBlock)>,
) -> Result<(), AppendError> {
    let metadata_chain = dataset.as_metadata_chain();
    for (hash, block) in metadata {
        tracing::debug!("Appending block #{} {}", block.sequence_number, hash);
        metadata_chain
            .append(
                block,
                AppendOpts {
                    expected_hash: Some(&hash),
                    ..AppendOpts::default()
                },
            )
            .await?;
    }

    Ok(())
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
) -> Vec<(Multihash, MetadataBlock)> {
    stream::iter(blocks_data)
        .then(|(hash, block_buf)| async move {
            tracing::debug!("> {} - {} bytes", hash, block_buf.len());
            let block = metadata_chain
                .get_block_from_bytes(&hash, block_buf.as_slice())
                .await
                .unwrap();
            (hash, block)
        })
        .collect()
        .await
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CollectMissingObjectReferencesFromIntervalError {
    #[error(transparent)]
    InvalidInterval(
        #[from]
        #[backtrace]
        InvalidIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<IterBlocksError> for CollectMissingObjectReferencesFromIntervalError {
    fn from(v: IterBlocksError) -> Self {
        match v {
            IterBlocksError::InvalidInterval(e) => Self::InvalidInterval(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

pub async fn collect_object_references_from_interval(
    dataset: &dyn Dataset,
    head: &Multihash,
    tail: Option<&Multihash>,
    missing_files_only: bool,
) -> Result<Vec<ObjectFileReference>, CollectMissingObjectReferencesFromIntervalError> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();

    let mut block_stream = dataset
        .as_metadata_chain()
        .iter_blocks_interval(head, tail, false);
    while let Some((_, block)) = block_stream.try_next().await? {
        collect_object_references_from_block(
            dataset,
            &block,
            &mut res_references,
            missing_files_only,
        )
        .await;
    }

    Ok(res_references)
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn collect_object_references_from_metadata(
    dataset: &dyn Dataset,
    blocks: &Vec<(Multihash, MetadataBlock)>,
    missing_files_only: bool,
) -> Vec<ObjectFileReference> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();
    for (_, block) in blocks {
        collect_object_references_from_block(
            dataset,
            &block,
            &mut res_references,
            missing_files_only,
        )
        .await
    }

    res_references
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn collect_object_references_from_block(
    dataset: &dyn Dataset,
    block: &MetadataBlock,
    target_references: &mut Vec<ObjectFileReference>,
    missing_files_only: bool,
) {
    let data_repo = dataset.as_data_repo();
    let checkpoint_repo = dataset.as_checkpoint_repo();

    match &block.event {
        MetadataEvent::AddData(e) => {
            if !missing_files_only
                || !data_repo
                    .contains(&e.output_data.physical_hash)
                    .await
                    .unwrap()
            {
                target_references.push(ObjectFileReference {
                    object_type: ObjectType::DataSlice,
                    physical_hash: e.output_data.physical_hash.clone(),
                    size: e.output_data.size,
                });
            }
            if let Some(checkpoint) = e.output_checkpoint.as_ref() {
                if !missing_files_only
                    || !checkpoint_repo
                        .contains(&checkpoint.physical_hash)
                        .await
                        .unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::Checkpoint,
                        physical_hash: checkpoint.physical_hash.clone(),
                        size: checkpoint.size,
                    });
                }
            }
        }
        MetadataEvent::ExecuteQuery(e) => {
            if let Some(data_slice) = e.output_data.as_ref() {
                if !missing_files_only
                    || !data_repo.contains(&data_slice.physical_hash).await.unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::DataSlice,
                        physical_hash: data_slice.physical_hash.clone(),
                        size: data_slice.size,
                    });
                }
            }
            if let Some(checkpoint) = e.output_checkpoint.as_ref() {
                if !missing_files_only
                    || !checkpoint_repo
                        .contains(&checkpoint.physical_hash)
                        .await
                        .unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::Checkpoint,
                        physical_hash: checkpoint.physical_hash.clone(),
                        size: checkpoint.size,
                    });
                }
            }
        }

        MetadataEvent::Seed(_)
        | MetadataEvent::SetPollingSource(_)
        | MetadataEvent::SetTransform(_)
        | MetadataEvent::SetVocab(_)
        | MetadataEvent::SetWatermark(_)
        | MetadataEvent::SetAttachments(_)
        | MetadataEvent::SetInfo(_)
        | MetadataEvent::SetLicense(_) => (),
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_pull_object_transfer_strategy(
    dataset: &dyn Dataset,
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
) -> Result<PullObjectTransferStrategy, InternalError> {
    let get_download_url_result = match object_file_ref.object_type {
        ObjectType::MetadataBlock => {
            dataset
                .as_metadata_chain()
                .as_object_repo()
                .get_download_url(&object_file_ref.physical_hash, TransferOpts::default())
                .await
        }
        ObjectType::DataSlice => {
            dataset
                .as_data_repo()
                .get_download_url(&object_file_ref.physical_hash, TransferOpts::default())
                .await
        }
        ObjectType::Checkpoint => {
            dataset
                .as_checkpoint_repo()
                .get_download_url(&object_file_ref.physical_hash, TransferOpts::default())
                .await
        }
    };

    let transfer_url_result = match get_download_url_result {
        Ok(result) => Ok(TransferUrl {
            url: result.url,
            expires_at: result.expires_at,
        }),
        Err(error) => match error {
            GetTransferUrlError::NotSupported => Ok(TransferUrl {
                url: get_simple_transfer_protocol_url(object_file_ref, dataset_url),
                expires_at: None,
            }),
            GetTransferUrlError::Access(e) => Err(e.int_err()), // TODO: propagate AccessError
            GetTransferUrlError::Internal(e) => Err(e),
        },
    };

    match transfer_url_result {
        Ok(transfer_url) => Ok(PullObjectTransferStrategy {
            object_file: object_file_ref.clone(),
            pull_strategy: ObjectPullStrategy::HttpDownload,
            download_from: transfer_url,
        }),
        Err(e) => Err(e),
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

fn get_simple_transfer_protocol_url(
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
) -> Url {
    let path_suffix = match object_file_ref.object_type {
        ObjectType::MetadataBlock => "blocks/",
        ObjectType::DataSlice => "data/",
        ObjectType::Checkpoint => "checkpoints/",
    };

    dataset_url
        .join(path_suffix)
        .unwrap()
        .join(object_file_ref.physical_hash.to_multibase_string().as_str())
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_push_object_transfer_strategy(
    dataset: &dyn Dataset,
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
) -> Result<PushObjectTransferStrategy, InternalError> {
    let object_repo = match object_file_ref.object_type {
        ObjectType::MetadataBlock => dataset.as_metadata_chain().as_object_repo(),
        ObjectType::DataSlice => dataset.as_data_repo(),
        ObjectType::Checkpoint => dataset.as_checkpoint_repo(),
    };

    let contains = object_repo
        .contains(&object_file_ref.physical_hash)
        .await
        .map_err(|e| e.int_err())?;

    if contains {
        Ok(PushObjectTransferStrategy {
            object_file: object_file_ref.clone(),
            push_strategy: ObjectPushStrategy::SkipUpload,
            upload_to: None,
        })
    } else {
        let get_upload_url_result = object_repo
            .get_upload_url(&object_file_ref.physical_hash, TransferOpts::default())
            .await;
        let transfer_url_result = match get_upload_url_result {
            Ok(result) => Ok(TransferUrl {
                url: result.url,
                expires_at: result.expires_at,
            }),
            Err(error) => match error {
                GetTransferUrlError::NotSupported => Ok(TransferUrl {
                    url: get_simple_transfer_protocol_url(object_file_ref, dataset_url),
                    expires_at: None,
                }),
                GetTransferUrlError::Access(e) => Err(e.int_err()), // TODO: propagate AccessError
                GetTransferUrlError::Internal(e) => Err(e),
            },
        };
        match transfer_url_result {
            Ok(transfer_url) => Ok(PushObjectTransferStrategy {
                object_file: object_file_ref.clone(),
                push_strategy: ObjectPushStrategy::HttpUpload,
                upload_to: Some(transfer_url),
            }),
            Err(e) => Err(e),
        }
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

    let response = client
        .get(object_transfer_strategy.download_from.url.clone())
        .send()
        .await
        .map_err(|e| e.int_err())?;

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

pub async fn dataset_export_object_file(
    dataset: &dyn Dataset,
    object_transfer_strategy: &PushObjectTransferStrategy,
) -> Result<(), SyncError> {
    if object_transfer_strategy.push_strategy != ObjectPushStrategy::HttpUpload {
        panic!(
            "Unsupported push strategy {:?}",
            object_transfer_strategy.push_strategy
        );
    }
    if object_transfer_strategy.upload_to.is_none() {
        panic!("Expected URL for upload strategy")
    }

    let object_file_reference = &object_transfer_strategy.object_file;

    let source_object_repository = match object_file_reference.object_type {
        ObjectType::MetadataBlock => panic!("Metadata block unexpected at objects export stage"),
        ObjectType::DataSlice => dataset.as_data_repo(),
        ObjectType::Checkpoint => dataset.as_checkpoint_repo(),
    };

    let size = source_object_repository
        .get_size(&object_file_reference.physical_hash)
        .await
        .map_err(|e| SyncError::Internal(e.int_err()))?;

    let stream = source_object_repository
        .get_stream(&object_file_reference.physical_hash)
        .await
        .map_err(|e| SyncError::Internal(e.int_err()))?;

    use tokio_util::io::ReaderStream;
    let reader_stream = ReaderStream::new(stream);

    let client = reqwest::Client::new();

    let response = client
        .put(
            object_transfer_strategy
                .upload_to
                .as_ref()
                .unwrap()
                .url
                .clone(),
        )
        .header("content-type", "application/octet-stream")
        .header("content-length", size)
        .body(hyper::Body::wrap_stream(reader_stream))
        .send()
        .await
        .map_err(|e| SyncError::Internal(e.int_err()))?;

    if response.status().is_success() {
        Ok(())
    } else {
        tracing::error!(
            "File transfer to {} failed, result is {:?}",
            object_transfer_strategy.upload_to.as_ref().unwrap().url,
            response
        );
        Err(SyncError::Internal(
            (ObjectUploadError { response }).int_err(),
        ))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
