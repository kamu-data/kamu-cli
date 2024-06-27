// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::io::Read;
use std::str::FromStr;

use bytes::Bytes;
use flate2::Compression;
use futures::TryStreamExt;
use kamu::deserialize_metadata_block;
use kamu::domain::*;
use opendatafabric::{MetadataBlock, MetadataEvent, Multihash};
use tar::Header;
use thiserror::Error;
use url::Url;

use super::BearerHeader;
use crate::smart_protocol::errors::ObjectUploadError;
use crate::smart_protocol::messages::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const MEDIA_TAR_GZ: &str = "application/tar+gzip";
const ENCODING_RAW: &str = "raw";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

pub async fn prepare_dataset_transfer_plan(
    metadata_chain: &dyn MetadataChain,
    stop_at: &Multihash,
    begin_after: Option<&Multihash>,
    ignore_missing_tail: bool,
) -> Result<TransferPlan, PrepareDatasetTransferEstimateError> {
    let mut block_stream =
        metadata_chain.iter_blocks_interval(stop_at, begin_after, ignore_missing_tail);

    let mut blocks_count: u32 = 0;
    let mut bytes_in_blocks: u64 = 0;

    let mut data_objects_count: u32 = 0;
    let mut checkpoint_objects_count: u32 = 0;
    let mut bytes_in_data_objects: u64 = 0;
    let mut bytes_in_checkpoint_objects: u64 = 0;

    let mut data_records_count: u64 = 0;

    while let Some((hash, block)) = block_stream.try_next().await? {
        blocks_count += 1;

        bytes_in_blocks += metadata_chain
            .as_metadata_block_repository()
            .get_block_size(&hash)
            .await
            .int_err()?;

        match block.event {
            MetadataEvent::AddData(add_data) => {
                if let Some(new_data) = &add_data.new_data {
                    data_objects_count += 1;
                    bytes_in_data_objects += new_data.size;
                    data_records_count += new_data.num_records();
                }
                if let Some(new_checkpoint) = &add_data.new_checkpoint {
                    checkpoint_objects_count += 1;
                    bytes_in_checkpoint_objects += new_checkpoint.size;
                }
            }
            MetadataEvent::ExecuteTransform(execute_transform) => {
                if let Some(new_data) = &execute_transform.new_data {
                    data_objects_count += 1;
                    bytes_in_data_objects += new_data.size;
                    data_records_count += new_data.num_records();
                }
                if let Some(new_checkpoint) = &execute_transform.new_checkpoint {
                    checkpoint_objects_count += 1;
                    bytes_in_checkpoint_objects += new_checkpoint.size;
                }
            }
            MetadataEvent::Seed(_)
            | MetadataEvent::SetDataSchema(_)
            | MetadataEvent::SetPollingSource(_)
            | MetadataEvent::DisablePollingSource(_)
            | MetadataEvent::AddPushSource(_)
            | MetadataEvent::DisablePushSource(_)
            | MetadataEvent::SetTransform(_)
            | MetadataEvent::SetVocab(_)
            | MetadataEvent::SetAttachments(_)
            | MetadataEvent::SetInfo(_)
            | MetadataEvent::SetLicense(_) => (),
        }
    }

    Ok(TransferPlan {
        num_blocks: blocks_count,
        num_objects: data_objects_count + checkpoint_objects_count,
        num_records: data_records_count,
        bytes_in_raw_blocks: bytes_in_blocks,
        bytes_in_raw_objects: bytes_in_data_objects + bytes_in_checkpoint_objects,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_dataset_metadata_batch(
    metadata_chain: &dyn MetadataChain,
    stop_at: &Multihash,
    begin_after: Option<&Multihash>,
    ignore_missing_tail: bool,
) -> Result<MetadataBlocksBatch, InternalError> {
    let mut num_blocks: u32 = 0;
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut tarball_builder = tar::Builder::new(encoder);

    let blocks_for_transfer: Vec<HashedMetadataBlock> = metadata_chain
        .iter_blocks_interval(stop_at, begin_after, ignore_missing_tail)
        .try_collect()
        .await
        .int_err()?;

    for (hash, _) in blocks_for_transfer.iter().rev() {
        num_blocks += 1;

        let block_bytes: Bytes = metadata_chain
            .as_metadata_block_repository()
            .get_block_data(hash)
            .await
            .int_err()?;

        let block_data: &[u8] = &block_bytes;

        let mut header = Header::new_gnu();
        header.set_size(block_bytes.len() as u64);

        tarball_builder
            .append_data(
                &mut header,
                hash.as_multibase().to_stack_string(),
                block_data,
            )
            .int_err()?;
    }

    let tarball_data = tarball_builder.into_inner().int_err()?.finish().int_err()?;

    Ok(MetadataBlocksBatch {
        num_blocks,
        media_type: String::from(MEDIA_TAR_GZ),
        encoding: String::from(ENCODING_RAW),
        payload: tarball_data,
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn decode_metadata_batch(
    blocks_batch: &MetadataBlocksBatch,
) -> Result<VecDeque<HashedMetadataBlock>, GetBlockError> {
    let blocks_data = unpack_dataset_metadata_batch(blocks_batch);

    blocks_data
        .into_iter()
        .map(|(hash, bytes)| {
            // TODO: Avoid depending on specific implementation of
            //       metadata_block_repository_helpers::deserialize_metadata_block.
            //       This is currently necessary because we need to be able to deserialize
            //       blocks BEFORE an instance of MetadataChain exists.
            //       Consider injecting a configurable block deserializer.
            deserialize_metadata_block(&hash, &bytes).map(|block| (hash, block))
        })
        .collect::<Result<VecDeque<_>, _>>()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AppendMetadataResponse {
    pub new_upstream_ids: Vec<opendatafabric::DatasetID>,
}

pub async fn dataset_append_metadata(
    dataset: &dyn Dataset,
    metadata: VecDeque<HashedMetadataBlock>,
    force_update_if_diverged: bool,
) -> Result<AppendMetadataResponse, AppendError> {
    if metadata.is_empty() {
        return Ok(AppendMetadataResponse {
            new_upstream_ids: vec![],
        });
    }

    let old_head = metadata.front().unwrap().1.prev_block_hash.clone();
    let new_head = metadata.back().unwrap().0.clone();

    let metadata_chain = dataset.as_metadata_chain();

    let mut new_upstream_ids: Vec<opendatafabric::DatasetID> = vec![];

    for (hash, block) in metadata {
        tracing::debug!(sequence_numer = %block.sequence_number, hash = %hash, "Appending block");

        if let opendatafabric::MetadataEvent::SetTransform(transform) = &block.event {
            // Collect only the latest upstream dataset IDs
            new_upstream_ids.clear();
            for new_input in &transform.inputs {
                if let Some(id) = new_input.dataset_ref.id() {
                    new_upstream_ids.push(id.clone());
                } else {
                    // Input references must be resolved to IDs here, but we
                    // ignore the errors and let the metadata chain reject this
                    // event
                }
            }
        }

        metadata_chain
            .append(
                block,
                AppendOpts {
                    update_ref: None,
                    expected_hash: Some(&hash),
                    ..AppendOpts::default()
                },
            )
            .await?;
    }

    metadata_chain
        .set_ref(
            &BlockRef::Head,
            &new_head,
            SetRefOpts {
                validate_block_present: false,
                check_ref_is: if force_update_if_diverged {
                    None
                } else {
                    Some(old_head.as_ref())
                },
            },
        )
        .await?;

    Ok(AppendMetadataResponse { new_upstream_ids })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn unpack_dataset_metadata_batch(blocks_batch: &MetadataBlocksBatch) -> Vec<(Multihash, Vec<u8>)> {
    assert!(
        blocks_batch.media_type.eq(MEDIA_TAR_GZ),
        "Unsupported media type {}",
        blocks_batch.media_type
    );

    assert!(
        blocks_batch.encoding.eq(ENCODING_RAW),
        "Unsupported batch encoding type {}",
        blocks_batch.encoding
    );

    let decoder = flate2::read::GzDecoder::new(blocks_batch.payload.as_slice());
    let mut archive = tar::Archive::new(decoder);
    let blocks_data: Vec<(Multihash, Vec<u8>)> = archive
        .entries()
        .unwrap()
        .filter_map(Result::ok)
        .map(|mut entry| {
            let entry_size = entry.size();
            let mut buf = vec![0_u8; usize::try_from(entry_size).unwrap()];
            entry.read_exact(buf.as_mut_slice()).unwrap();

            let path = entry.path().unwrap();
            let hash = Multihash::from_multibase(path.to_str().unwrap()).unwrap();

            (hash, buf)
        })
        .collect();

    blocks_data
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    ignore_missing_tail: bool,
    missing_files_only: bool,
) -> Result<Vec<ObjectFileReference>, CollectMissingObjectReferencesFromIntervalError> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();

    let mut block_stream =
        dataset
            .as_metadata_chain()
            .iter_blocks_interval(head, tail, ignore_missing_tail);
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn collect_object_references_from_metadata(
    dataset: &dyn Dataset,
    blocks: &VecDeque<HashedMetadataBlock>,
    missing_files_only: bool,
) -> Vec<ObjectFileReference> {
    let mut res_references: Vec<ObjectFileReference> = Vec::new();
    for (_, block) in blocks {
        collect_object_references_from_block(
            dataset,
            block,
            &mut res_references,
            missing_files_only,
        )
        .await;
    }

    res_references
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            if let Some(new_data) = &e.new_data {
                if !missing_files_only
                    || !data_repo.contains(&new_data.physical_hash).await.unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::DataSlice,
                        physical_hash: new_data.physical_hash.clone(),
                        size: new_data.size,
                    });
                }
            }
            if let Some(new_checkpoint) = &e.new_checkpoint {
                if !missing_files_only
                    || !checkpoint_repo
                        .contains(&new_checkpoint.physical_hash)
                        .await
                        .unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::Checkpoint,
                        physical_hash: new_checkpoint.physical_hash.clone(),
                        size: new_checkpoint.size,
                    });
                }
            }
        }
        MetadataEvent::ExecuteTransform(e) => {
            if let Some(new_data) = &e.new_data {
                if !missing_files_only
                    || !data_repo.contains(&new_data.physical_hash).await.unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::DataSlice,
                        physical_hash: new_data.physical_hash.clone(),
                        size: new_data.size,
                    });
                }
            }
            if let Some(new_checkpoint) = &e.new_checkpoint {
                if !missing_files_only
                    || !checkpoint_repo
                        .contains(&new_checkpoint.physical_hash)
                        .await
                        .unwrap()
                {
                    target_references.push(ObjectFileReference {
                        object_type: ObjectType::Checkpoint,
                        physical_hash: new_checkpoint.physical_hash.clone(),
                        size: new_checkpoint.size,
                    });
                }
            }
        }

        MetadataEvent::Seed(_)
        | MetadataEvent::SetDataSchema(_)
        | MetadataEvent::SetPollingSource(_)
        | MetadataEvent::DisablePollingSource(_)
        | MetadataEvent::AddPushSource(_)
        | MetadataEvent::DisablePushSource(_)
        | MetadataEvent::SetTransform(_)
        | MetadataEvent::SetVocab(_)
        | MetadataEvent::SetAttachments(_)
        | MetadataEvent::SetInfo(_)
        | MetadataEvent::SetLicense(_) => (),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_pull_object_transfer_strategy(
    dataset: &dyn Dataset,
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
    maybe_bearer_header: &Option<BearerHeader>,
) -> Result<PullObjectTransferStrategy, InternalError> {
    let get_download_url_result = match object_file_ref.object_type {
        ObjectType::DataSlice => {
            dataset
                .as_data_repo()
                .get_external_download_url(
                    &object_file_ref.physical_hash,
                    ExternalTransferOpts::default(),
                )
                .await
        }
        ObjectType::Checkpoint => {
            dataset
                .as_checkpoint_repo()
                .get_external_download_url(
                    &object_file_ref.physical_hash,
                    ExternalTransferOpts::default(),
                )
                .await
        }
    };

    let transfer_url_result = match get_download_url_result {
        Ok(result) => Ok(TransferUrl {
            url: result.url,
            headers: primitivize_header_map(&result.header_map),
            expires_at: result.expires_at,
        }),
        Err(error) => match error {
            GetExternalUrlError::NotSupported => Ok(TransferUrl {
                url: get_simple_transfer_protocol_url(object_file_ref, dataset_url),
                headers: get_simple_transfer_protocol_headers(maybe_bearer_header),
                expires_at: None,
            }),
            GetExternalUrlError::Access(e) => Err(e.int_err()), /* TODO: propagate */
            // AccessError
            GetExternalUrlError::Internal(e) => Err(e),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_simple_transfer_protocol_url(
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
) -> Url {
    let path_suffix = match object_file_ref.object_type {
        ObjectType::DataSlice => "data/",
        ObjectType::Checkpoint => "checkpoints/",
    };

    dataset_url
        .join(path_suffix)
        .unwrap()
        .join(
            &object_file_ref
                .physical_hash
                .as_multibase()
                .to_stack_string(),
        )
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_simple_transfer_protocol_headers(
    maybe_bearer_header: &Option<BearerHeader>,
) -> Vec<HeaderRow> {
    if let Some(bearer) = maybe_bearer_header {
        vec![HeaderRow {
            name: http::header::AUTHORIZATION.to_string(),
            value: format!("Bearer {}", bearer.0.token()),
        }]
    } else {
        vec![]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn primitivize_header_map(header_map: &http::HeaderMap) -> Vec<HeaderRow> {
    let mut res = Vec::new();

    for (name, value) in header_map {
        res.push(HeaderRow {
            name: name.to_string(),
            value: value.to_str().unwrap().to_string(),
        });
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn reconstruct_header_map(headers_as_primitives: Vec<HeaderRow>) -> http::HeaderMap {
    let mut res = http::HeaderMap::new();

    for HeaderRow { name, value } in headers_as_primitives {
        res.append(
            http::HeaderName::from_str(name.as_str()).unwrap(),
            http::HeaderValue::from_str(value.as_str()).unwrap(),
        );
    }

    res
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_push_object_transfer_strategy(
    dataset: &dyn Dataset,
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
    maybe_bearer_header: &Option<BearerHeader>,
) -> Result<PushObjectTransferStrategy, InternalError> {
    let object_repo = match object_file_ref.object_type {
        ObjectType::DataSlice => dataset.as_data_repo(),
        ObjectType::Checkpoint => dataset.as_checkpoint_repo(),
    };

    let contains = object_repo
        .contains(&object_file_ref.physical_hash)
        .await
        .int_err()?;

    if contains {
        Ok(PushObjectTransferStrategy {
            object_file: object_file_ref.clone(),
            push_strategy: ObjectPushStrategy::SkipUpload,
            upload_to: None,
        })
    } else {
        let get_upload_url_result = object_repo
            .get_external_upload_url(
                &object_file_ref.physical_hash,
                ExternalTransferOpts::default(),
            )
            .await;
        let transfer_url_result = match get_upload_url_result {
            Ok(result) => Ok(TransferUrl {
                url: result.url,
                headers: primitivize_header_map(&result.header_map),
                expires_at: result.expires_at,
            }),
            Err(error) => match error {
                GetExternalUrlError::NotSupported => Ok(TransferUrl {
                    url: get_simple_transfer_protocol_url(object_file_ref, dataset_url),
                    headers: get_simple_transfer_protocol_headers(maybe_bearer_header),
                    expires_at: None,
                }),
                GetExternalUrlError::Access(e) => Err(e.int_err()), /* TODO: propagate */
                // AccessError
                GetExternalUrlError::Internal(e) => Err(e),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_import_object_file(
    dataset: &dyn Dataset,
    object_transfer_strategy: PullObjectTransferStrategy,
) -> Result<(), SyncError> {
    assert!(
        !(object_transfer_strategy.pull_strategy != ObjectPullStrategy::HttpDownload),
        "Unsupported pull strategy {:?}",
        object_transfer_strategy.pull_strategy
    );

    let object_file_reference = &object_transfer_strategy.object_file;

    let client = reqwest::Client::new();

    let response = client
        .get(object_transfer_strategy.download_from.url.clone())
        .headers(reconstruct_header_map(
            object_transfer_strategy.download_from.headers,
        ))
        .send()
        .await
        .int_err()?;

    let stream = response.bytes_stream();

    use tokio_util::compat::FuturesAsyncReadCompatExt;
    let reader = stream
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();

    let target_object_repository = match object_file_reference.object_type {
        ObjectType::DataSlice => dataset.as_data_repo(),
        ObjectType::Checkpoint => dataset.as_checkpoint_repo(),
    };

    let res = target_object_repository
        .insert_stream(
            Box::new(reader),
            InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&object_file_reference.physical_hash),
                size_hint: Some(object_file_reference.size),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_export_object_file(
    dataset: &dyn Dataset,
    object_transfer_strategy: PushObjectTransferStrategy,
) -> Result<(), SyncError> {
    if object_transfer_strategy.push_strategy == ObjectPushStrategy::SkipUpload {
        tracing::debug!(
            object_type = ?object_transfer_strategy.object_file.object_type,
            physical_hash = %object_transfer_strategy.object_file.physical_hash,
            "Skipping upload",
        );
        return Ok(());
    }
    assert!(
        !(object_transfer_strategy.push_strategy != ObjectPushStrategy::HttpUpload),
        "Unsupported push strategy {:?}",
        object_transfer_strategy.push_strategy
    );
    assert!(
        object_transfer_strategy.upload_to.is_some(),
        "Expected URL for upload strategy"
    );

    let object_file_reference = &object_transfer_strategy.object_file;

    let source_object_repository = match object_file_reference.object_type {
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

    let upload_to = object_transfer_strategy.upload_to.unwrap();

    let mut header_map = reconstruct_header_map(upload_to.headers);
    header_map.append(
        http::header::CONTENT_TYPE,
        http::HeaderValue::from_static("application/octet-stream"),
    );
    header_map.append(http::header::CONTENT_LENGTH, http::HeaderValue::from(size));

    let response = client
        .put(upload_to.url.clone())
        .headers(header_map)
        .body(hyper::Body::wrap_stream(reader_stream))
        .send()
        .await
        .map_err(|e| SyncError::Internal(e.int_err()))?;

    if response.status().is_success() {
        Ok(())
    } else {
        tracing::error!(
            "File transfer to {} failed, result is {:?}",
            upload_to.url,
            response
        );
        Err(SyncError::Internal(
            (ObjectUploadError { response }).int_err(),
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
