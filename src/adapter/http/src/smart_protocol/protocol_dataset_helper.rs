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
use headers::Header as _;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use odf::dataset::MetadataChainExt;
use tar::Header;
use thiserror::Error;
use url::Url;

use crate::smart_protocol::errors::ObjectUploadError;
use crate::smart_protocol::messages::*;
use crate::{BearerHeader, OdfSmtpVersion};

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
        odf::dataset::InvalidIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::IterBlocksError> for PrepareDatasetTransferEstimateError {
    fn from(v: odf::IterBlocksError) -> Self {
        match v {
            odf::IterBlocksError::InvalidInterval(e) => Self::InvalidInterval(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

pub async fn prepare_dataset_transfer_plan(
    metadata_chain: &dyn odf::MetadataChain,
    stop_at: &odf::Multihash,
    begin_after: Option<&odf::Multihash>,
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

        bytes_in_blocks += metadata_chain.get_block_size(&hash).await.int_err()?;

        match block.event {
            odf::MetadataEvent::AddData(add_data) => {
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
            odf::MetadataEvent::ExecuteTransform(execute_transform) => {
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
            odf::MetadataEvent::Seed(_)
            | odf::MetadataEvent::SetDataSchema(_)
            | odf::MetadataEvent::SetPollingSource(_)
            | odf::MetadataEvent::DisablePollingSource(_)
            | odf::MetadataEvent::AddPushSource(_)
            | odf::MetadataEvent::DisablePushSource(_)
            | odf::MetadataEvent::SetTransform(_)
            | odf::MetadataEvent::SetVocab(_)
            | odf::MetadataEvent::SetAttachments(_)
            | odf::MetadataEvent::SetInfo(_)
            | odf::MetadataEvent::SetLicense(_) => (),
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
    metadata_chain: &dyn odf::MetadataChain,
    stop_at: &odf::Multihash,
    begin_after: Option<&odf::Multihash>,
    ignore_missing_tail: bool,
) -> Result<MetadataBlocksBatch, InternalError> {
    let mut num_blocks: u32 = 0;
    let encoder = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
    let mut tarball_builder = tar::Builder::new(encoder);

    let blocks_for_transfer: Vec<odf::dataset::HashedMetadataBlock> = metadata_chain
        .iter_blocks_interval(stop_at, begin_after, ignore_missing_tail)
        .try_collect()
        .await
        .int_err()?;

    for (hash, _) in blocks_for_transfer.iter().rev() {
        num_blocks += 1;

        let block_bytes: Bytes = metadata_chain.get_block_bytes(hash).await.int_err()?;

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
) -> Result<VecDeque<odf::dataset::HashedMetadataBlock>, odf::GetBlockError> {
    let blocks_data = unpack_dataset_metadata_batch(blocks_batch);

    blocks_data
        .into_iter()
        .map(|(hash, bytes)| {
            // TODO: Avoid depending on specific implementation of
            //       metadata_block_repository_helpers::deserialize_metadata_block.
            //       This is currently necessary because we need to be able to deserialize
            //       blocks BEFORE an instance of MetadataChain exists.
            //       Consider injecting a configurable block deserializer.
            odf::storage::deserialize_metadata_block(&hash, &bytes).map(|block| (hash, block))
        })
        .collect::<Result<VecDeque<_>, _>>()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn unpack_dataset_metadata_batch(
    blocks_batch: &MetadataBlocksBatch,
) -> Vec<(odf::Multihash, Vec<u8>)> {
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
    let blocks_data: Vec<(odf::Multihash, Vec<u8>)> = archive
        .entries()
        .unwrap()
        .filter_map(Result::ok)
        .map(|mut entry| {
            let entry_size = entry.size();
            let mut buf = vec![0_u8; usize::try_from(entry_size).unwrap()];
            entry.read_exact(buf.as_mut_slice()).unwrap();

            let path = entry.path().unwrap();
            let hash = odf::Multihash::from_multibase(path.to_str().unwrap()).unwrap();

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
        odf::dataset::InvalidIntervalError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<odf::IterBlocksError> for CollectMissingObjectReferencesFromIntervalError {
    fn from(v: odf::IterBlocksError) -> Self {
        match v {
            odf::IterBlocksError::InvalidInterval(e) => Self::InvalidInterval(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

pub async fn collect_object_references_from_interval(
    dataset: &dyn odf::Dataset,
    head: &odf::Multihash,
    tail: Option<&odf::Multihash>,
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
    dataset: &dyn odf::Dataset,
    blocks: &VecDeque<odf::dataset::HashedMetadataBlock>,
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
    dataset: &dyn odf::Dataset,
    block: &odf::MetadataBlock,
    target_references: &mut Vec<ObjectFileReference>,
    missing_files_only: bool,
) {
    let data_repo = dataset.as_data_repo();
    let checkpoint_repo = dataset.as_checkpoint_repo();

    match &block.event {
        odf::MetadataEvent::AddData(e) => {
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
        odf::MetadataEvent::ExecuteTransform(e) => {
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

        odf::MetadataEvent::Seed(_)
        | odf::MetadataEvent::SetDataSchema(_)
        | odf::MetadataEvent::SetPollingSource(_)
        | odf::MetadataEvent::DisablePollingSource(_)
        | odf::MetadataEvent::AddPushSource(_)
        | odf::MetadataEvent::DisablePushSource(_)
        | odf::MetadataEvent::SetTransform(_)
        | odf::MetadataEvent::SetVocab(_)
        | odf::MetadataEvent::SetAttachments(_)
        | odf::MetadataEvent::SetInfo(_)
        | odf::MetadataEvent::SetLicense(_) => (),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn prepare_pull_object_transfer_strategy(
    dataset: &dyn odf::Dataset,
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
    maybe_bearer_header: Option<&BearerHeader>,
) -> Result<PullObjectTransferStrategy, InternalError> {
    let get_download_url_result = match object_file_ref.object_type {
        ObjectType::DataSlice => {
            dataset
                .as_data_repo()
                .get_external_download_url(
                    &object_file_ref.physical_hash,
                    odf::storage::ExternalTransferOpts::default(),
                )
                .await
        }
        ObjectType::Checkpoint => {
            dataset
                .as_checkpoint_repo()
                .get_external_download_url(
                    &object_file_ref.physical_hash,
                    odf::storage::ExternalTransferOpts::default(),
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
            odf::storage::GetExternalUrlError::NotSupported => Ok(TransferUrl {
                url: get_simple_transfer_protocol_url(object_file_ref, dataset_url),
                headers: get_simple_transfer_protocol_headers(
                    maybe_bearer_header,
                    SMART_TRANSFER_PROTOCOL_VERSION,
                ),
                expires_at: None,
            }),
            odf::storage::GetExternalUrlError::Access(e) => Err(e.int_err()), /* TODO: propagate */
            // AccessError
            odf::storage::GetExternalUrlError::Internal(e) => Err(e),
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
    maybe_bearer_header: Option<&BearerHeader>,
    version: i32,
) -> Vec<HeaderRow> {
    let mut headers = vec![HeaderRow {
        name: OdfSmtpVersion::name().to_string(),
        value: version.to_string(),
    }];
    if let Some(bearer) = maybe_bearer_header {
        headers.push(HeaderRow {
            name: http::header::AUTHORIZATION.to_string(),
            value: format!("Bearer {}", bearer.0.token()),
        });
    };
    headers
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
    dataset: &dyn odf::Dataset,
    object_file_ref: &ObjectFileReference,
    dataset_url: &Url,
    maybe_bearer_header: Option<&BearerHeader>,
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
                odf::storage::ExternalTransferOpts::default(),
            )
            .await;
        let transfer_url_result = match get_upload_url_result {
            Ok(result) => Ok(TransferUrl {
                url: result.url,
                headers: primitivize_header_map(&result.header_map),
                expires_at: result.expires_at,
            }),
            Err(error) => match error {
                odf::storage::GetExternalUrlError::NotSupported => Ok(TransferUrl {
                    url: get_simple_transfer_protocol_url(object_file_ref, dataset_url),
                    headers: get_simple_transfer_protocol_headers(
                        maybe_bearer_header,
                        SMART_TRANSFER_PROTOCOL_VERSION,
                    ),
                    expires_at: None,
                }),
                odf::storage::GetExternalUrlError::Access(e) => Err(e.int_err()), /* TODO: propagate */
                // AccessError
                odf::storage::GetExternalUrlError::Internal(e) => Err(e),
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
    dataset: &dyn odf::Dataset,
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
            odf::storage::InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&object_file_reference.physical_hash),
                size_hint: Some(object_file_reference.size),
            },
        )
        .await;

    match res {
        Ok(_) => Ok(()),
        Err(odf::storage::InsertError::HashMismatch(e)) => Err(CorruptedSourceError {
            message: concat!(
                "Data file hash declared by the source didn't match ",
                "the computed - this may be an indication of hashing ",
                "algorithm mismatch or an attempted tampering",
            )
            .to_owned(),
            source: Some(e.into()),
        }
        .into()),
        Err(odf::storage::InsertError::Access(e)) => Err(SyncError::Access(e)),
        Err(odf::storage::InsertError::Internal(e)) => Err(SyncError::Internal(e)),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_export_object_file(
    dataset: &dyn odf::Dataset,
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
        .int_err()?;

    let stream = source_object_repository
        .get_stream(&object_file_reference.physical_hash)
        .await
        .int_err()?;

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
        .body(reqwest::Body::wrap_stream(reader_stream))
        .send()
        .await
        .int_err()?;

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

/// Check if the seed block has the same `dataset_id` and `kind` as the existing
/// dataset.
pub(crate) fn ensure_seed_block_equals(
    first_incoming_block_maybe: Option<&(odf::Multihash, odf::MetadataBlock)>,
    existing_dataset_handle: &odf::DatasetHandle,
) -> bool {
    if let Some((_, first_incoming_block)) = first_incoming_block_maybe
        && let odf::MetadataEvent::Seed(seed_event) = &first_incoming_block.event
        && (seed_event.dataset_id != existing_dataset_handle.id
            || seed_event.dataset_kind != existing_dataset_handle.kind)
    {
        return false;
    }
    true
}
