// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use futures::TryStreamExt;
use headers::Header as _;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::smart_protocol::errors::{ObjectDownloadError, ObjectUploadError};
use crate::smart_protocol::messages::*;
use crate::{BearerHeader, OdfSmtpVersion};

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
    }
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

fn reconstruct_header_map(
    headers_as_primitives: Vec<HeaderRow>,
) -> Result<http::HeaderMap, InternalError> {
    let mut res = http::HeaderMap::new();

    for HeaderRow { name, value } in headers_as_primitives {
        res.append(
            http::HeaderName::from_str(name.as_str()).int_err()?,
            http::HeaderValue::from_str(value.as_str()).int_err()?,
        );
    }

    Ok(res)
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
    match object_transfer_strategy.pull_strategy {
        ObjectPullStrategy::HttpDownload => {}
    }

    let object_file_reference = &object_transfer_strategy.object_file;

    let client = reqwest::Client::new();

    let response = client
        .get(object_transfer_strategy.download_from.url.clone())
        .headers(reconstruct_header_map(
            object_transfer_strategy.download_from.headers,
        )?)
        .send()
        .await
        .int_err()?;

    // The body of a failed download is not the object: hashing it would report
    // a corrupted source
    if !response.status().is_success() {
        return Err(SyncError::Internal(
            ObjectDownloadError {
                status: response.status(),
            }
            .int_err(),
        ));
    }

    let stream = response.bytes_stream();

    let reader = stream
        .map_err(std::io::Error::other)
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
            message: format!(
                "Source {} file hash didn't match the computed - this may be an indication of \
                 hashing algorithm mismatch or an attempted tampering",
                match object_file_reference.object_type {
                    ObjectType::DataSlice => "data",
                    ObjectType::Checkpoint => "checkpoint",
                }
            ),
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
    let upload_to = match object_transfer_strategy.push_strategy {
        ObjectPushStrategy::SkipUpload => {
            tracing::debug!(
                object_type = ?object_transfer_strategy.object_file.object_type,
                physical_hash = %object_transfer_strategy.object_file.physical_hash,
                "Skipping upload",
            );
            return Ok(());
        }
        ObjectPushStrategy::HttpUpload => object_transfer_strategy
            .upload_to
            .ok_or_else(|| "The server sent no URL to upload the object to".int_err())?,
    };

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

    let reader_stream = ReaderStream::new(stream);

    let client = reqwest::Client::new();

    let mut header_map = reconstruct_header_map(upload_to.headers)?;
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
