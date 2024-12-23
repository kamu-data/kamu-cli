// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use axum::response::IntoResponse;
use axum_extra::typed_header::TypedHeader;
use database_common::DatabaseTransactionRunner;
use http_common::*;
use internal_error::ResultIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use odf::metadata::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use odf::metadata::serde::MetadataBlockSerializer;
use url::Url;

use crate::smart_protocol::messages::SMART_TRANSFER_PROTOCOL_VERSION;
use crate::smart_protocol::{AxumServerPullProtocolInstance, AxumServerPushProtocolInstance};
use crate::{BearerHeader, DatasetAliasInPath, OdfSmtpVersion, OdfSmtpVersionTyped};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Path)]
pub struct RefFromPath {
    /// Name of the reference
    reference: String,
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Path)]
pub struct BlockHashFromPath {
    /// Hash of the block
    #[param(value_type = String)]
    block_hash: odf::Multihash,
}

#[derive(serde::Deserialize, utoipa::IntoParams)]
#[into_params(parameter_in = Path)]
pub struct PhysicalHashFromPath {
    /// Physical hash of the block
    #[param(value_type = String)]
    physical_hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get named block reference
#[utoipa::path(
    get,
    path = "/refs/{reference}",
    params(RefFromPath, DatasetAliasInPath),
    responses((status = OK, body = String)),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_refs_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Path(ref_param): axum::extract::Path<RefFromPath>,
) -> Result<String, ApiError> {
    let block_ref = match odf::BlockRef::from_str(ref_param.reference.as_str()) {
        Ok(block_ref) => Ok(block_ref),
        Err(e) => Err(ApiError::not_found(e)),
    }?;

    match dataset.as_metadata_chain().resolve_ref(&block_ref).await {
        Ok(hash) => Ok(hash.to_string()),
        Err(e @ odf::storage::GetRefError::NotFound(_)) => Err(ApiError::not_found(e)),
        Err(e) => Err(e.api_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get block by hash
#[utoipa::path(
    get,
    path = "/blocks/{block_hash}",
    params(BlockHashFromPath, DatasetAliasInPath),
    responses((status = OK, description = "block content", content_type = "application/octet-stream", body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_blocks_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<BlockHashFromPath>,
) -> Result<Vec<u8>, ApiError> {
    let block: odf::MetadataBlock = match dataset
        .as_metadata_chain()
        .get_block(&hash_param.block_hash)
        .await
    {
        Ok(block) => Ok(block),
        Err(e @ odf::storage::GetBlockError::NotFound(_)) => Err(ApiError::not_found(e)),
        Err(e) => Err(e.api_err()),
    }?;

    let block_bytes = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .int_err()
        .api_err()?;

    Ok(block_bytes.collapse_vec())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get data slice by hash
#[utoipa::path(
    get,
    path = "/data/{physical_hash}",
    params(PhysicalHashFromPath, DatasetAliasInPath),
    responses((status = OK, description = "data file content", content_type = "application/octet-stream", body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_data_get_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> Result<impl IntoResponse, ApiError> {
    dataset_get_object_common(dataset.as_data_repo(), &hash_param.physical_hash).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Get checkpoint by hash
#[utoipa::path(
    get,
    path = "/checkpoints/{physical_hash}",
    params(PhysicalHashFromPath, DatasetAliasInPath),
    responses((status = OK, description = "checkpoint file content", content_type = "application/octet-stream", body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_checkpoints_get_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> Result<impl IntoResponse, ApiError> {
    dataset_get_object_common(dataset.as_checkpoint_repo(), &hash_param.physical_hash).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn dataset_get_object_common(
    object_repository: &dyn odf::storage::ObjectRepository,
    physical_hash: &odf::Multihash,
) -> Result<impl IntoResponse, ApiError> {
    let stream = match object_repository.get_stream(physical_hash).await {
        Ok(stream) => Ok(stream),
        Err(e @ odf::storage::GetError::NotFound(_)) => Err(ApiError::not_found(e)),
        Err(e) => Err(e.api_err()),
    }?;

    Ok(axum::body::Body::from_stream(
        tokio_util::io::ReaderStream::new(stream),
    ))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Upload data slice
#[utoipa::path(
    put,
    path = "/data/{physical_hash}",
    params(PhysicalHashFromPath, DatasetAliasInPath),
    request_body = Vec<u8>,
    responses((status = OK, body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_data_put_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    TypedHeader(content_length): TypedHeader<headers::ContentLength>,
    body: axum::body::Body,
) -> Result<(), ApiError> {
    dataset_put_object_common(
        dataset.as_data_repo(),
        hash_param.physical_hash,
        content_length.0,
        body,
    )
    .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Upload checkpoint
#[utoipa::path(
    put,
    path = "/checkpoints/{physical_hash}",
    params(PhysicalHashFromPath, DatasetAliasInPath),
    request_body = Vec<u8>,
    responses((status = OK, body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_checkpoints_put_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    TypedHeader(content_length): TypedHeader<headers::ContentLength>,
    body: axum::body::Body,
) -> Result<(), ApiError> {
    dataset_put_object_common(
        dataset.as_checkpoint_repo(),
        hash_param.physical_hash,
        content_length.0,
        body,
    )
    .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn dataset_put_object_common(
    object_repository: &dyn odf::storage::ObjectRepository,
    physical_hash: odf::Multihash,
    content_length: u64,
    body: axum::body::Body,
) -> Result<(), ApiError> {
    let src = Box::new(crate::axum_utils::body_into_async_read(body));

    object_repository
        .insert_stream(
            src,
            odf::storage::InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&physical_hash),
                size_hint: Some(content_length),
            },
        )
        .await
        .api_err()?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Initiate push via Smart Transfer Protocol
#[utoipa::path(
    get,
    path = "/push",
    params(DatasetAliasInPath),
    responses((status = OK, body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Extension(dataset_ref): axum::extract::Extension<odf::DatasetRef>,
    axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    uri: axum::extract::OriginalUri,
    TypedHeader(OdfSmtpVersion(version_header)): OdfSmtpVersionTyped,
    maybe_bearer_header: Option<BearerHeader>,
) -> Result<axum::response::Response, ApiError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(_) => Ok(()),
        CurrentAccountSubject::Anonymous(_) => Err(ApiError::new_unauthorized()),
    }?;
    ensure_version_compatibility(version_header)?;

    let server_url_config = catalog.get_one::<ServerUrlConfig>().unwrap();
    let dataset_url = get_base_dataset_url(uri, &server_url_config.protocols.base_url_rest, 1);

    let maybe_dataset = {
        let dataset_ref = dataset_ref.clone();
        DatabaseTransactionRunner::new(catalog.clone())
            .transactional_with(|dataset_registry: Arc<dyn DatasetRegistry>| async move {
                match dataset_registry.get_dataset_by_ref(&dataset_ref).await {
                    Ok(resolved_dataset) => Ok(Some((*resolved_dataset).clone())),
                    Err(odf::dataset::GetDatasetError::NotFound(_)) => {
                        // Make sure account in dataset ref being created and token account match
                        let CurrentAccountSubject::Logged(acc) = current_account_subject.as_ref()
                        else {
                            unreachable!()
                        };
                        if let Some(ref_account_name) = dataset_ref.account_name() {
                            if ref_account_name != &acc.account_name {
                                return Err(ApiError::new_forbidden());
                            }
                        }
                        Ok(None)
                    }
                    Err(err) => Err(err.api_err()),
                }
            })
            .await
    }?;

    Ok(ws.on_upgrade(|socket| {
        AxumServerPushProtocolInstance::new(
            socket,
            catalog,
            dataset_ref,
            maybe_dataset,
            dataset_url,
            maybe_bearer_header,
        )
        .serve()
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Initiate pull via Smart Transfer Protocol
#[utoipa::path(
    get,
    path = "/pull",
    params(DatasetAliasInPath),
    responses((status = OK, body = ())),
    tag = "odf-transfer",
    security(
        (),
        ("api_key" = [])
    )
)]
pub async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn odf::Dataset>>,
    axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    uri: axum::extract::OriginalUri,
    TypedHeader(OdfSmtpVersion(version_header)): OdfSmtpVersionTyped,
    maybe_bearer_header: Option<BearerHeader>,
) -> Result<axum::response::Response, ApiError> {
    ensure_version_compatibility(version_header)?;

    let server_url_config = catalog.get_one::<ServerUrlConfig>().unwrap();
    let dataset_url = get_base_dataset_url(uri, &server_url_config.protocols.base_url_rest, 1);

    Ok(ws.on_upgrade(move |socket| {
        AxumServerPullProtocolInstance::new(socket, dataset, dataset_url, maybe_bearer_header)
            .serve()
    }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn get_base_dataset_url(
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
    base_url_rest: &Url,
    depth: usize,
) -> Url {
    let mut path: Vec<_> = uri.path().split('/').collect();
    for _ in 0..depth {
        path.pop();
    }
    let path_string = format!("{}/", path.join("/"));
    base_url_rest.join(path_string.as_str()).unwrap()
}

fn ensure_version_compatibility(expected_version: i32) -> Result<(), ApiError> {
    if expected_version != SMART_TRANSFER_PROTOCOL_VERSION {
        return Err(ApiError::incompatible_client_version());
    }
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
