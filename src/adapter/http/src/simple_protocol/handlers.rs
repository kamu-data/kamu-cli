// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

use event_bus::EventBus;
use kamu::domain::*;
use kamu_accounts::CurrentAccountSubject;
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::{DatasetRef, Multihash};
use url::Url;

use crate::api_error::*;
use crate::smart_protocol::{
    AxumServerPullProtocolInstance,
    AxumServerPushProtocolInstance,
    BearerHeader,
};

////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Deserialize)]
pub struct RefFromPath {
    reference: String,
}

#[derive(serde::Deserialize)]
pub struct BlockHashFromPath {
    block_hash: Multihash,
}

#[derive(serde::Deserialize)]
pub struct PhysicalHashFromPath {
    physical_hash: Multihash,
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_refs_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(ref_param): axum::extract::Path<RefFromPath>,
) -> Result<String, ApiError> {
    let block_ref = match BlockRef::from_str(ref_param.reference.as_str()) {
        Ok(block_ref) => Ok(block_ref),
        Err(e) => Err(ApiError::not_found(e)),
    }?;

    match dataset.as_metadata_chain().resolve_ref(&block_ref).await {
        Ok(hash) => Ok(hash.to_string()),
        Err(e @ GetRefError::NotFound(_)) => Err(ApiError::not_found(e)),
        Err(e) => Err(e.api_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_blocks_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<BlockHashFromPath>,
) -> Result<Vec<u8>, ApiError> {
    let block: opendatafabric::MetadataBlock = match dataset
        .as_metadata_chain()
        .get_block(&hash_param.block_hash)
        .await
    {
        Ok(block) => Ok(block),
        Err(e @ GetBlockError::NotFound(_)) => Err(ApiError::not_found(e)),
        Err(e) => Err(e.api_err()),
    }?;

    let block_bytes = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .int_err()
        .api_err()?;

    Ok(block_bytes.collapse_vec())
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_get_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> Result<axum::response::Response, ApiError> {
    dataset_get_object_common(dataset.as_data_repo(), &hash_param.physical_hash).await
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_get_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> Result<axum::response::Response, ApiError> {
    dataset_get_object_common(dataset.as_checkpoint_repo(), &hash_param.physical_hash).await
}

////////////////////////////////////////////////////////////////////////////////

async fn dataset_get_object_common(
    object_repository: &dyn ObjectRepository,
    physical_hash: &Multihash,
) -> Result<axum::response::Response, ApiError> {
    let stream = match object_repository.get_stream(physical_hash).await {
        Ok(stream) => Ok(stream),
        Err(e @ GetError::NotFound(_)) => Err(ApiError::not_found(e)),
        Err(e) => Err(e.api_err()),
    }?;

    axum::response::Response::builder()
        .body(axum::body::boxed(axum_extra::body::AsyncReadBody::new(
            stream,
        )))
        .int_err()
        .api_err()
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_put_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    axum::TypedHeader(content_length): axum::TypedHeader<axum::headers::ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), ApiError> {
    dataset_put_object_common(
        dataset.as_data_repo(),
        hash_param.physical_hash,
        content_length.0,
        body_stream,
    )
    .await
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_put_handler(
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    axum::TypedHeader(content_length): axum::TypedHeader<axum::headers::ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), ApiError> {
    dataset_put_object_common(
        dataset.as_checkpoint_repo(),
        hash_param.physical_hash,
        content_length.0,
        body_stream,
    )
    .await
}

////////////////////////////////////////////////////////////////////////////////

async fn dataset_put_object_common(
    object_repository: &dyn ObjectRepository,
    physical_hash: Multihash,
    content_length: u64,
    body_stream: axum::extract::BodyStream,
) -> Result<(), ApiError> {
    let src = Box::new(crate::axum_utils::body_into_async_read(body_stream));

    object_repository
        .insert_stream(
            src,
            InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&physical_hash),
                size_hint: Some(content_length),
            },
        )
        .await
        .api_err()?;

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Extension(dataset_ref): axum::extract::Extension<DatasetRef>,
    axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    uri: axum::extract::OriginalUri,
    maybe_bearer_header: Option<BearerHeader>,
) -> Result<axum::response::Response, ApiError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    match current_account_subject.as_ref() {
        CurrentAccountSubject::Logged(_) => Ok(()),
        CurrentAccountSubject::Anonymous(_) => Err(ApiError::new_unauthorized()),
    }?;

    let server_url_config = catalog.get_one::<ServerUrlConfig>().unwrap();
    let dataset_url = get_base_dataset_url(uri, &server_url_config.protocols.base_url_rest, 1);

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    let dataset = match dataset_repo.get_dataset(&dataset_ref).await {
        Ok(ds) => Ok(Some(ds)),
        Err(GetDatasetError::NotFound(_)) => {
            // Make sure account in dataset ref being created and token account match
            let CurrentAccountSubject::Logged(acc) = current_account_subject.as_ref() else {
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
    }?;

    let event_bus = catalog.get_one::<EventBus>().unwrap();

    Ok(ws.on_upgrade(|socket| {
        AxumServerPushProtocolInstance::new(
            socket,
            event_bus,
            dataset_repo,
            dataset_ref,
            dataset,
            dataset_url,
            maybe_bearer_header,
        )
        .serve()
    }))
}

////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    axum::extract::Extension(dataset): axum::extract::Extension<Arc<dyn Dataset>>,
    axum::extract::Extension(catalog): axum::extract::Extension<dill::Catalog>,
    uri: axum::extract::OriginalUri,
    maybe_bearer_header: Option<BearerHeader>,
) -> axum::response::Response {
    let server_url_config = catalog.get_one::<ServerUrlConfig>().unwrap();
    let dataset_url = get_base_dataset_url(uri, &server_url_config.protocols.base_url_rest, 1);

    ws.on_upgrade(move |socket| {
        AxumServerPullProtocolInstance::new(socket, dataset, dataset_url, maybe_bearer_header)
            .serve()
    })
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
