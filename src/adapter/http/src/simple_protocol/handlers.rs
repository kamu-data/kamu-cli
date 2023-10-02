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

use axum::extract::Extension;
use axum::headers::ContentLength;
use axum::TypedHeader;
use futures::TryStreamExt;
use kamu::domain::*;
use opendatafabric::serde::flatbuffers::FlatbuffersMetadataBlockSerializer;
use opendatafabric::serde::MetadataBlockSerializer;
use opendatafabric::{DatasetRef, Multihash};
use url::Url;

use crate::axum_utils::*;
use crate::smart_protocol::{AxumServerPullProtocolInstance, AxumServerPushProtocolInstance};

/////////////////////////////////////////////////////////////////////////////////

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

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_refs_handler(
    Extension(dataset): Extension<Arc<dyn Dataset>>,
    axum::extract::Path(ref_param): axum::extract::Path<RefFromPath>,
) -> Result<String, axum::response::Response> {
    let block_ref = match BlockRef::from_str(&ref_param.reference.as_str()) {
        Ok(block_ref) => Ok(block_ref),
        Err(_) => Err(not_found_response()),
    }?;

    let get_ref_result = dataset.as_metadata_chain().get_ref(&block_ref).await;

    match get_ref_result {
        Ok(hash) => Ok(hash.to_string()),
        Err(GetRefError::NotFound(_)) => Err(not_found_response()),
        Err(_) => {
            tracing::debug!(
                reference = %ref_param.reference,
                "Internal error while resolving reference"
            );
            return Err(internal_server_error_response());
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_blocks_handler(
    Extension(dataset): Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<BlockHashFromPath>,
) -> Result<Vec<u8>, axum::response::Response> {
    let block = match dataset
        .as_metadata_chain()
        .get_block(&hash_param.block_hash)
        .await
    {
        Ok(block) => block,
        Err(GetBlockError::NotFound(_)) => return Err(not_found_response()),
        Err(e) => {
            tracing::debug!(block_hash = %hash_param.block_hash, "GetBlockError: {}", e);
            return Err(internal_server_error_response());
        }
    };

    match FlatbuffersMetadataBlockSerializer.write_manifest(&block) {
        Ok(block_bytes) => Ok(block_bytes.collapse_vec()),
        Err(e) => {
            tracing::debug!(block_hash = %hash_param.block_hash, "Block serialization failed: {}", e);
            Err(internal_server_error_response())
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_get_handler(
    Extension(dataset): Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> axum::response::Response {
    let data_stream = match dataset
        .as_data_repo()
        .get_stream(&hash_param.physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return not_found_response(),
        Err(e) => {
            tracing::debug!(physical_hash = %hash_param.physical_hash, "Data GetError: {}", e);
            return internal_server_error_response();
        }
    };

    let body = axum_extra::body::AsyncReadBody::new(data_stream);
    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_get_handler(
    Extension(dataset): Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> axum::response::Response {
    let checkpoint_stream = match dataset
        .as_checkpoint_repo()
        .get_stream(&hash_param.physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return not_found_response(),
        Err(e) => {
            tracing::debug!(physical_hash = %hash_param.physical_hash, "Checkpoint GetError: {}", e);
            return internal_server_error_response();
        }
    };

    let body = axum_extra::body::AsyncReadBody::new(checkpoint_stream);
    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_put_handler(
    Extension(dataset): Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), axum::response::Response> {
    dataset_put_object_common(
        dataset.as_data_repo(),
        hash_param.physical_hash,
        content_length.0 as usize,
        body_stream,
    )
    .await
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_put_handler(
    Extension(dataset): Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
    TypedHeader(content_length): TypedHeader<ContentLength>,
    body_stream: axum::extract::BodyStream,
) -> Result<(), axum::response::Response> {
    dataset_put_object_common(
        dataset.as_checkpoint_repo(),
        hash_param.physical_hash,
        content_length.0 as usize,
        body_stream,
    )
    .await
}

/////////////////////////////////////////////////////////////////////////////////

async fn dataset_put_object_common(
    object_repository: &dyn ObjectRepository,
    physical_hash: Multihash,
    content_length: usize,
    body_stream: axum::extract::BodyStream,
) -> Result<(), axum::response::Response> {
    use tokio_util::compat::FuturesAsyncReadCompatExt;
    let reader = body_stream
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        .into_async_read()
        .compat();

    object_repository
        .insert_stream(
            Box::new(reader),
            InsertOpts {
                precomputed_hash: None,
                expected_hash: Some(&physical_hash),
                size_hint: Some(content_length),
            },
        )
        .await
        .map_err(|_| {
            axum::response::Response::builder()
                .status(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(Default::default())
                .unwrap()
        })?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    Extension(dataset_ref): Extension<DatasetRef>,
    Extension(catalog): Extension<dill::Catalog>,
    host: axum::extract::Host,
    uri: axum::extract::OriginalUri,
) -> axum::response::Response {
    let dataset_url = get_base_dataset_url(host, uri, 1);

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();

    let dataset = match dataset_repo.get_dataset(&dataset_ref).await {
        Ok(ds) => Some(ds),
        Err(GetDatasetError::NotFound(_)) => None,
        Err(err) => {
            tracing::error!("Could not get dataset: {:?}", err);
            return internal_server_error_response();
        }
    };

    ws.on_upgrade(|socket| {
        AxumServerPushProtocolInstance::new(socket, dataset_repo, dataset_ref, dataset, dataset_url)
            .serve()
    })
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    dataset: Extension<Arc<dyn Dataset>>,
    host: axum::extract::Host,
    uri: axum::extract::OriginalUri,
) -> axum::response::Response {
    let dataset_url = get_base_dataset_url(host, uri, 1);

    ws.on_upgrade(move |socket| {
        AxumServerPullProtocolInstance::new(socket, dataset.0, dataset_url).serve()
    })
}

/////////////////////////////////////////////////////////////////////////////////

fn get_base_dataset_url(
    host: axum::extract::Host,
    uri: axum::extract::OriginalUri,
    depth: usize,
) -> Url {
    let api_server_url = get_api_server_url(host);

    let mut path: Vec<_> = uri.0.path().split('/').collect();
    for _ in 0..depth {
        path.pop();
    }
    let path_string = format!("{}/", path.join("/"));
    api_server_url.join(path_string.as_str()).unwrap()
}

/////////////////////////////////////////////////////////////////////////////////

fn get_api_server_url(host: axum::extract::Host) -> Url {
    let scheme = std::env::var("KAMU_PROTOCOL_SCHEME").unwrap_or_else(|_| String::from("http"));
    Url::parse(&format!("{}://{}", scheme, host.0)).unwrap()
}

/////////////////////////////////////////////////////////////////////////////////
