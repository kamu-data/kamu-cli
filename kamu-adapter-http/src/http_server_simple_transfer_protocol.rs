// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ws_smart_transfer_protocol_axum_server;
use axum::extract::Extension;
use kamu::domain::*;

use opendatafabric::{
    serde::{flatbuffers::FlatbuffersMetadataBlockSerializer, MetadataBlockSerializer},
    Multihash,
};
use std::{str::FromStr, sync::Arc};
use url::Url;

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
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(ref_param): axum::extract::Path<RefFromPath>,
) -> Result<String, axum::http::StatusCode> {
    let block_ref = match BlockRef::from_str(&ref_param.reference.as_str()) {
        Ok(block_ref) => Ok(block_ref),
        Err(_) => Err(axum::http::StatusCode::NOT_FOUND),
    }?;

    let get_ref_result = dataset.as_metadata_chain().get_ref(&block_ref).await;

    match get_ref_result {
        Ok(hash) => Ok(hash.to_string()),
        Err(GetRefError::NotFound(_)) => Err(axum::http::StatusCode::NOT_FOUND),
        Err(_) => {
            tracing::debug!(
                "Internal error while resolving reference '{}'",
                ref_param.reference
            );
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_blocks_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<BlockHashFromPath>,
) -> Result<Vec<u8>, axum::http::StatusCode> {
    let block = match dataset
        .as_metadata_chain()
        .get_block(&hash_param.block_hash)
        .await
    {
        Ok(block) => block,
        Err(GetBlockError::NotFound(_)) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::debug!("GetBlockError: {}, {}", hash_param.block_hash, e);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    match FlatbuffersMetadataBlockSerializer.write_manifest(&block) {
        Ok(block_bytes) => Ok(block_bytes.collapse_vec()),
        Err(e) => {
            tracing::debug!(
                "Block serialization failed: {}, {}",
                hash_param.block_hash,
                e
            );
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let data_stream = match dataset
        .as_data_repo()
        .get_stream(&hash_param.physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::debug!("Data GetError: {}, {}", hash_param.physical_hash, e);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let body = axum_extra::body::AsyncReadBody::new(data_stream);
    Ok(axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap())
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_checkpoints_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(hash_param): axum::extract::Path<PhysicalHashFromPath>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let checkpoint_stream = match dataset
        .as_checkpoint_repo()
        .get_stream(&hash_param.physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::debug!("Checkpoint GetError: {}, {}", hash_param.physical_hash, e);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    let body = axum_extra::body::AsyncReadBody::new(checkpoint_stream);
    Ok(axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap())
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    dataset_builder: Extension<Arc<Box<dyn DatasetBuilder>>>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| {
        ws_smart_transfer_protocol_axum_server::dataset_push_ws_handler(socket, dataset_builder.0)
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
        ws_smart_transfer_protocol_axum_server::dataset_pull_ws_handler(
            socket,
            dataset.0,
            dataset_url,
        )
    })
}

/////////////////////////////////////////////////////////////////////////////////

fn get_base_dataset_url(
    host: axum::extract::Host,
    uri: axum::extract::OriginalUri,
    depth: usize,
) -> Url {
    let scheme = std::env::var("KAMU_PROTOCOL_SCHEME").unwrap_or_else(|_| String::from("http"));
    let url = Url::parse(&format!("{}://{}", scheme, host.0)).unwrap();

    let mut path: Vec<_> = uri.0.path().split('/').collect();
    for _ in 0..depth {
        path.pop();
    }

    url.join(&path.join("/")).unwrap()
}

/////////////////////////////////////////////////////////////////////////////////
