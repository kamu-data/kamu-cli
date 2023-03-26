// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ws_smart_transfer_protocol_axum_server, PARAMETER_BLOCK_HASH, PARAMETER_PHYSICAL_HASH,
    PARAMETER_REF,
};
use axum::extract::Extension;
use kamu::domain::*;

use opendatafabric::{
    serde::{flatbuffers::FlatbuffersMetadataBlockSerializer, MetadataBlockSerializer},
    Multihash,
};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use url::Url;

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_refs_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> Result<String, axum::http::StatusCode> {
    let ref_param = params.get(PARAMETER_REF).unwrap();

    let block_ref = match BlockRef::from_str(ref_param.as_str()) {
        Ok(block_ref) => Ok(block_ref),
        Err(_) => Err(axum::http::StatusCode::NOT_FOUND),
    }?;

    let get_ref_result = dataset.as_metadata_chain().get_ref(&block_ref).await;

    match get_ref_result {
        Ok(hash) => Ok(hash.to_string()),
        Err(GetRefError::NotFound(_)) => Err(axum::http::StatusCode::NOT_FOUND),
        Err(_) => {
            tracing::debug!("Internal error while resolving reference '{}'", ref_param);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_blocks_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> Result<Vec<u8>, axum::http::StatusCode> {
    let block_hash_param = params.get(PARAMETER_BLOCK_HASH).unwrap();

    let block_hash: Multihash = match Multihash::from_multibase_str(block_hash_param.as_str()) {
        Ok(block_hash) => block_hash,
        Err(e) => {
            tracing::debug!("MultihashError: {}, {}", block_hash_param, e);
            return Err(axum::http::StatusCode::BAD_REQUEST);
        }
    };

    let block = match dataset.as_metadata_chain().get_block(&block_hash).await {
        Ok(block) => block,
        Err(GetBlockError::NotFound(_)) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::debug!("GetBlockError: {}, {}", block_hash_param, e);
            return Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
    };

    match FlatbuffersMetadataBlockSerializer.write_manifest(&block) {
        Ok(block_bytes) => Ok(block_bytes.collapse_vec()),
        Err(e) => {
            tracing::debug!("Block serialization failed: {}, {}", block_hash_param, e);
            Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////

pub async fn dataset_data_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let physical_hash_param = params.get(PARAMETER_PHYSICAL_HASH).unwrap();

    let physical_hash = match Multihash::from_multibase_str(physical_hash_param.as_str()) {
        Ok(physical_hash) => physical_hash,
        Err(e) => {
            tracing::debug!("MultihashError: {}, {}", physical_hash_param, e);
            return Err(axum::http::StatusCode::BAD_REQUEST);
        }
    };

    let data_stream = match dataset.as_data_repo().get_stream(&physical_hash).await {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::debug!("Data GetError: {}, {}", physical_hash_param, e);
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
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let physical_hash_param = params.get(PARAMETER_PHYSICAL_HASH).unwrap();

    let physical_hash = match Multihash::from_multibase_str(physical_hash_param.as_str()) {
        Ok(physical_hash) => physical_hash,
        Err(e) => {
            tracing::debug!("MultihashError: {}, {}", physical_hash_param, e);
            return Err(axum::http::StatusCode::BAD_REQUEST);
        }
    };

    let checkpoint_stream = match dataset
        .as_checkpoint_repo()
        .get_stream(&physical_hash)
        .await
    {
        Ok(stream) => stream,
        Err(GetError::NotFound(_)) => return Err(axum::http::StatusCode::NOT_FOUND),
        Err(e) => {
            tracing::debug!("Checkpoint GetError: {}, {}", physical_hash_param, e);
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
    dataset: Extension<Arc<dyn Dataset>>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| {
        ws_smart_transfer_protocol_axum_server::dataset_push_ws_handler(socket, dataset.0)
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
    // TODO: HTTP is hardcoded
    let url = Url::parse(&format!("http://{}", host.0)).unwrap();

    let mut path: Vec<_> = uri.0.path().split('/').collect();
    for _ in 0..depth {
        path.pop();
    }

    url.join(&path.join("/")).unwrap()
}

/////////////////////////////////////////////////////////////////////////////////
