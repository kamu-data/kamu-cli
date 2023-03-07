// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ws_smart_transfer_protocol_axum_server, DatasetResolver, PARAMETER_BLOCK_HASH,
    PARAMETER_DATASET_NAME, PARAMETER_PHYSICAL_HASH, PARAMETER_REF,
};
use axum::extract::Extension;
use kamu::domain::{BlockRef, Dataset};

use opendatafabric::{
    serde::{flatbuffers::FlatbuffersMetadataBlockSerializer, MetadataBlockSerializer},
    Multihash,
};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use url::Url;

pub async fn dataset_refs_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> String {
    let ref_param = params.get(PARAMETER_REF).unwrap();
    let block_ref = BlockRef::from_str(ref_param.as_str()).unwrap();
    let hash = dataset
        .as_metadata_chain()
        .get_ref(&block_ref)
        .await
        .unwrap();

    hash.to_string()
}

pub async fn dataset_blocks_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> Vec<u8> {
    let block_hash_param = params.get(PARAMETER_BLOCK_HASH).unwrap();
    let block_hash = Multihash::from_multibase_str(block_hash_param.as_str()).unwrap();
    let block = dataset
        .as_metadata_chain()
        .get_block(&block_hash)
        .await
        .unwrap();

    let block_bytes = FlatbuffersMetadataBlockSerializer
        .write_manifest(&block)
        .unwrap();

    block_bytes.collapse_vec()
}

pub async fn dataset_data_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> axum::response::Response {
    let physical_hash_param = params.get(PARAMETER_PHYSICAL_HASH).unwrap();
    let physical_hash = Multihash::from_multibase_str(physical_hash_param.as_str()).unwrap();
    let data_stream = dataset
        .as_data_repo()
        .get_stream(&physical_hash)
        .await
        .unwrap();
    let body = axum_extra::body::AsyncReadBody::new(data_stream);

    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

pub async fn dataset_checkpoints_handler(
    dataset: Extension<Arc<dyn Dataset>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
) -> axum::response::Response {
    let physical_hash_param = params.get(PARAMETER_PHYSICAL_HASH).unwrap();
    let physical_hash = Multihash::from_multibase_str(physical_hash_param.as_str()).unwrap();
    let checkpoint_stream = dataset
        .as_checkpoint_repo()
        .get_stream(&physical_hash)
        .await
        .unwrap();
    let body = axum_extra::body::AsyncReadBody::new(checkpoint_stream);

    axum::response::Response::builder()
        .body(axum::body::boxed(body))
        .unwrap()
}

pub async fn dataset_push_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    dataset: Extension<Arc<dyn Dataset>>,
) -> axum::response::Response {
    ws.on_upgrade(|socket| {
        ws_smart_transfer_protocol_axum_server::dataset_push_ws_handler(socket, dataset.0)
    })
}

pub async fn dataset_pull_ws_upgrade_handler(
    ws: axum::extract::ws::WebSocketUpgrade,
    dataset: Extension<Arc<dyn Dataset>>,
) -> axum::response::Response {
    ws.on_upgrade(move |socket| {
        ws_smart_transfer_protocol_axum_server::dataset_pull_ws_handler(socket, dataset.0)
    })
}

pub async fn resolve_dataset_by_name(
    dataset_resolver: axum::Extension<Arc<dyn DatasetResolver>>,
    axum::extract::Path(params): axum::extract::Path<HashMap<String, String>>,
    axum::TypedHeader(api_host): axum::TypedHeader<axum::headers::Host>,
    mut req: axum::http::Request<axum::body::Body>,
    next: axum::middleware::Next<axum::body::Body>,
) -> Result<axum::response::Response, axum::http::StatusCode> {
    let base_url = resolve_base_api_url(api_host);

    let dataset_name_param = params.get(PARAMETER_DATASET_NAME).unwrap();
    let dataset_resolution = dataset_resolver
        .resolve_dataset(dataset_name_param.as_str(), base_url)
        .await;

    match dataset_resolution {
        Ok(dataset) => {
            req.extensions_mut().insert(dataset);
            Ok(next.run(req).await)
        }
        Err(e) => match e {
            kamu::domain::GetDatasetError::NotFound(_) => Err(axum::http::StatusCode::NOT_FOUND),
            kamu::domain::GetDatasetError::Internal(_) => {
                println!("Dataset resolution error");
                Err(axum::http::StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

fn resolve_base_api_url(api_host: axum::headers::Host) -> Url {
    let mut base_url_str = String::from("http://");
    base_url_str += api_host.hostname();
    if let Some(port) = api_host.port() {
        base_url_str += ":";
        base_url_str += &port.to_string();
    }
    base_url_str += "/";

    Url::parse(base_url_str.as_str()).unwrap()
}
